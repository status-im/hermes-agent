"""Status app platform adapter (Hermes plugin).

Configuration in config.yaml::

    platforms:
      status_app:
        enabled: true

``extra`` normally needs no hand-editing — ``_env_enablement()`` seeds
chat_key, display_name and domain from the environment at config load.

Environment variables are declared in ``plugin.yaml``, which is the single
source of truth: ``_get_env_variables()`` reads them and ``interactive_setup()``
renders its prompts from the same declarations.

    STATUS_APP_CHAT_KEY       (required) chat key of the contact to talk to
    STATUS_APP_PASSWORD       (required) Status account password
    STATUS_APP_DISPLAY_NAME   (optional) this bot's Status display name,
                              default "My Hermes Agent"
    STATUS_APP_MNEMONIC       (optional) recovery phrase, to restore an account
    STATUS_APP_DOMAIN         (optional) status-go host, default "localhost"
    STATUS_APP_DOMAIN_PORT    (optional) status-go port, default "8080"

Defaults for the optional vars are declared once in ``_get_env_declarations()``
and applied by ``_get_env_variables()``, so they hold whether the value came
from the setup wizard or was never set at all.

Read by the gateway rather than by this module, and registered in
``register()`` rather than declared in plugin.yaml:

    STATUS_APP_HOME_CHANNEL     cron delivery target (cron_deliver_env_var)

Access control needs no env var of its own: STATUS_APP_CHAT_KEY is the whole
policy. ``_on_message`` drops every other sender, and
``StatusAppAdapter.enforces_own_access_policy`` tells the gateway that, so its
own ``_is_user_authorized`` default-deny does not fire.
"""
# PEP 563 — REQUIRED here, not stylistic. Annotations below reference SDK
# names (``models.Message``), and a parameter annotation is evaluated when
# its ``def`` executes, i.e. while the class body runs at import time. With
# status_sdk absent the guarded import leaves ``models`` undefined, so those
# annotations raised NameError and killed the import of this whole module —
# which meant check_requirements() could never run to lazy-install the SDK,
# and the plugin loader reported "Failed to load plugin 'status_app-platform':
# name 'models' is not defined". Making annotations lazy strings fixes it.
from __future__ import annotations

import os, time, asyncio, threading, datetime, yaml, logging, contextlib
from typing import Any, Dict, List, Optional, Union

# The adapter's own logger, matching every other platform adapter
# (gateway/platforms/base.py, plugins/platforms/irc/adapter.py). Deliberately
# NOT the SDK's ``Account.logger``: borrowing the client's logger would make
# every log line in this file depend on an Account existing, which in turn
# forces the Account to be constructed before it is needed.
logger = logging.getLogger(__name__)

# NOTE: deliberately no logging.basicConfig() here. This module is imported
# during plugin discovery, inside whatever process Hermes happens to be —
# CLI, TUI, gateway, cron. basicConfig() configures the ROOT logger, so a
# call here would re-format (or silently fail to re-format, if handlers are
# already attached) logging for every other adapter and for Hermes itself.
# Log configuration belongs to hermes_logging.setup_logging(); a plugin only
# ever gets a child logger.

# Guarded so a missing SDK degrades to "requirements not met" instead of
# breaking plugin discovery: an unguarded ImportError here would abort the
# import of this module, and check_requirements() would never run.
try:
    from status_sdk import Account, exceptions, models
    STATUS_SDK_AVAILABLE = True
except ImportError:
    STATUS_SDK_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

PLATFORM_NAME = "status_app"
# Hard limit, not a guess: the SDK raises MessageTooLongError above 2000,
# matching Status App's own cap. The gateway chunks to this before calling
# send(), so the exception should never fire in practice.
MAX_MESSAGE_LENGTH = 2_000

# Inbound events are queued rather than dispatched directly so a burst can't
# grow unbounded. ``messages.new`` fires with whole chat objects, so bursts
# are the normal case here, not the exception.
QUEUE_MAXSIZE = 500
DEDUP_MAX_SIZE = 2_000
DEDUP_WINDOW_SECONDS = 600

# Pushed onto the queue when the listener thread exits, so the consumer
# stops waiting instead of hanging on an empty queue forever.
_STOP = object()

# Ceiling for a standalone (out-of-process) send. login() blocks on a waku
# handshake with a 60s signal timeout when the backend has no live session,
# so this has to clear that with room to spare — but still bound it, or a
# cron job wedges indefinitely on an unreachable status-go.
STANDALONE_SEND_TIMEOUT = 60.0 * 5

# Ceiling for the mutual-contact handshake in connect(). When neither side has
# accepted yet, ``listen_contact_requests()`` blocks until a human taps Accept
# in Status — which may be never. connect() is awaited by gateway startup, so
# it has to give up eventually and let the gateway's reconnect loop retry
# rather than leave the platform wedged in "connecting" forever.
CONTACT_REQUEST_TIMEOUT = 300.0

# models.Message.chat_type -> Hermes chat_type. Hermes keys DM-specific
# behaviour off the literal "dm"; the SDK calls the same thing "private".
# Communities map to "group" — closest thing Hermes models.
_CHAT_TYPES = {"private": "dm", "group": "group", "community": "group"}

# Local nickname for the peer in our own contact list. Status requires a name
# on add_contact(); this one is never shown to the user — the peer's own
# profile name is what their client displays.
_CONTACT_DISPLAY_NAME = "Status User"

# GitHub repo the setup wizard builds status-go from. Named here rather than
# inline so there is one place to point at upstream (status-im/status-go) once
# the changes this connector depends on land there.
_STATUS_GO_REPO = "nickninov/status-go"


def _get_env_declarations() -> List[Dict[str, Any]]:
    """The env-var blocks declared in `plugin.yaml`, in manifest order.

    Each entry keeps its manifest fields (name / description / prompt / url /
    password) and gains ``required``. ``interactive_setup()`` renders straight
    from these, so declaring a var in plugin.yaml is all it takes for the
    wizard to ask for it.
    """
    file_path = os.path.join(os.path.dirname(__file__), "plugin.yaml")
    with open(file_path, 'r', encoding="utf-8") as f:
        data: dict = yaml.load(f, Loader=yaml.SafeLoader)

    default = {
        "STATUS_APP_DISPLAY_NAME": "My Hermes Agent",
        "STATUS_APP_DOMAIN": "localhost",
        "STATUS_APP_DOMAIN_PORT": "8080"
    }

    return [
        {**info, "required": required, "default": default.get(info["name"])}
        for current, required in [
            (data.get("requires_env") or [], True),
            (data.get("optional_env") or [], False),
        ]
        for info in current
    ]


def _get_env_variables() -> Dict[str, Dict[str, str | bool]]:
    """
    Get env variables from `plugin.yaml`

    Single leading underscore, not double: a ``__name`` is mangled to
    ``_StatusAppAdapter__name`` everywhere it appears inside the class body,
    so ``StatusAppAdapter.__init__`` could never call it.
    """
    return {
        info["name"]: {
            # Fall back to the declared default. Without this the defaults
            # declared above would only ever reach interactive_setup()'s
            # prompts, and a user who skipped the wizard (env vars set by
            # hand, docker -e, a cron process) would hand __init__ a None
            # where it expects a string — int(None) for the port,
            # Account(domain=None), login(name=None). Only optional vars
            # carry defaults, so this can never mask a missing required one.
            "value": os.environ.get(info["name"]) or info.get("default"),
            "required": info["required"],
        }
        for info in _get_env_declarations()
    }

def check_requirements() -> bool:
    """Return True when this platform's dependencies are available.

    Runs before validate_config() in create_adapter(), and is the only gate
    whose failure surfaces ``install_hint`` to the user. Dependencies only —
    whether the account is configured is validate_config()'s job.

    Lazy-installs ``status-sdk`` on first use via
    ``tools.lazy_deps.ensure_and_bind("platform.status_app")`` and rebinds the
    module globals the guarded import at the top left unset, mirroring
    ``check_teams_requirements`` / ``check_slack_requirements``. ``prompt=False``
    because this runs inside gateway startup and config-load paths, where
    there's no one at a terminal to answer.
    """
    if STATUS_SDK_AVAILABLE:
        return True

    def _import() -> dict:
        from status_sdk import Account, exceptions, models

        return {
            "Account": Account,
            "exceptions": exceptions,
            "models": models,
            "STATUS_SDK_AVAILABLE": True,
        }

    from tools.lazy_deps import ensure_and_bind

    return ensure_and_bind("platform.status_app", _import, globals(), prompt=False)


def validate_config(config: PlatformConfig) -> bool:
    """Return True when the PlatformConfig is properly configured.

    Env-based rather than extra-based: every required value is read from the
    environment via ``_get_env_variables()`` — by __init__ to build the
    adapter and by this gate to decide whether it's worth building — so these
    are the values that actually determine whether connect() can succeed.
    Returning False here stops the registry from ever building the adapter.
    """
    env_vars = _get_env_variables()
    return all([info["value"] for info in env_vars.values() if info["required"]])



def is_connected(config: PlatformConfig) -> bool:
    """Return True when the platform counts as connected for status display."""
    return validate_config(config)


class StatusAppAdapter(BasePlatformAdapter):
    """Status app adapter."""

    # Read by base.max_message_length_for_chat() to chunk outbound text.
    # Without this attribute base falls back to 4096 — and since it coerces
    # 0 to 4096 too, a falsy value here silently produces oversized sends.
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    # This adapter gates inbound access itself: _on_message drops everything
    # whose sender is not STATUS_APP_CHAT_KEY, so anything reaching the gateway
    # has already passed a hard one-key allowlist. Declaring that is what stops
    # _is_user_authorized() from default-denying it, and it replaces the
    # STATUS_APP_ALLOWED_USERS / _ALLOW_ALL_USERS env pair outright — no key
    # for the user to copy, nothing to keep in sync with the
    # configured-vs-resolved encoding, nothing for a later .env reload to undo.
    # The gateway honours this only while the policy below reads "allowlist";
    # it refuses to trust "open", which would be a fail-open.
    enforces_own_access_policy = True
    
    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        self.config = config
        extra = config.extra or {}
        env_vars = {
            key: info["value"] 
            for key, info in _get_env_variables().items()
        }
        self._display_name: str = env_vars["STATUS_APP_DISPLAY_NAME"]
        self._password: str = env_vars["STATUS_APP_PASSWORD"]
        self._chat_key: str = env_vars["STATUS_APP_CHAT_KEY"]
        # Read by _is_user_authorized() via _adapter_dm_policy/_group_policy.
        # "allowlist" is the literal the gateway checks for before trusting
        # enforces_own_access_policy above; the restriction it names is
        # _on_message's single-contact filter. Group is set too because
        # _CHAT_TYPES falls back to "group" for any chat_type the SDK reports
        # that we don't map, and that branch reads _group_policy instead.
        self._dm_policy = "allowlist"
        self._group_policy = "allowlist"
        # Optional and undeclared-by-default: unset means "create a fresh
        # account", so this stays None rather than "" and every read below
        # is a truthiness test, never len().
        self._mnemonic: Optional[str] = env_vars["STATUS_APP_MNEMONIC"]
        # Validated eagerly, even though the Account it feeds is not built
        # until connect(): a non-numeric port is user error in .env, and it
        # should fail at construction with its own name in the message rather
        # than as an opaque connect() failure 30s into gateway startup.
        self._domain: str = env_vars["STATUS_APP_DOMAIN"]
        raw_port = env_vars["STATUS_APP_DOMAIN_PORT"]
        try:
            self._backend_port: int = int(raw_port)
        except (TypeError, ValueError):
            raise ValueError(f"STATUS_APP_DOMAIN_PORT must be a port number, got {raw_port!r}")

        # Built in connect(), not here — see connect()'s docstring. Not
        # Optional[Account]: under the guarded import Account may be undefined,
        # which type checkers reject in a type expression.
        self._account: Optional[Any] = None
        # The Status Backend binary launcher path
        self._binary_launch_path: Optional[str] = None

        # Listener state. The SDK's listen_messages() is a blocking
        # generator, so it runs on its own thread and hands events to the
        # event loop through the queue.
        self._own_key: Optional[str] = None
        self._queue: Optional[asyncio.Queue] = None
        self._pump_thread: Optional[threading.Thread] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self._seen: Dict[str, float] = {}

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect and start listening. Return True on success.

        Every SDK call below blocks — ``login()`` waits on a waku handshake,
        ``listen_contact_requests()`` is a generator that parks until a human
        taps Accept — so none of them may run on the event loop directly.
        Doing so would stall the whole gateway: every other platform's
        adapter, every in-flight agent turn, every timer. They run on
        dedicated daemon threads via ``_run_blocking`` instead.

        The Account is built here rather than in ``__init__`` so that
        constructing an adapter needs no SDK: CI installs neither the
        ``status-app`` extra nor lazy deps, so ``Account`` is an undefined
        name there, and every unit test would otherwise have to patch it.
        """
        if not self._binary_launch_path:
            self._binary_launch_path = download_build_and_launch(self._domain, self._backend_port, self._binary_launch_path)

        self._account = Account(domain=self._domain, backend_port=self._backend_port)

        try:
            await self._run_blocking(self._login, name="status-app-login")
        except Exception as e:
            logger.error(f"Status login failed: {e}")
            return False

        try:
            established = await self._run_blocking(
                self._establish_contact,
                timeout=CONTACT_REQUEST_TIMEOUT,
                name="status-app-contact",
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Contact request to {self._chat_key} was not accepted within "
                f"{CONTACT_REQUEST_TIMEOUT:g}s — accept it in Status, then reconnect"
            )
            return False
        except Exception as e:
            logger.error(f"Status contact handshake failed: {e}")
            return False

        if not established:
            return False

        self._mark_connected()
        self._start_listener()
        return True

    async def _run_blocking(
        self,
        fn,
        *,
        timeout: Optional[float] = None,
        name: str,
    ) -> Any:
        """Run a blocking SDK call on a dedicated daemon thread.

        Deliberately not ``run_in_executor``: a call that may never return
        (``listen_contact_requests()``) would permanently consume one of the
        default executor's worker slots when we time out and walk away, and
        those slots are shared with every other executor user in the process.
        A daemon thread is abandoned just as cheaply but costs nothing shared,
        and it does not hold interpreter exit open.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()

        def _settle(setter, value) -> None:
            # wait_for cancels the future on timeout; the thread may still be
            # running and settle afterwards, so never touch a done future.
            if not future.done():
                setter(value)

        def _runner() -> None:
            try:
                result = fn()
            except BaseException as e:  # noqa: BLE001 — relayed to the awaiter
                setter, value = future.set_exception, e
            else:
                setter, value = future.set_result, result
            try:
                loop.call_soon_threadsafe(_settle, setter, value)
            except RuntimeError:
                pass  # loop closed underneath us during shutdown

        threading.Thread(target=_runner, name=name, daemon=True).start()
        if timeout is None:
            return await future
        return await asyncio.wait_for(future, timeout)

    def _login(self) -> None:
        """Log in and resolve the configured chat key. Blocking."""
        params = {"password": self._password, "name": self._display_name}
        if self._mnemonic:
            params["mnemonic"] = self._mnemonic

        self._account.login(**params)
        compressed_key = self._account.info["compressed_key"]
        logger.info(f"{self._display_name} Contact Key: {compressed_key}")
        # STATUS_APP_CHAT_KEY may be an ENS name or a compressed key; resolve
        # it once here so every later comparison — the inbound sender filter in
        # particular — is against the same uncompressed form the SDK stamps on
        # Message.from_public_key.
        self._chat_key = self._account.get_public_key(self._chat_key)

    def _establish_contact(self) -> bool:
        """Ensure a mutual contact with ``self._chat_key``. Blocking.

        Returns True once the contact is mutual (or the peer has already added
        us, which is enough for messages to flow). Blocks in
        ``listen_contact_requests()`` while waiting on a human to tap Accept —
        ``connect()`` bounds that with CONTACT_REQUEST_TIMEOUT.
        """
        # display_name MUST be passed by keyword. add_contact's signature is
        # (public_key, request_id=None, display_name=None), so a positional
        # second argument lands in request_id, which the SDK forwards to
        # status-go as AcceptContactRequest.id — a types.HexBytes. status-go
        # then rejects it with "cannot unmarshal hex string without 0x prefix"
        # and the whole handshake fails.
        contact: dict = self._account.contacts.get(self._chat_key, {})
        if not contact:
            logger.info(f"Contact not found. Sending friend request to {self._chat_key}")
            self._account.add_contact(self._chat_key, display_name=_CONTACT_DISPLAY_NAME)
            contact = self._account.contacts.get(self._chat_key, {})

        if contact.get("mutual", False):
            return True

        has_added_us = contact.get("has_added_us", False)
        self._account.add_contact(self._chat_key, display_name=_CONTACT_DISPLAY_NAME)
        if has_added_us:
            return True

        for request in self._account.listen_contact_requests():
            if request.public_key != self._chat_key:
                continue
            if request.incoming:
                # An incoming request is ACCEPTED, not re-sent: passing
                # request_id is what routes this to acceptContactRequest.
                self._account.add_contact(self._chat_key, request_id=request.id)
                break
            if request.accepted:
                break
        else:
            # Generator ended without a match — the signal socket closed.
            logger.error("Contact request stream ended before the contact was mutual")
            return False

        logger.info("User has accepted agent's contact request!")
        return True

    def _start_listener(self) -> None:
        """Start the listener thread and the queue consumer."""
        self._own_key: str = self._account.info["public_key"]
        self._queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._consumer_task = asyncio.create_task(self._consume())
        self._pump_thread = threading.Thread(
            target=self._pump,
            args=(asyncio.get_running_loop(),),
            name="status-app-listen",
            daemon=True,
        )
        self._pump_thread.start()
        logger.info("Listening for messages")

    def _offer(self, message: models.Message) -> None:
        """Put an event on the queue, dropping it if the agent is behind.

        Runs on the event loop (scheduled by the pump thread). Dropping is
        better than unbounded growth — the sender can always ask again.
        """
        try:
            self._queue.put_nowait(message)
        except asyncio.QueueFull:
            logger.warning("Inbound queue full — dropping event")

    def _pump(self, loop: asyncio.AbstractEventLoop) -> None:
        """Drain the SDK's blocking generator onto the event loop.

        Runs on its own thread for the life of the connection.
        ``call_soon_threadsafe`` is the only safe way to touch the loop from
        here — awaiting anything on this thread would deadlock.
        """
        try:
            for message in self._account.listen_messages():
                if not self._running:
                    break
                try:
                    loop.call_soon_threadsafe(self._offer, message)
                except RuntimeError:
                    return  # loop closed underneath us during shutdown
        except Exception as e:
            if self._running:
                logger.error(f"Listener thread died: {e}")
        finally:
            try:
                loop.call_soon_threadsafe(self._offer, _STOP)
            except RuntimeError:
                pass

    async def _consume(self) -> None:
        """Dispatch queued messages to _on_message."""
        while self._running:
            message = await self._queue.get()
            if message is _STOP:
                return
            try:
                await self._on_message(message)
            except Exception as e:
                logger.error(f"Error handling message: {e}")

    async def disconnect(self) -> None:
        """Stop listeners, close connections, cancel tasks.

        Closing the signal websocket is what breaks listen_messages() out of
        its block — without it the pump thread stays parked forever and
        outlives the adapter.
        """
        self._running = False
        self._mark_disconnected()

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

        if self._account is not None:
            # Both calls block on HTTP/websocket teardown.
            await asyncio.get_running_loop().run_in_executor(None, self._logout)

        if self._pump_thread and self._pump_thread.is_alive():
            self._pump_thread.join(timeout=5.0)
            if self._pump_thread.is_alive():
                logger.warning("Listener thread did not stop within 5s")
        self._pump_thread = None
        self._seen.clear()
        # Drop the logged-out Account so a later connect() builds a fresh one
        # rather than reusing a client whose signal socket we just closed.
        self._account = None

    def _logout(self) -> None:
        """Close the signal socket, then log out. Runs in an executor.

        Order matters. ``logout()`` only POSTs the logout endpoint and clears
        local state — it never touches the websocket, so without the explicit
        disconnect the pump thread stays blocked in listen_messages() and the
        join below times out on every shutdown.
        """
        signal = getattr(self._account, "signal", None)
        if signal is not None:
            try:
                signal.disconnect()
            except Exception as e:
                logger.debug(f"Signal disconnect: {e}")
        self._account.logout()
        
    # -- Inbound message processing -----------------------------------------

    async def _on_message(self, message: models.Message) -> None:
        """Convert one SDK message into a MessageEvent and dispatch it.

        ``listen_messages()`` yields a parsed ``models.Message`` per message,
        so this no longer walks ``event["chats"]`` looking for
        ``lastMessage``. Dedup is kept as cheap insurance against the signal
        re-delivering an id, but it is no longer load-bearing the way it was
        when whole chat objects arrived on every unrelated chat update.
        """
        # Two rejections, deliberately separate. The first stops a reply loop
        # on our own outbound messages. The second enforces the single-contact
        # design: this connector serves exactly the peer named by
        # STATUS_APP_CHAT_KEY, so traffic from anyone else — a community
        # channel, a second contact who added us — is not ours to answer.

        if message.from_public_key == self._own_key:
            return
        if message.from_public_key != self._chat_key:
            logger.debug(
                f"Ignoring message from non-configured contact {message.from_public_key}"
            )
            return

        if self._is_duplicate(message.id):
            return

        # Stickers and images arrive as a URL/path in `content`; forwarding
        # that as prompt text would just confuse the agent.
        if message.content_type != "text":
            logger.debug(f"Ignoring {message.content_type} message {message.id}")
            return

        text = (message.content or "").strip()
        if not text:
            return

        # NOT message.chat_id. On an inbound 1:1 message status-go reports
        # chatId as OUR OWN public key (verified against a live backend: own
        # key 0x04ae8cde…, inbound chatId 0x04ae8cde…), and no chat exists
        # under it — status-go's only chat for this conversation is keyed by
        # the PEER, which is exactly self._chat_key. chat_id round-trips into
        # send(), so using message.chat_id addressed a chat that cannot exist
        # and every reply died with "Chat not found".
        source = self.build_source(
            chat_id=self._chat_key,
            chat_name=f"{datetime.datetime.now().date()}-{self._chat_key}",
            chat_type=_CHAT_TYPES.get(message.chat_type, "group"),
            # The actual sender, not the chat key: single-contact makes them
            # equal today, but conflating them would hide the difference if
            # that ever stops being true.
            user_id=message.from_public_key,
            user_name=message.from_public_key,
        )

        logger.info(f"Message from {message.from_public_key}: {text[:80]}")
        await self.handle_message(MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message.id,
            raw_message=message,
            timestamp=self._as_utc(message.timestamp),
        ))

    @staticmethod
    def _as_utc(value: datetime.datetime) -> datetime.datetime:
        """Normalise an SDK timestamp to tz-aware UTC.

        ``Message.from_raw`` builds it with ``datetime.fromtimestamp(ms/1000)``
        and no tzinfo, so it is naive *local* time. Hermes works in tz-aware
        UTC, and comparing a naive datetime against an aware one raises
        TypeError — so convert rather than pass it through.
        """
        if value is None:
            return datetime.datetime.now(tz=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)

    def _is_duplicate(self, message_id: str) -> bool:
        """Return True if this message id was already seen recently."""
        now = time.time()
        if len(self._seen) > DEDUP_MAX_SIZE:
            cutoff = now - DEDUP_WINDOW_SECONDS
            self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
        if message_id in self._seen:
            return True
        self._seen[message_id] = now
        return False

    # -- Outbound messaging -------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:

        """Send a text message.

        The SDK signals failure by raising (``NotLoggedInError``,
        ``InvalidContactError``, ``MessageTooLongError``) rather than
        returning, so every exception has to become a SendResult — otherwise
        it escapes into send_with_retry() and its plain-text fallback never
        runs. ``send_message`` returns the new message's id, which rides back
        on the SendResult so callers can reply to or react to it.
        """
        if self._account is None:
            return SendResult(success=False, error="Not logged in")

        try:
            # Blocking RPC — must not run on the event loop.
            message_id = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: self._account.send_message(
                    chat_id=self._account.get_public_key(chat_id),
                    message=content,
                    reply_to_message_id=reply_to,
                ),
            )
        except exceptions.NotLoggedInError as e:
            return SendResult(success=False, error=str(e))
        except Exception as e:
            logger.error(f"send failed: {e}")
            return SendResult(success=False, error=str(e))

        return SendResult(success=True, message_id=message_id)


    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return ``{"name": ..., "type": ..., "chat_id": ...}``.

        ``account.chats`` merges community channels with mutual contacts and
        re-fetches over RPC on every access (it deliberately keeps no internal
        state), so it blocks and belongs in an executor like every other SDK
        call here.

        The SDK's own vocabulary is "contact" / "channel"; Hermes wants
        "dm" / "group" / "channel", and a Status contact chat is always 1:1.
        """
        if self._account is None:
            raise exceptions.NotLoggedInError()

        loop = asyncio.get_running_loop()
        # A community channel id isn't a chat key, so get_public_key() raises
        # for it. Both forms are accepted as the caller's chat_id, hence the
        # match against either.
        try:
            public_key = await loop.run_in_executor(
                None, lambda: self._account.get_public_key(chat_id)
            )
        except Exception:
            public_key = chat_id

        chats = await loop.run_in_executor(None, lambda: self._account.chats)
        for chat in chats:
            if chat.get("id") not in (chat_id, public_key):
                continue
            return {
                "chat_id": chat["id"],
                "name": chat.get("name") or chat["id"],
                "type": "channel" if chat.get("type") == "channel" else "dm",
            }

        raise exceptions.ChatNotFoundError(f"No chat for {chat_id}")


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def _env_enablement() -> dict | None:
    env_vars = _get_env_variables()
    if not all(info["value"] for info in env_vars.values() if info["required"]):
        return None

    get_value = lambda name: str(env_vars.get(name, {}).get("value") or "").strip()

    chat_key = get_value("STATUS_APP_CHAT_KEY")
    display_name = get_value("STATUS_APP_DISPLAY_NAME")
    # Secrets (password, mnemonic) are deliberately NOT seeded: ``extra`` is
    # serialized by PlatformConfig.to_dict(), so a dashboard config write can
    # round-trip them into config.yaml. __init__ reads them from env anyway.
    seed: dict = {
        "chat_key": chat_key,
        "display_name": display_name,
        "domain": get_value("STATUS_APP_DOMAIN"),
        "domain_port": get_value("STATUS_APP_DOMAIN_PORT"),
        # The contact we talk to IS the home channel — there's no separate
        # room/channel concept to point cron at.
        "home_channel": {"chat_id": chat_key, "name": display_name},
    }
    return seed



async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process send for cron / send_message_tool fallbacks.

    Without this, ``deliver=status_app`` cron jobs fail with "No live
    adapter for platform" when cron runs outside the gateway process.
    Return ``{"success": True, ...}`` or ``{"error": ...}``.

    ``thread_id`` and ``force_document`` are accepted to satisfy the
    standalone_sender_fn signature but ignored: Status has no threads, and no
    inline-vs-attachment distinction to switch on.

    Chunking is the caller's job — ``_send_to_platform`` reads
    ``max_message_length`` off our PlatformEntry (2000) and splits before
    calling us, so ``message`` already fits under the SDK's cap.
    """
    if not STATUS_SDK_AVAILABLE:
        return {"error": "status_sdk is not installed"}

    env_vars = _get_env_variables()
    if not all(info["value"] for info in env_vars.values() if info["required"]):
        return {"error": (
            "Status app is not configured — set STATUS_APP_CHAT_KEY, "
            "STATUS_APP_DISPLAY_NAME and STATUS_APP_PASSWORD"
        )}

    get_value = lambda name: str(env_vars.get(name, {}).get("value") or "").strip()

    extra = getattr(pconfig, "extra", None) or {}
    domain = get_value("STATUS_APP_DOMAIN") or str(extra.get("domain") or "") or "localhost"
    # Must match __init__'s Account(): a cron/send_message_tool delivery that
    # defaulted to the SDK's built-in port while the gateway ran status-go on
    # a custom one would fail with a connection error that looks nothing like
    # a port mismatch.
    raw_port = get_value("STATUS_APP_DOMAIN_PORT") or str(extra.get("domain_port") or "") or "8080"
    try:
        backend_port = int(raw_port)
    except ValueError:
        return {"error": f"STATUS_APP_DOMAIN_PORT is not a number: {raw_port!r}"}

    target = str(chat_id or "").strip() or get_value("STATUS_APP_CHAT_KEY")
    if not target:
        return {"error": "No chat_id given and STATUS_APP_CHAT_KEY is unset"}

    def send() -> Optional[str]:
        """Log in, send, and leave the session up. Blocking — runs in an executor."""
        account = Account(domain=domain, backend_port=backend_port)
        params = {
            "password": get_value("STATUS_APP_PASSWORD"),
            "name": get_value("STATUS_APP_DISPLAY_NAME"),
        }
        mnemonic = get_value("STATUS_APP_MNEMONIC")
        if mnemonic:
            params["mnemonic"] = mnemonic

        # Deliberately NOT paired with a logout(). status-go is a
        # single-account backend, so a gateway running elsewhere shares this
        # session: logging out here would tear down its messenger and kill
        # inbound delivery. Account.login() already detects a live session and
        # returns early rather than re-authenticating (status_sdk/account.py
        # "Account already logged in!"), so re-entering it is cheap and
        # leaving it up costs nothing.
        account.login(**params)
        public_key = account.get_public_key(target)
        return account.send_message(public_key, message)

    try:
        # A cold backend pays a waku handshake inside login() (60s signal wait),
        # so this is bounded — a cron job must not hang forever. The timeout
        # abandons the executor thread rather than killing it; it finishes on
        # its own, and the worst case is a message that lands after we've
        # already reported the timeout.
        message_id = await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(None, send),
            timeout=STANDALONE_SEND_TIMEOUT,
        )
    except asyncio.TimeoutError:
        return {"error": f"Status app send timed out after {STANDALONE_SEND_TIMEOUT:g}s"}
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return {"error": f"Status app send failed: {e}"}

    return {
        "success": True,
        "message_id": message_id,
        "platform": PLATFORM_NAME,
        "chat_id": target,
    }


def interactive_setup():
    """Interactive ``hermes gateway setup`` flow, rendered from plugin.yaml.

    Registered as ``setup_fn``. Without it ``_configure_platform()`` falls
    through to its generic branch and tells the user to configure
    ``gateway.platforms.status_app`` in config.yaml — which is wrong for a
    platform whose entire configuration lives in env vars.

    hermes_cli helpers are lazy-imported so this module stays importable from
    the gateway runtime and tests, where the CLI's import surface isn't wanted.
    """
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("Status App setup")
    backend_setup_values: Dict[str, str] = {}
    print_info("Hermes reaches Status App through Status Backend you run yourself.")
    if not prompt_yes_no("Do you have Status Backend already set up?", False):
        backend_setup_values.update({
            "STATUS_APP_DOMAIN": prompt("Domain you will be running Status Backend on", default="localhost"),
            "STATUS_APP_DOMAIN_PORT": prompt("Domain's port you will be running Status Backend on", default="8080"),
        })
        # Imported here, not at module scope: on a first-time install the
        # SDK is not present yet (it lazy-installs via check_requirements),
        # so a module-level `utils as sdk_utils` would be unbound in exactly
        # the run that needs it — the setup wizard.
        if not check_requirements():
            print_warning("status-sdk is unavailable — cannot download Status Backend")
            return
        from hermes_constants import display_hermes_home

        print_info(f"Downloading Status-Backend build from {_STATUS_GO_REPO} and launching on {backend_setup_values['STATUS_APP_DOMAIN']}:{backend_setup_values['STATUS_APP_DOMAIN_PORT']}")
        print_info(f"   Install directory: {display_hermes_home()}/status-backend")

        download_build_and_launch(
            backend_setup_values['STATUS_APP_DOMAIN'], 
            backend_setup_values['STATUS_APP_DOMAIN_PORT']
        )

        print_info("Launched Status-Backend")

    for info in _get_env_declarations():
        name: str = info["name"]
        if name in backend_setup_values:
            save_env_value(name, backend_setup_values[name])
            continue

        label = str(info.get("prompt") or name)
        existing: Optional[str] = get_env_value(name)

        print()
        print_info(info["description"])
        # STATUS_APP_DOMAIN / _DOMAIN_PORT declare no help URL — only print
        # one when the manifest actually carries it.
        if info.get("url"):
            print_info(info["url"])

        if existing and not prompt_yes_no(
            f"Found value for {name}. Would you like to overwrite existing value?", False
        ):
            continue

        params: Dict[str, Any] = {"question": label, "password": info.get("password", False)}
        if info.get("default"):
            params["default"] = info["default"]

        value = prompt(**params).strip()
        if not value and info["required"]:
            print_warning(f"{name} is required — skipping Status setup")
            return

        save_env_value(name, value)

    # No access-control prompt: STATUS_APP_CHAT_KEY already IS the access
    # policy. The adapter drops every other sender at intake and tells the
    # gateway so via enforces_own_access_policy, so there is no second list
    # to write or keep in sync.
    print()
    print_success("Status configuration saved!")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


def download_build_and_launch(domain: str, port: Union[int, str], launch_path: Optional[str] = None) -> str:
    from status_sdk import utils as sdk_utils
    from hermes_constants import get_hermes_home

    if isinstance(port, str):
        port = int(port)
    
    backend_dir = get_hermes_home() / "status-backend"
    backend_dir.mkdir(parents=True, exist_ok=True)
    with contextlib.chdir(backend_dir):
        params = {
            "launcher": launch_path,
            "repo_name": _STATUS_GO_REPO,
            "address": f"{domain}:{port}",
        }
        download_path = sdk_utils.download_build_and_launch(**params)

    return download_path

def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup.

    Extra kwargs are forwarded to ``PlatformEntry``; unknown keys raise
    TypeError. See ``gateway/platform_registry.py`` for the full field list.
    """
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Status App",
        adapter_factory=lambda cfg: StatusAppAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[i["name"] for i in _get_env_declarations() if i["required"]],
        install_hint="pip install status-sdk",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="STATUS_APP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        # No allowed_users_env / allow_all_env: access control is
        # STATUS_APP_CHAT_KEY, enforced in _on_message and declared to the
        # gateway by StatusAppAdapter.enforces_own_access_policy.
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🛡️ ",
        platform_hint=(
            "You are chatting via Status App, an end-to-end encrypted "
            "messenger. You are talking to exactly one contact, identified by "
            "a cryptographic public key rather than a username or phone number."
        ),
    )
