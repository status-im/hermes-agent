"""Dev driver for the Status app adapter — run it by hand while building.

Not part of the plugin contract. The loader only reads ``plugin.yaml`` and
``register()``, so this file is inert at runtime; it exists to give you a
loop that is faster than starting the whole gateway.

Usage (from the repo root, with the venv active)::

    # Connect, print every inbound message, stay up until Ctrl+C
    python plugins/platforms/status_app/dev_run.py

    # Same, but reply to every inbound message so you can see send() work
    python plugins/platforms/status_app/dev_run.py --echo

    # Skip listening; just fire one outbound message and exit
    python plugins/platforms/status_app/dev_run.py --to 0xPUBKEY --send "hi"

    # Route inbound through BasePlatformAdapter.handle_message instead of
    # the local printer — exercises session keys, authorization and
    # chunking. Heavier, and it needs the auth env vars set.
    python plugins/platforms/status_app/dev_run.py --echo --gateway-path

Reads the same env vars as the adapter: whatever ``_env_enablement()``
returns is used as ``PlatformConfig.extra``, so your env plumbing is
exercised too, not bypassed.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import logging
import os
import sys
from pathlib import Path

_PLUGIN_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PLUGIN_DIR.parents[2]

# Must match PLATFORM_NAME in adapter.py — it's the key the registry uses.
PLATFORM_NAME = "status_app"

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_hermes_env() -> list:
    """Load .env into os.environ exactly as Hermes' entrypoints do.

    Running this file with plain ``python`` skips Hermes' startup, so
    without this only shell-exported vars would be visible and
    ``os.getenv`` would disagree with what ``hermes gateway`` sees.

    ``load_hermes_dotenv`` is the same loader used by ``hermes_cli/main.py``,
    ``gateway/run.py`` and ``cli.py``. It resolves the home from the
    ``HERMES_HOME`` env var and falls back to ``~/.hermes`` — which is the
    wrong directory on Windows — so export the resolved home first, the way
    ``main.py`` does before calling it.
    """
    try:
        from hermes_cli.config import get_hermes_home
        from hermes_cli.env_loader import load_hermes_dotenv

        os.environ.setdefault("HERMES_HOME", str(get_hermes_home()))
        return load_hermes_dotenv()
    except Exception as e:  # a broken .env shouldn't stop the dev loop
        print(f"warning: could not load .env: {e}")
        return []


def _load_adapter_module():
    """Load adapter.py by path, so this works from any cwd."""
    spec = importlib.util.spec_from_file_location(
        "status_app_dev_adapter", _PLUGIN_DIR / "adapter.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# Substrings that mark a config key as secret. Bare "key" is deliberately
# absent — it would mask public_key / key_uid, which you want to read.
_SECRET_HINTS = ("password", "mnemonic", "secret", "token", "api_key")


def _format_extra(extra: dict, show_secrets: bool) -> str:
    """Render PlatformConfig.extra, masking secrets unless asked otherwise."""
    if show_secrets:
        return repr(extra)
    return repr(
        {
            k: ("***" if any(h in k.lower() for h in _SECRET_HINTS) else v)
            for k, v in extra.items()
        }
    )


def _check() -> int:
    """Verify the plugin through the real registry. No Docker, no login.

    This is the path ``hermes gateway start`` takes: discovery parses
    plugin.yaml, the deferred loader runs register(), then create_adapter()
    gates on check_fn() and validate_config() before building the adapter.
    A failure here means the gateway would silently skip the platform.
    """
    from gateway.config import PlatformConfig
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins

    discover_plugins()
    # Platform plugins register lazily. all_entries() runs the pending
    # deferred loaders; platform_registry.get() alone does NOT, and makes a
    # perfectly good plugin look like it was never registered.
    platform_registry.all_entries()

    entry = platform_registry.get(PLATFORM_NAME)
    if entry is None:
        print(f"FAIL  {PLATFORM_NAME} not registered — check plugin.yaml and register()")
        return 1

    config = PlatformConfig(enabled=True)
    checks = [
        ("registered", True, f"label={entry.label} emoji={entry.emoji}"),
        ("check_fn()", entry.check_fn(), "dependencies present"),
        ("validate_config()", entry.validate_config(config), "account configured"),
        ("max_message_length", entry.max_message_length == 2_000, entry.max_message_length),
        ("platform_hint", bool(entry.platform_hint), "agent knows it's on Status"),
        ("install_hint", bool(entry.install_hint), "shown when deps missing"),
    ]
    failed = 0
    for name, ok, detail in checks:
        print(f"  {'ok  ' if ok else 'FAIL'}  {name:<20} {detail}")
        failed += 0 if ok else 1

    adapter = platform_registry.create_adapter(PLATFORM_NAME, config)
    built = adapter is not None
    print(f"  {'ok  ' if built else 'FAIL'}  create_adapter()     "
          f"{type(adapter).__name__ if built else 'None — gateway would skip this platform'}")
    failed += 0 if built else 1

    print("\nall checks passed" if not failed else f"\n{failed} check(s) failed")
    return 0 if not failed else 1


def _build_config(adapter_mod):
    """Build a PlatformConfig the way the gateway would."""
    from gateway.config import PlatformConfig

    config = PlatformConfig(enabled=True)
    seed = adapter_mod._env_enablement() or {}
    # home_channel is turned into a dataclass by core; irrelevant here.
    config.extra = {k: v for k, v in seed.items() if k != "home_channel"}
    return config


async def _drive(args) -> int:
    loaded = _load_hermes_env()
    adapter_mod = _load_adapter_module()

    print(f"env loaded    : {[str(p) for p in loaded] or 'nothing — no .env found'}")
    print(f"requirements  : {adapter_mod.check_requirements()}")

    config = _build_config(adapter_mod)
    print(f"config.extra  : {_format_extra(config.extra, args.show_secrets)}")

    adapter = adapter_mod.StatusAppAdapter(config)

    if args.gateway_path:
        async def _handler(event):
            print(f"\n[gateway] {event.source.user_id}: {event.text}")
            return f"echo: {event.text}" if args.echo else None

        adapter.set_message_handler(_handler)
    else:
        # Bypass the gateway entirely: replace the instance's dispatch with
        # a local printer. Keeps the loop to adapter code only.
        async def _on_event(event):
            print(f"\n[inbound] chat={event.source.chat_id} from={event.source.user_id}")
            print(f"          text={event.text!r} id={event.message_id}")
            if args.echo:
                result = await adapter.send(event.source.chat_id, f"echo: {event.text}")
                print(f"[reply  ] success={result.success} error={getattr(result, 'error', None)}")

        adapter.handle_message = _on_event  # type: ignore[method-assign]

    print("\nconnecting…")
    ok = await adapter.connect()
    print(f"connect() -> {ok}")
    if not ok:
        return 1

    try:
        if args.send is not None:
            if not args.to:
                print("--send requires --to <chat_id>")
                return 2
            result = await adapter.send(args.to, args.send)
            print(f"send() -> success={result.success} error={getattr(result, 'error', None)}")
            return 0 if result.success else 1

        print("listening — Ctrl+C to stop\n")
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        print("\ndisconnecting…")
        await adapter.disconnect()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Status adapter by hand.")
    parser.add_argument("--echo", action="store_true", help="reply to every inbound message")
    parser.add_argument("--send", metavar="TEXT", help="send one message and exit")
    parser.add_argument("--to", metavar="CHAT_ID", help="target chat id for --send")
    parser.add_argument(
        "--gateway-path",
        action="store_true",
        help="dispatch through BasePlatformAdapter.handle_message",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify plugin wiring through the real registry, then exit (no login)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging")
    parser.add_argument(
        "--show-secrets",
        action="store_true",
        help="print password/mnemonic/token values in full instead of ***",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-7s %(name)s: %(message)s",
    )

    if args.check:
        _load_hermes_env()
        return _check()

    try:
        return asyncio.run(_drive(args))
    except KeyboardInterrupt:
        print("\ninterrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
