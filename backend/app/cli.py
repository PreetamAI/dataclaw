"""DataClaw CLI — `dataclaw <command>`.

Commands:
    dataclaw init           generate ~/.dataclaw/.env if needed, then run migrations
    dataclaw start          launch backend, open browser  (alias: dashboard)
    dataclaw stop           stop a running daemon
    dataclaw status         show running services
    dataclaw doctor         verify install + dependencies
    dataclaw logs           tail the backend log
    dataclaw version        print version
    dataclaw connectors list
                            print connector catalog and stability
    dataclaw compile        rebuild the knowledge graph
    dataclaw mcp verify     verify MCP catalog executors
    dataclaw rotate-master-key
                            re-encrypt stored secrets with a new master key
    dataclaw secrets import <file>
                            load connector credentials from a local JSON file
                            so secrets don't have to be pasted through chat
                            or other untrusted channels
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import secrets as _secrets
import signal
import socket
import subprocess
import sys
import time
import webbrowser
from importlib import metadata
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATACLAW_HOME", str(Path.home() / ".dataclaw")))
ENV_PATH = DATA_DIR / ".env"
PID_PATH = DATA_DIR / "dataclaw.pid"
LOG_PATH = DATA_DIR / "dataclaw.log"
DEFAULT_PORT = 8000
DEFAULT_HOST = "127.0.0.1"


def _env_template(master_key: str, session_secret: str, db_url: str, demo_db_url: str) -> str:
    return (
        "# DataClaw local install config. Process env vars override these values.\n"
        f"MASTER_KEY={master_key}\n"
        f"SESSION_SECRET={session_secret}\n"
        f"DATABASE_URL={db_url}\n"
        f"DEMO_DATABASE_URL={demo_db_url}\n"
        "# Add your OpenAI key to enable the chat agent. Leave empty to use Ollama (see docs).\n"
        "OPENAI_API_KEY=\n"
        "OPENAI_MODEL=gpt-4.1-mini\n"
        "DEMO_MODE=true\n"
        "# Local installs run without auth. Set to false + configure ADMIN_EMAIL/ADMIN_PASSWORD\n"
        "# for hosted deployments.\n"
        "DATACLAW_AUTH_DISABLED=true\n"
        "CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173,http://127.0.0.1:8000\n"
    )


def _generate_master_key() -> str:
    from cryptography.fernet import Fernet
    return Fernet.generate_key().decode()


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _port_in_use(port: int, host: str = DEFAULT_HOST) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
        except OSError:
            return True
    return False


def _read_pid() -> int | None:
    if not PID_PATH.exists():
        return None
    try:
        return int(PID_PATH.read_text().strip())
    except ValueError:
        return None


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def cmd_init(args: argparse.Namespace) -> int:
    _ensure_data_dir()
    if ENV_PATH.exists() and not args.force:
        print(f"{ENV_PATH} already exists. Keeping existing config.")
    else:
        db_path = DATA_DIR / "app.sqlite"
        demo_db_path = DATA_DIR / "demo.sqlite"
        contents = _env_template(
            master_key=_generate_master_key(),
            session_secret=_secrets.token_urlsafe(48),
            db_url=f"sqlite+aiosqlite:///{db_path}",
            demo_db_url=f"sqlite+aiosqlite:///{demo_db_path}",
        )
        ENV_PATH.write_text(contents)
        ENV_PATH.chmod(0o600)
        print(f"✓ Generated {ENV_PATH}")
        print("  - master key (Fernet) and session secret are unique to this install")
        print(f"  - data lives in {DATA_DIR}")
        from app.core.config import load_env_file
        load_env_file(ENV_PATH)
    migrate_code = cmd_migrate(argparse.Namespace())
    if migrate_code != 0:
        return migrate_code
    print()
    print("Next:  dataclaw dashboard")
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    if not ENV_PATH.exists() and not os.environ.get("DATABASE_URL"):
        print("No config found. Running `dataclaw init` first.")
        init_code = cmd_init(argparse.Namespace(force=False))
        if init_code != 0:
            return init_code
    else:
        migrate_code = cmd_migrate(argparse.Namespace())
        if migrate_code != 0:
            return migrate_code
    pid = _read_pid()
    if pid and _process_alive(pid):
        print(f"DataClaw already running (pid {pid}). Run `dataclaw stop` first.")
        return 1
    port = args.port
    host = args.host
    if _port_in_use(port, host):
        print(f"Port {host}:{port} already in use. Pass --port to change.", file=sys.stderr)
        return 1

    env = os.environ.copy()
    env.setdefault("DATACLAW_HOME", str(DATA_DIR))

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        host,
        "--port",
        str(port),
        "--log-level",
        args.log_level,
    ]

    _ensure_data_dir()

    if args.foreground:
        print(f"DataClaw starting on http://{host}:{port}  (Ctrl-C to stop)")
        if not args.no_browser:
            _open_browser_after_ready(host, port)
        proc = subprocess.Popen(cmd, env=env)
        try:
            return proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            return 130

    log = LOG_PATH.open("ab", buffering=0)
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=log,
        stderr=log,
        start_new_session=True,
    )
    PID_PATH.write_text(str(proc.pid))
    if not _wait_for_port(host, port, timeout=20):
        print(f"DataClaw failed to come up. See {LOG_PATH}", file=sys.stderr)
        return 1
    print(f"DataClaw running on http://{host}:{port}  (pid {proc.pid})")
    print(f"  logs:   tail -f {LOG_PATH}")
    print("  stop:   dataclaw stop")
    if not args.no_browser:
        webbrowser.open(f"http://{host}:{port}")
    return 0


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.settimeout(0.5)
                sock.connect((host, port))
                return True
            except OSError:
                time.sleep(0.2)
    return False


def _open_browser_after_ready(host: str, port: int) -> None:
    import threading

    def _watch() -> None:
        if _wait_for_port(host, port, timeout=20):
            webbrowser.open(f"http://{host}:{port}")

    threading.Thread(target=_watch, daemon=True).start()


def cmd_stop(_: argparse.Namespace) -> int:
    pid = _read_pid()
    if pid is None or not _process_alive(pid):
        print("DataClaw is not running.")
        if PID_PATH.exists():
            PID_PATH.unlink()
        return 0
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except ProcessLookupError:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.time() + 5
    while time.time() < deadline and _process_alive(pid):
        time.sleep(0.2)
    if _process_alive(pid):
        os.kill(pid, signal.SIGKILL)
    if PID_PATH.exists():
        PID_PATH.unlink()
    print(f"Stopped DataClaw (pid {pid}).")
    return 0


def cmd_status(_: argparse.Namespace) -> int:
    pid = _read_pid()
    if pid is None or not _process_alive(pid):
        print("status: stopped")
        return 0
    print(f"status: running (pid {pid})")
    print(f"  data:    {DATA_DIR}")
    print(f"  logs:    {LOG_PATH}")
    print(f"  config:  {ENV_PATH}")
    return 0


def cmd_logs(args: argparse.Namespace) -> int:
    if not LOG_PATH.exists():
        print(f"No log file yet at {LOG_PATH}")
        return 0
    if args.follow:
        cmd = ["tail", "-n", "200", "-f", str(LOG_PATH)]
        return subprocess.call(cmd)
    cmd = ["tail", "-n", str(args.lines), str(LOG_PATH)]
    return subprocess.call(cmd)


def cmd_doctor(_: argparse.Namespace) -> int:
    ok = True
    print("dataclaw doctor")
    print(f"  python:        {sys.version.split()[0]}")
    try:
        version = metadata.version("dataclaw-platform")
    except metadata.PackageNotFoundError:
        try:
            version = metadata.version("dataclaw")
        except metadata.PackageNotFoundError:
            version = "unknown (running from source?)"
    print(f"  package:       {version}")
    print(f"  config dir:    {DATA_DIR}  ({'exists' if DATA_DIR.exists() else 'missing'})")
    print(f"  config file:   {ENV_PATH}  ({'exists' if ENV_PATH.exists() else 'missing'})")
    if not ENV_PATH.exists():
        print("    → run `dataclaw init`")
        ok = False
    static = Path(__file__).parent / "static" / "index.html"
    print(f"  bundled UI:    {static.parent}  ({'present' if static.exists() else 'missing'})")
    if not static.exists():
        print("    → frontend not bundled; build with `make bundle-frontend` from the repo")
    pid = _read_pid()
    print(f"  daemon:        {'running pid '+str(pid) if pid and _process_alive(pid) else 'stopped'}")
    return 0 if ok else 1


def cmd_version(_: argparse.Namespace) -> int:
    try:
        version = metadata.version("dataclaw-platform")
    except metadata.PackageNotFoundError:
        try:
            version = metadata.version("dataclaw")
        except metadata.PackageNotFoundError:
            version = "0.0.0+source"
    print(f"dataclaw {version}")
    return 0


async def _bootstrap_admin(email: str, password: str) -> None:
    from sqlalchemy import select

    from app.core.security import password_hash
    from app.db.session import SessionLocal
    from app.models.domain import User
    from app.services.settings_store import update_llm_provider

    async with SessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == email))
        if user is None:
            session.add(User(email=email, password_hash=password_hash(password), is_admin=True))
        else:
            user.password_hash = password_hash(password)
            user.is_admin = True
        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key:
            await update_llm_provider(
                session,
                "openai",
                {"api_key": openai_key, "model": os.getenv("OPENAI_MODEL", "gpt-4.1-mini")},
            )
        await session.commit()


def cmd_bootstrap_admin(args: argparse.Namespace) -> int:
    email = args.email or input("Admin email: ").strip()
    password = args.password
    generated = False
    if not password:
        if args.generate_password:
            password = _secrets.token_urlsafe(18)
            generated = True
        else:
            password = getpass.getpass("Admin password: ")
    if not email or not password:
        print("email and password are required", file=sys.stderr)
        return 1
    asyncio.run(_bootstrap_admin(email, password))
    print(f"Bootstrapped admin user: {email}")
    if generated:
        print(f"Bootstrap admin password: {password}", file=sys.stderr)
    return 0


def cmd_migrate(_: argparse.Namespace) -> int:
    from app.db.migrate import main as run_migrations
    run_migrations()
    return 0


def cmd_verify_mcp_catalog(_: argparse.Namespace) -> int:
    from app.services.mcp_verify import verify_mcp_catalog

    issues = verify_mcp_catalog()
    if not issues:
        print("MCP catalog verification passed.")
        return 0
    print("MCP catalog verification failed:", file=sys.stderr)
    for issue in issues:
        print(f"  - {issue.connector_slug}.{issue.tool_name}: {issue.reason}", file=sys.stderr)
    return 1


def cmd_connectors_list(args: argparse.Namespace) -> int:
    from app.services.connectors.catalog import CATALOG_BY_SLUG

    rows = sorted(CATALOG_BY_SLUG.values(), key=lambda item: (item.category, item.slug))
    if args.json:
        print(
            json.dumps(
                [
                    {
                        "slug": item.slug,
                        "display_name": item.display_name,
                        "category": item.category,
                        "stability": item.stability,
                        "known_issues": item.known_issues,
                        "stability_notes": item.stability_notes,
                    }
                    for item in rows
                ],
                indent=2,
            )
        )
        return 0

    print("slug                 category             stability          display name")
    print("-------------------  -------------------  -----------------  ----------------")
    for item in rows:
        print(f"{item.slug:<19}  {item.category:<19}  {item.stability:<17}  {item.display_name}")
    return 0


async def _compile_workspaces(workspace_id: str | None) -> list[tuple[str, dict]]:
    from sqlalchemy import select

    from app.db.session import SessionLocal
    from app.models.domain import Workspace
    from app.services.knowledge_compile.service import CompileService

    async with SessionLocal() as session:
        if workspace_id:
            workspaces = list((await session.scalars(select(Workspace).where(Workspace.id == workspace_id))).all())
        else:
            workspaces = list((await session.scalars(select(Workspace))).all())
        results: list[tuple[str, dict]] = []
        for workspace in workspaces:
            result = await CompileService(session).compile(workspace.id)
            results.append((workspace.id, result.model_dump()))
        return results


def cmd_compile(args: argparse.Namespace) -> int:
    results = asyncio.run(_compile_workspaces(args.workspace_id))
    if not results:
        print("No workspaces found. Run `dataclaw init` and start DataClaw once to seed the default workspace.", file=sys.stderr)
        return 1
    for workspace_id, result in results:
        print(
            f"{workspace_id}: "
            f"{result['nodes_created']} nodes created, "
            f"{result['nodes_updated']} nodes updated, "
            f"{result['edges_created']} edges created "
            f"({result['runtime_ms']} ms)"
        )
    return 0


async def _rotate_master_key(old_key: str, new_key: str) -> tuple[int, int]:
    from sqlalchemy import select

    from app.core.security import decrypt_json, encrypt_json
    from app.db.session import SessionLocal
    from app.models.domain import AppSetting, Connector

    async with SessionLocal() as session:
        connector_count = 0
        for connector in (await session.scalars(select(Connector))).all():
            if not connector.encrypted_credentials:
                continue
            payload = decrypt_json(old_key, connector.encrypted_credentials)
            connector.encrypted_credentials = encrypt_json(new_key, payload)
            connector_count += 1

        setting_count = 0
        for setting in (await session.scalars(select(AppSetting))).all():
            if not setting.encrypted_value:
                continue
            payload = decrypt_json(old_key, setting.encrypted_value)
            setting.encrypted_value = encrypt_json(new_key, payload)
            setting_count += 1

        await session.commit()
        return connector_count, setting_count


def cmd_rotate_master_key(args: argparse.Namespace) -> int:
    from cryptography.fernet import InvalidToken

    from app.core.config import get_settings
    from app.core.security import DEFAULT_MASTER_KEYS

    get_settings.cache_clear()

    pid = _read_pid()
    if pid and _process_alive(pid):
        print("DataClaw is running. Stop it with `dataclaw stop` before rotating MASTER_KEY.", file=sys.stderr)
        return 1

    old_key = os.environ.get("DATACLAW_OLD_MASTER_KEY") or getpass.getpass("Old master key: ")
    new_key = os.environ.get("DATACLAW_NEW_MASTER_KEY") or getpass.getpass("New master key: ")
    if not old_key or not new_key:
        print("old and new master keys are required", file=sys.stderr)
        return 1
    if old_key == new_key:
        print("old and new master keys must differ", file=sys.stderr)
        return 1
    if new_key in DEFAULT_MASTER_KEYS:
        print("new master key must not be empty or a default placeholder", file=sys.stderr)
        return 1
    try:
        connector_count, setting_count = asyncio.run(_rotate_master_key(old_key, new_key))
    except InvalidToken:
        print("Master key rotation failed: old key could not decrypt every stored payload.", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Master key rotation failed with {type(exc).__name__}.", file=sys.stderr)
        return 1
    print(
        "Rotated master key encrypted payloads: "
        f"{connector_count} connector credentials, {setting_count} app settings."
    )
    print("Update MASTER_KEY in your environment before restarting DataClaw.")
    return 0


async def _import_secrets(path: Path, dry_run: bool) -> int:
    from sqlalchemy import select

    from app.core.config import get_settings
    from app.core.security import encrypt_json
    from app.db.session import SessionLocal
    from app.models.domain import Connector
    from app.services.connectors.adapters import adapter_for
    from app.services.connectors.catalog import CATALOG_BY_SLUG

    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON in {path}: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print(f"{path} must be a JSON object keyed by connector slug.", file=sys.stderr)
        return 1

    unknown = [slug for slug in payload if slug not in CATALOG_BY_SLUG]
    if unknown:
        print(f"Unknown connector slug(s): {', '.join(sorted(unknown))}", file=sys.stderr)
        return 1

    settings = get_settings()
    failures = 0
    async with SessionLocal() as session:
        for slug, creds in payload.items():
            if not isinstance(creds, dict):
                print(f"  {slug}: credentials must be a JSON object — skipped", file=sys.stderr)
                failures += 1
                continue
            try:
                result = await adapter_for(slug).test(creds)
            except Exception as exc:
                print(f"  {slug}: {type(exc).__name__}: {exc}", file=sys.stderr)
                failures += 1
                continue
            print(f"  {slug}: test {result.status} — {result.message}")
            if result.status != "ok":
                failures += 1
                continue
            if dry_run:
                continue
            connector = await session.scalar(select(Connector).where(Connector.slug == slug))
            if connector is None:
                print(
                    f"    {slug}: no connector row exists yet — start DataClaw once to seed it.",
                    file=sys.stderr,
                )
                failures += 1
                continue
            connector.encrypted_credentials = encrypt_json(settings.master_key, creds)
            connector.credential_state = "configured"
            connector.status = result.status
            connector.last_test_message = result.message
        if not dry_run:
            await session.commit()
    return 1 if failures else 0


def cmd_secrets_import(args: argparse.Namespace) -> int:
    path = Path(args.path)
    if not path.exists():
        print(f"{path} does not exist", file=sys.stderr)
        return 1
    return asyncio.run(_import_secrets(path, args.dry_run))


async def _dump(path: Path) -> None:
    from sqlalchemy import select

    from app.db.session import SessionLocal
    from app.models.domain import AppSetting, Connector, User, Workspace

    async with SessionLocal() as session:
        payload = {
            "users": [
                {"email": row.email, "password_hash": row.password_hash, "is_admin": row.is_admin}
                for row in (await session.scalars(select(User))).all()
            ],
            "workspaces": [
                {"name": row.name, "onboarding_complete": row.onboarding_complete}
                for row in (await session.scalars(select(Workspace))).all()
            ],
            "connectors": [
                {
                    "slug": row.slug,
                    "category": row.category,
                    "display_name": row.display_name,
                    "status": row.status,
                    "credential_state": row.credential_state,
                    "encrypted_credentials": row.encrypted_credentials,
                    "sync_summary": row.sync_summary,
                }
                for row in (await session.scalars(select(Connector))).all()
            ],
            "app_settings": [
                {"key": row.key, "encrypted_value": row.encrypted_value}
                for row in (await session.scalars(select(AppSetting))).all()
            ],
        }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def cmd_dump(args: argparse.Namespace) -> int:
    asyncio.run(_dump(Path(args.path)))
    print(f"Wrote {args.path}")
    return 0


def cmd_load(args: argparse.Namespace) -> int:
    path = Path(args.path)
    if not path.exists():
        print(f"{path} does not exist", file=sys.stderr)
        return 1
    print("Load is intentionally conservative in v0; use Alembic plus connector resync for restore.", file=sys.stderr)
    return 1


def main(argv: list[str] | None = None) -> int:
    # Load ~/.dataclaw/.env into os.environ before any settings/config access.
    # Process env wins, so Docker/CI vars still override the on-disk file.
    from app.core.config import load_env_file
    os.environ.setdefault("DATACLAW_HOME", str(DATA_DIR))
    load_env_file(ENV_PATH)

    parser = argparse.ArgumentParser(
        prog="dataclaw",
        description="DataClaw — agentic layer for the modern data stack.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="generate ~/.dataclaw/.env if needed, then run migrations")
    p_init.add_argument("--force", action="store_true", help="overwrite existing config")
    p_init.set_defaults(func=cmd_init)

    p_start = sub.add_parser("start", help="launch backend, open browser")
    p_start.add_argument("--host", default=DEFAULT_HOST)
    p_start.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_start.add_argument("--no-browser", action="store_true", help="don't auto-open the browser")
    p_start.add_argument("--foreground", "-f", action="store_true", help="run in foreground")
    p_start.add_argument("--log-level", default="info")
    p_start.set_defaults(func=cmd_start)

    p_dashboard = sub.add_parser("dashboard", help="alias for `start`")
    p_dashboard.add_argument("--host", default=DEFAULT_HOST)
    p_dashboard.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_dashboard.add_argument("--no-browser", action="store_true")
    p_dashboard.add_argument("--foreground", "-f", action="store_true")
    p_dashboard.add_argument("--log-level", default="info")
    p_dashboard.set_defaults(func=cmd_start)

    p_stop = sub.add_parser("stop", help="stop the running daemon")
    p_stop.set_defaults(func=cmd_stop)

    p_status = sub.add_parser("status", help="print running status")
    p_status.set_defaults(func=cmd_status)

    p_logs = sub.add_parser("logs", help="tail the backend log")
    p_logs.add_argument("-n", "--lines", type=int, default=100)
    p_logs.add_argument("-f", "--follow", action="store_true")
    p_logs.set_defaults(func=cmd_logs)

    p_doctor = sub.add_parser("doctor", help="verify install + dependencies")
    p_doctor.set_defaults(func=cmd_doctor)

    p_version = sub.add_parser("version", help="print version")
    p_version.set_defaults(func=cmd_version)

    p_bootstrap = sub.add_parser("bootstrap-admin", help="create or reset the admin user")
    p_bootstrap.add_argument("--email")
    p_bootstrap.add_argument("--password")
    p_bootstrap.add_argument("--generate-password", action="store_true")
    p_bootstrap.set_defaults(func=cmd_bootstrap_admin)

    p_migrate = sub.add_parser("migrate", help="run database migrations")
    p_migrate.set_defaults(func=cmd_migrate)

    p_verify_mcp_catalog = sub.add_parser("verify-mcp-catalog", help="verify MCP catalog tools have executor backing")
    p_verify_mcp_catalog.set_defaults(func=cmd_verify_mcp_catalog)

    p_connectors = sub.add_parser("connectors", help="connector catalog commands")
    connectors_sub = p_connectors.add_subparsers(dest="connector_cmd", required=True)
    p_connectors_list = connectors_sub.add_parser("list", help="list connector catalog entries")
    p_connectors_list.add_argument("--json", action="store_true", help="print machine-readable catalog JSON")
    p_connectors_list.set_defaults(func=cmd_connectors_list)

    p_compile = sub.add_parser("compile", help="rebuild the knowledge graph for one or all workspaces")
    p_compile.add_argument("--workspace-id", help="compile only one workspace")
    p_compile.set_defaults(func=cmd_compile)

    p_mcp = sub.add_parser("mcp", help="MCP catalog commands")
    mcp_sub = p_mcp.add_subparsers(dest="mcp_cmd", required=True)
    p_mcp_verify = mcp_sub.add_parser("verify", help="verify MCP catalog tools have executor backing")
    p_mcp_verify.set_defaults(func=cmd_verify_mcp_catalog)

    p_rotate = sub.add_parser("rotate-master-key", help="re-encrypt stored secrets with a new master key")
    p_rotate.set_defaults(func=cmd_rotate_master_key)

    p_secrets = sub.add_parser("secrets", help="manage connector credentials")
    secrets_sub = p_secrets.add_subparsers(dest="secrets_cmd", required=True)
    p_secrets_import = secrets_sub.add_parser(
        "import",
        help="load connector credentials from a local JSON file",
        description=(
            "Read a JSON file shaped like {\"slug\": {credential_fields}} and "
            "test+persist each entry. Avoids pasting secrets through untrusted "
            "channels. Run while DataClaw is stopped or accept normal SQLite "
            "concurrency."
        ),
    )
    p_secrets_import.add_argument("path", help="path to the JSON file")
    p_secrets_import.add_argument("--dry-run", action="store_true", help="test connections without persisting")
    p_secrets_import.set_defaults(func=cmd_secrets_import)

    p_dump = sub.add_parser("dump", help="write a conservative JSON backup")
    p_dump.add_argument("path")
    p_dump.set_defaults(func=cmd_dump)

    p_load = sub.add_parser("load", help="restore a JSON backup")
    p_load.add_argument("path")
    p_load.set_defaults(func=cmd_load)

    args = parser.parse_args(argv)
    return args.func(args)


cli = main


if __name__ == "__main__":
    sys.exit(main())
