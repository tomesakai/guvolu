"""长期部署的 holdout 预检任务必须严格分离代码、数据与运行时。"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Final

import pytest

from guvolu.research import governance as governance_module


POWERSHELL: Final = shutil.which("powershell.exe")
GIT: Final = shutil.which("git.exe") or shutil.which("git")
SOURCE_ROOT: Final = Path(__file__).resolve().parents[1]
VINTAGE_ID: Final = "holdout-vintage-" + "a" * 64


def _run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        **kwargs,
    )


def _git(repository: Path, *args: str) -> str:
    result = _run(["git", "-C", str(repository), *args])
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree_identity(root: Path) -> tuple[str, int, int]:
    lines = bytearray()
    total = 0
    files = sorted(
        (path.relative_to(root).as_posix(), path)
        for path in root.rglob("*")
        if path.is_file()
    )
    for relative, path in files:
        body = path.read_bytes()
        total += len(body)
        lines.extend(relative.encode("utf-8"))
        lines.extend(b"\0" + str(len(body)).encode("ascii") + b"\0")
        lines.extend(hashlib.sha256(body).hexdigest().encode("ascii") + b"\n")
    return hashlib.sha256(lines).hexdigest(), len(files), total


def _pyvenv_home(config: Path) -> Path:
    for line in config.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip().lower() == "home":
            return Path(value.strip()).resolve()
    raise AssertionError("test pyvenv.cfg has no home")


def _fake_preflight_text(*, real_governance: bool = False) -> str:
    text = """from __future__ import annotations
import argparse
import importlib.machinery
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

__GOVERNANCE_IMPORT__

parser = argparse.ArgumentParser()
parser.add_argument("--root", type=Path, required=True)
parser.add_argument("--registry", type=Path, required=True)
parser.add_argument("--json-output", type=Path, required=True)
parser.add_argument("--vintage-id")
args = parser.parse_args()
controls_path = args.root / "test-preflight-controls.json"
controls = (
    json.loads(controls_path.read_text(encoding="utf-8"))
    if controls_path.exists()
    else {}
)
snapshot_value = ""
snapshot_aux_present = False
real_governance_journal_mode = ""
real_governance_row_count = ""
snapshot_query = controls.get("snapshot_query")
if snapshot_query:
    connection = sqlite3.connect(
        args.registry.resolve().as_uri() + "?mode=ro&cache=private",
        uri=True,
        isolation_level=None,
    )
    try:
        connection.execute("PRAGMA query_only=ON")
        if connection.execute("PRAGMA query_only").fetchone() != (1,):
            raise RuntimeError("test runner snapshot connection is not query_only")
        value = connection.execute(snapshot_query).fetchone()
        snapshot_value = "" if value is None else repr(value[0])
    finally:
        connection.close()
    snapshot_aux_present = any(
        Path(str(args.registry) + suffix).exists()
        for suffix in ("-wal", "-shm", "-journal")
    )
if controls.get("real_governance_reader"):
    real_connection = governance_module._connect_read_only(args.registry)
    try:
        real_governance_journal_mode = real_connection.execute(
            "PRAGMA journal_mode"
        ).fetchone()[0]
        real_governance_row_count = str(
            real_connection.execute(
                "SELECT COUNT(*) FROM research_exposure"
            ).fetchone()[0]
        )
    finally:
        real_connection.close()
    snapshot_aux_present = snapshot_aux_present or any(
        Path(str(args.registry) + suffix).exists()
        for suffix in ("-wal", "-shm", "-journal")
    )

import_attack_mode = controls.get("import_attack_mode")
if import_attack_mode:
    Path(controls["import_attack_ready"]).write_text("ready", encoding="utf-8")
    time.sleep(float(controls.get("import_attack_delay", 1.0)))
    module_name = controls["import_attack_module"]
    if import_attack_mode == "normal":
        __import__(module_name)
    elif import_attack_mode == "direct-pathfinder":
        spec = importlib.machinery.PathFinder.find_spec(
            module_name,
            [controls["import_attack_search_path"]],
        )
        if spec is None or spec.loader is None:
            raise ModuleNotFoundError(module_name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    else:
        raise RuntimeError("unknown import attack mode")

snapshot_attack = controls.get("snapshot_attack_result")
if snapshot_attack:
    attack = r'''import json
import os
import sys
from pathlib import Path

target = Path(sys.argv[1])
replacement = Path(str(target) + ".attacker")
results = {}
try:
    with target.open("r+b", buffering=0) as stream:
        stream.write(b"ATTACK")
    results["write"] = True
except OSError:
    results["write"] = False
try:
    target.unlink()
    results["delete"] = True
except OSError:
    results["delete"] = False
try:
    replacement.write_bytes(b"replacement")
    os.replace(replacement, target)
    results["replace"] = True
except OSError:
    results["replace"] = False
finally:
    try:
        replacement.unlink()
    except OSError:
        pass
Path(sys.argv[2]).write_text(json.dumps(results), encoding="utf-8")
'''
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-B", "-X", "utf8", "-c", attack,
         str(args.registry), snapshot_attack],
        check=False,
        timeout=10,
    )
    if completed.returncode != 0:
        raise RuntimeError("snapshot attack subprocess did not complete")

capture = controls.get("capture")
if capture:
    probed_environment = sorted(
        name
        for name in controls.get("probe_environment_names", [])
        if name in os.environ
    )
    Path(capture).write_text(
        "ROOT=" + str(args.root.resolve()) + "\\n"
        "REGISTRY=" + str(args.registry.resolve()) + "\\n"
        "PYTHONPATH=" + os.environ.get("PYTHONPATH", "") + "\\n"
        "FLAGS=" + repr((sys.flags.isolated, sys.flags.no_site,
                          sys.flags.dont_write_bytecode, sys.flags.utf8_mode)) + "\\n"
        "PYCACHE=" + str(sys.pycache_prefix) + "\\n"
        "GOVERNANCE_ORIGIN=" + ORIGIN + "\\n"
        "SNAPSHOT_VALUE=" + snapshot_value + "\\n"
        "SNAPSHOT_AUX_PRESENT=" + repr(snapshot_aux_present) + "\\n"
        "REAL_GOVERNANCE_JOURNAL_MODE=" + real_governance_journal_mode + "\\n"
        "REAL_GOVERNANCE_ROW_COUNT=" + real_governance_row_count + "\\n"
        "PROBED_ENVIRONMENT=" + repr(probed_environment) + "\\n",
        encoding="utf-8",
    )
marker = controls.get("child_marker")
if marker:
    subprocess.Popen([sys.executable, "-I", "-S", "-B", "-c",
        "import sys,time,pathlib;time.sleep(20);pathlib.Path(sys.argv[1]).write_text('orphan')",
        marker])
output_bytes = int(controls.get("output_bytes", 0))
if output_bytes:
    stream = sys.stderr if controls.get("output_stream") == "stderr" else sys.stdout
    stream.write("X" * output_bytes)
    stream.flush()
sleep_seconds = float(controls.get("sleep_seconds", 0))
if sleep_seconds:
    time.sleep(sleep_seconds)
if controls.get("force_guard_close_failure"):
    frame = sys._getframe()
    while frame is not None and "kernel" not in frame.f_globals:
        frame = frame.f_back
    if frame is None:
        raise RuntimeError("test could not locate isolated launcher globals")
    frame.f_globals["kernel"].CloseHandle = lambda _handle: 0
    raise RuntimeError("PRIMARY_BUSINESS_FAILURE_MUST_WIN")
args.json_output.write_text(
    json.dumps({"status": "healthy", "read_only": True}) + "\\n",
    encoding="utf-8",
)
print("隔离预检成功")
if controls.get("chinese_stderr"):
    print("中文标准错误", file=sys.stderr)
"""
    governance_import = (
        "from guvolu.research import governance as governance_module\n"
        'ORIGIN = "clean-code-root-real-governance"'
        if real_governance
        else "from guvolu.research.governance import ORIGIN\n"
        "governance_module = None"
    )
    return text.replace("__GOVERNANCE_IMPORT__", governance_import)


def _code_repository(
    tmp_path: Path,
    *,
    detached: bool = True,
    runnable: bool = False,
    real_governance: bool = False,
) -> tuple[Path, Path, str]:
    """复制当前脚本到独立 git 仓库并封成 detached clean HEAD。"""
    code_root = tmp_path / "detached code root"
    scripts = code_root / "scripts"
    code_source = code_root / "src"
    python_dir = code_root / ".venv" / "Scripts"
    site_packages = code_root / ".venv" / "Lib" / "site-packages"
    scripts.mkdir(parents=True)
    (code_source / "guvolu" / "research").mkdir(parents=True)
    python_dir.mkdir(parents=True)
    site_packages.mkdir(parents=True)
    for name in (
        "register_holdout_preflight_task.ps1",
        "run_holdout_preflight_task.ps1",
    ):
        shutil.copy2(SOURCE_ROOT / "scripts" / name, scripts / name)
    (scripts / "preflight_holdout.py").write_text(
        _fake_preflight_text(real_governance=real_governance),
        encoding="utf-8",
    )
    if real_governance:
        shutil.copytree(
            SOURCE_ROOT / "src" / "guvolu",
            code_source / "guvolu",
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        # The production research package initializer eagerly imports the full
        # panel pipeline (and therefore optional analytical dependencies).  This
        # fixture exercises the real governance module and its direct dependency
        # graph under ``-S`` without broadening the isolated launcher's trusted
        # site-packages surface.
        (code_source / "guvolu" / "research" / "__init__.py").write_text(
            '"""Minimal package initializer for the governance reader test."""\n',
            encoding="utf-8",
        )
    else:
        (code_source / "guvolu" / "__init__.py").write_text(
            "", encoding="utf-8"
        )
        (code_source / "guvolu" / "research" / "__init__.py").write_text(
            "", encoding="utf-8",
        )
        (code_source / "guvolu" / "research" / "governance.py").write_text(
            'ORIGIN = "clean-code-root"\n', encoding="utf-8",
        )
    python = python_dir / "python.exe"
    pyvenv_config = code_root / ".venv" / "pyvenv.cfg"
    if runnable:
        shutil.copy2(Path(sys.executable), python)
        pyvenv_config.write_text(
            f"home = {Path(sys.base_prefix).resolve()}\n"
            "include-system-site-packages = false\n",
            encoding="utf-8",
        )
        base_probe = _run([
            str(python), "-I", "-S", "-B", "-X", "utf8", "-c",
            "import sys;print(sys.base_prefix)",
        ])
        assert base_probe.returncode == 0, base_probe.stderr
        base_runtime = Path(base_probe.stdout.strip()).resolve()
    else:
        python.write_bytes(b"test-only-not-executable")
        base_runtime = tmp_path / "bound base Python"
        (base_runtime / "DLLs").mkdir(parents=True)
        (base_runtime / "Lib").mkdir()
        (base_runtime / "python-test.zip").write_bytes(b"bound-base")
    pyvenv_config.write_text(
        f"home = {base_runtime}\ninclude-system-site-packages = false\n",
        encoding="utf-8",
    )
    (code_root / ".gitignore").write_text(
        ".venv/\nscripts/__pycache__/\nsrc/**/__pycache__/\n",
        encoding="utf-8",
    )
    _git(code_root.parent, "init", str(code_root))
    _git(code_root, "config", "user.email", "tests@example.invalid")
    _git(code_root, "config", "user.name", "Tests")
    _git(code_root, "config", "core.autocrlf", "false")
    _git(code_root, "add", "scripts", "src", ".gitignore")
    _git(code_root, "commit", "-m", "sealed preflight code")
    head = _git(code_root, "rev-parse", "HEAD")
    if detached:
        _git(code_root, "checkout", "--detach", head)
    return code_root, python, head


def _data_roots(tmp_path: Path) -> tuple[Path, Path]:
    live_repository = tmp_path / "live data repository"
    runtime = tmp_path / "frozen D runtime"
    (live_repository / "data").mkdir(parents=True)
    (runtime / "src").mkdir(parents=True)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    registry.parent.mkdir(parents=True)
    connection = sqlite3.connect(registry)
    try:
        connection.execute("CREATE TABLE authority_marker(value TEXT NOT NULL)")
        connection.execute("INSERT INTO authority_marker VALUES ('D-authority')")
        connection.commit()
    finally:
        connection.close()
    fake_live_registry = (
        live_repository / "data" / "research" / "governance.sqlite3"
    )
    fake_live_registry.parent.mkdir(parents=True)
    connection = sqlite3.connect(fake_live_registry)
    try:
        connection.execute("CREATE TABLE decoy_marker(value TEXT NOT NULL)")
        connection.execute("INSERT INTO decoy_marker VALUES ('must-never-read-main')")
        connection.commit()
    finally:
        connection.close()
    _install_copied_wal_store(tmp_path, registry, zero_wal=True)
    return live_repository, runtime


def _write_test_controls(runtime: Path, **controls: object) -> None:
    (runtime / "test-preflight-controls.json").write_text(
        json.dumps(controls),
        encoding="utf-8",
    )


def _governance_store_bytes(registry: Path) -> dict[str, bytes | None]:
    return {
        suffix: (
            Path(str(registry) + suffix).read_bytes()
            if Path(str(registry) + suffix).exists()
            else None
        )
        for suffix in ("", "-wal", "-shm", "-journal")
    }


def _install_copied_wal_store(
    tmp_path: Path,
    registry: Path,
    *,
    zero_wal: bool,
) -> None:
    """复制一个无活动句柄、但 sidecar 状态真实一致的 WAL vintage。"""
    staging = tmp_path / ("staging-zero-wal.sqlite3" if zero_wal else "staging-wal.sqlite3")
    connection = sqlite3.connect(staging)
    try:
        connection.execute("CREATE TABLE authority_marker(value TEXT NOT NULL)")
        connection.execute("INSERT INTO authority_marker VALUES ('D-authority')")
        connection.commit()
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("INSERT INTO authority_marker VALUES ('wal-committed')")
        connection.commit()
        if zero_wal:
            assert connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (
                0,
                0,
                0,
            )
        wal = Path(str(staging) + "-wal")
        shm = Path(str(staging) + "-shm")
        assert wal.exists() and shm.exists()
        if zero_wal:
            assert wal.stat().st_size == 0
        for suffix in ("", "-wal", "-shm", "-journal"):
            target = Path(str(registry) + suffix)
            if target.exists():
                target.unlink()
        shutil.copyfile(staging, registry)
        shutil.copyfile(wal, Path(str(registry) + "-wal"))
        shutil.copyfile(shm, Path(str(registry) + "-shm"))
    finally:
        connection.close()
        for suffix in ("", "-wal", "-shm", "-journal"):
            candidate = Path(str(staging) + suffix)
            if candidate.exists():
                candidate.unlink()


def _install_fk_violation_store(tmp_path: Path, registry: Path) -> None:
    staging = tmp_path / "staging-fk-violation.sqlite3"
    connection = sqlite3.connect(staging)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("CREATE TABLE authority_marker(value TEXT NOT NULL)")
        connection.execute("INSERT INTO authority_marker VALUES ('D-authority')")
        connection.execute("CREATE TABLE fk_parent(id INTEGER PRIMARY KEY)")
        connection.execute(
            "CREATE TABLE fk_child(parent_id INTEGER REFERENCES fk_parent(id))"
        )
        connection.execute("INSERT INTO fk_child VALUES (404)")
        connection.commit()
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        assert connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (
            0,
            0,
            0,
        )
        for suffix in ("", "-wal", "-shm", "-journal"):
            target = Path(str(registry) + suffix)
            if target.exists():
                target.unlink()
        shutil.copyfile(staging, registry)
        shutil.copyfile(
            Path(str(staging) + "-wal"), Path(str(registry) + "-wal")
        )
        shutil.copyfile(
            Path(str(staging) + "-shm"), Path(str(registry) + "-shm")
        )
    finally:
        connection.close()
        for suffix in ("", "-wal", "-shm", "-journal"):
            candidate = Path(str(staging) + suffix)
            if candidate.exists():
                candidate.unlink()


def _install_v9_wal_store(
    tmp_path: Path,
    registry: Path,
    *,
    page_size: int,
    initial_rows: int,
    committed_wal_row: bool,
) -> int:
    staging = tmp_path / (
        f"staging-v9-{page_size}-{initial_rows}-{int(committed_wal_row)}.sqlite3"
    )
    bootstrap = sqlite3.connect(staging)
    try:
        bootstrap.execute(f"PRAGMA page_size={page_size}")
        bootstrap.execute("VACUUM")
    finally:
        bootstrap.close()
    connection = governance_module._connect(staging, write=True)
    try:
        assert connection.execute("PRAGMA page_size").fetchone()[0] == page_size
        rows = [
            (
                f"exposure-{index:06d}",
                f"identity-{index:06d}",
                "BTC-USDT",
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
                "2026-01-03T00:00:00+00:00",
            )
            for index in range(initial_rows)
        ]
        connection.executemany(
            "INSERT INTO research_exposure("
            "exposure_id,research_identity,market_id,start_time,end_time,recorded_at"
            ") VALUES(?,?,?,?,?,?)",
            rows,
        )
        checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        assert checkpoint is not None and checkpoint[0] == 0
        expected_rows = initial_rows
        if committed_wal_row:
            connection.execute(
                "INSERT INTO research_exposure("
                "exposure_id,research_identity,market_id,start_time,end_time,recorded_at"
                ") VALUES(?,?,?,?,?,?)",
                (
                    "exposure-wal-committed",
                    "identity-wal-committed",
                    "BTC-USDT",
                    "2026-02-01T00:00:00+00:00",
                    "2026-02-02T00:00:00+00:00",
                    "2026-02-03T00:00:00+00:00",
                ),
            )
            expected_rows += 1
        for suffix in ("", "-wal", "-shm", "-journal"):
            target = Path(str(registry) + suffix)
            if target.exists():
                target.unlink()
        shutil.copyfile(staging, registry)
        shutil.copyfile(
            Path(str(staging) + "-wal"), Path(str(registry) + "-wal")
        )
        shutil.copyfile(
            Path(str(staging) + "-shm"), Path(str(registry) + "-shm")
        )
        return expected_rows
    finally:
        connection.close()
        for suffix in ("", "-wal", "-shm", "-journal"):
            candidate = Path(str(staging) + suffix)
            if candidate.exists():
                candidate.unlink()


def _start_authority_file_attack(
    target: Path,
    operation: str,
    result_path: Path,
) -> subprocess.Popen[str]:
    attack = r"""import os
import sys
from pathlib import Path

target = Path(sys.argv[1])
operation = sys.argv[2]
result = Path(sys.argv[3])
try:
    if operation == "write":
        with target.open("r+b", buffering=0) as stream:
            stream.seek(0)
            stream.write(b"MUTATE")
    elif operation == "delete":
        target.unlink()
    elif operation == "create":
        with target.open("xb", buffering=0) as stream:
            stream.write(b"TRANSIENT")
    else:
        raise RuntimeError("unknown attack operation")
    result.write_text("succeeded-after-release", encoding="utf-8")
except OSError as error:
    result.write_text("denied:" + str(error.winerror), encoding="utf-8")
"""
    return subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-S",
            "-B",
            "-c",
            attack,
            str(target),
            operation,
            str(result_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )


def _registration_args(
    code_root: Path,
    python: Path,
    head: str,
    live_repository: Path,
    runtime: Path,
) -> list[str]:
    assert POWERSHELL is not None
    assert GIT is not None
    return [
        POWERSHELL,
        "-NoProfile",
        "-File",
        str(code_root / "scripts" / "register_holdout_preflight_task.ps1"),
        "-Repository",
        str(live_repository),
        "-RuntimeRoot",
        str(runtime),
        "-PythonExecutable",
        str(python),
        "-GitExecutable",
        str(Path(GIT).resolve()),
        "-ExpectedCodeHead",
        head,
    ]


def _decode_bootstrap(arguments: str) -> str:
    marker = "-EncodedCommand "
    assert arguments.count(marker) == 1
    encoded = arguments.split(marker, maxsplit=1)[1]
    return base64.b64decode(encoded).decode("utf-16-le")


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_describe_uses_detached_code_root_and_never_calls_task_cmdlets(
    tmp_path: Path,
) -> None:
    assert GIT is not None
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    harness = tmp_path / "describe-only-harness.ps1"
    harness.write_text(
        """$ErrorActionPreference = 'Stop'
function Get-ScheduledTask { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function New-ScheduledTaskAction { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function New-ScheduledTaskTrigger { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function New-ScheduledTaskPrincipal { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function New-ScheduledTaskSettingsSet { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function Register-ScheduledTask { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function Export-ScheduledTask { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function Disable-ScheduledTask { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
function Stop-ScheduledTask { throw 'SCHEDULED_TASK_CMDLET_CALLED' }
& $args[0] -Repository $args[1] -RuntimeRoot $args[2] `
    -PythonExecutable $args[3] -GitExecutable $args[6] `
    -ExpectedCodeHead $args[4] `
    -VintageId $args[5] -DailyAt '07:25' -DescribeOnly
""",
        encoding="utf-8",
    )
    result = _run(
        [
            POWERSHELL,
            "-NoProfile",
            "-File",
            str(harness),
            str(code_root / "scripts" / "register_holdout_preflight_task.ps1"),
            str(live_repository),
            str(runtime),
            str(python),
            head,
            VINTAGE_ID,
            str(Path(GIT).resolve()),
        ],
    )
    assert result.returncode == 0, result.stderr
    assert "SCHEDULED_TASK_CMDLET_CALLED" not in result.stderr
    definition = json.loads(result.stdout)
    bootstrap = _decode_bootstrap(definition["arguments"])
    wrapper = code_root / "scripts" / "run_holdout_preflight_task.ps1"
    governance_runner = code_root / "scripts" / "preflight_holdout.py"

    assert definition["task_name"] == "guvolu-holdout-preflight"
    assert definition["working_directory"] == str(code_root.resolve())
    assert definition["code_root"] == str(code_root.resolve())
    assert definition["live_repository"] == str(live_repository.resolve())
    assert definition["runtime_root"] == str(runtime.resolve())
    assert definition["authoritative_data_root"] == str(runtime.resolve())
    assert definition["authoritative_governance_registry"] == str(
        runtime.resolve() / "data" / "research" / "governance.sqlite3",
    )
    assert definition["authority_sidecar_precondition"] == (
        "db+wal+shm-preexisting;rollback-journal-absent"
    )
    assert definition["registry_snapshot_journal_mode"] == "delete"
    assert definition["registry_snapshot_auxiliaries"] == (
        "absent-before-and-after-business"
    )
    assert definition["python_executable"] == str(python.resolve())
    assert definition["expected_code_head"] == head
    assert definition["actual_code_head"] == head
    assert definition["task_runner"] == str(wrapper.resolve())
    assert definition["task_runner_sha256"] == _sha256(wrapper)
    assert definition["governance_runner"] == str(governance_runner.resolve())
    assert definition["governance_runner_sha256"] == _sha256(governance_runner)
    assert definition["python_sha256"] == _sha256(python)
    assert definition["daily_at_local"] == "07:25"
    assert definition["vintage_id"] == VINTAGE_ID
    assert definition["enabled"] is False
    assert definition["state"] == "Disabled"
    assert definition["unattended_coverage_capable"] is False
    assert definition["principal_logon_type"] == "Interactive"
    assert definition["multiple_instances"] == "IgnoreNew"
    assert definition["start_when_available"] is True
    assert definition["allow_start_on_batteries"] is True
    assert definition["dont_stop_if_going_on_batteries"] is True
    assert definition["wake_to_run"] is True
    assert definition["execution_time_limit_minutes"] == 30
    assert definition["execution_timeout_seconds"] == 1500
    assert definition["environment_attestation"] == "partial"
    assert definition["child_environment"] == "minimal-nonsecret-allowlist"
    assert "restricted-handle-list" in definition["process_guard"]
    assert definition["python_startup"].startswith("-I -S -B -X utf8")
    assert Path(definition["execute"]).is_absolute()
    assert definition["execute"].lower().endswith(
        r"\system32\windowspowershell\v1.0\powershell.exe",
    )
    assert definition["restart_count"] == 0
    assert "[System.IO.File]::Open($wrapper" in bootstrap
    assert "preflight wrapper bootstrap hash mismatch" in bootstrap
    assert "finally { $stream.Dispose() }" in bootstrap
    assert f"Repository = '{live_repository.resolve()}'" in bootstrap
    assert f"RuntimeRoot = '{runtime.resolve()}'" in bootstrap
    assert f"PythonExecutable = '{python.resolve()}'" in bootstrap
    assert f"ExpectedCodeHead = '{head}'" in bootstrap
    assert f"ExpectedWrapperSha256 = '{_sha256(wrapper)}'" in bootstrap
    assert "ExpectedBaseRuntimeTreeSha256 = '" in bootstrap
    assert "ExpectedCodeSourceTreeSha256 = '" in bootstrap
    assert "ExecutionTimeoutSeconds = '1500'" in bootstrap


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("vintage", VINTAGE_ID + " --help", "canonical holdout vintage"),
        ("head", "not-a-head", "ParameterArgumentValidationError"),
    ],
)
def test_registration_rejects_malformed_identities(
    tmp_path: Path,
    field: str,
    value: str,
    expected_error: str,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    args = _registration_args(
        code_root, python, head, live_repository, runtime,
    )
    if field == "head":
        args[args.index("-ExpectedCodeHead") + 1] = value
    else:
        args += ["-VintageId", value]
    args.append("-DescribeOnly")
    result = _run(args)
    assert result.returncode != 0
    assert expected_error in result.stderr


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize("condition", ["branch", "dirty", "head"])
def test_registration_rejects_unsealed_code_identity(
    tmp_path: Path,
    condition: str,
) -> None:
    code_root, python, head = _code_repository(
        tmp_path,
        detached=condition != "branch",
    )
    live_repository, runtime = _data_roots(tmp_path)
    if condition == "dirty":
        with (code_root / "scripts" / "preflight_holdout.py").open(
            "a", encoding="utf-8",
        ) as handle:
            handle.write("# tracked dirt\n")
    if condition == "head":
        head = "0" * 40
    result = _run(
        _registration_args(
            code_root, python, head, live_repository, runtime,
        ) + ["-DescribeOnly"],
    )
    assert result.returncode != 0
    expected = {
        "branch": "must be detached",
        "dirty": "tracked changes or non-ignored untracked files",
        "head": "HEAD mismatch",
    }[condition]
    assert expected in result.stderr


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize("alias", ["repository", "runtime", "python"])
def test_registration_rejects_code_data_or_python_boundary_aliases(
    tmp_path: Path,
    alias: str,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    if alias == "repository":
        live_repository = code_root
    elif alias == "runtime":
        runtime = code_root
    else:
        external = tmp_path / "external python" / "Scripts"
        external.mkdir(parents=True)
        python = external / "python.exe"
        python.write_bytes(b"external-test-python")
    result = _run(
        _registration_args(
            code_root, python, head, live_repository, runtime,
        ) + ["-DescribeOnly"],
    )
    assert result.returncode != 0
    assert (
        "separate, non-nested roots" in result.stderr
        or "must be inside CodeRoot" in result.stderr
    )


def _mock_harness_text() -> str:
    """返回纯函数 mock；绝不加载真实 ScheduledTasks 模块。"""
    return r"""$ErrorActionPreference = 'Stop'
$global:MockGetCount = 0
$global:MockAction = $null
$global:MockTrigger = $null
$global:MockPrincipal = $null
$global:MockSettings = $null
$global:MockMode = $args[6]
function Get-ScheduledTask {
    param($TaskName, $ErrorAction)
    $global:MockGetCount += 1
    if ($global:MockGetCount -eq 1) {
        if ($global:MockMode -eq 'unsafe-existing') {
            return [pscustomobject]@{
                TaskName = 'guvolu-holdout-preflight'
                TaskPath = '\'
                State = 'Ready'
                Settings = [pscustomobject]@{ Enabled = $true }
            }
        }
        return
    }
    $Action = [pscustomobject]@{
        Execute = $global:MockAction.Execute
        Arguments = $global:MockAction.Arguments
        WorkingDirectory = $global:MockAction.WorkingDirectory
    }
    $Trigger = [pscustomobject]@{
        StartBoundary = $global:MockTrigger.StartBoundary
        DaysInterval = $global:MockTrigger.DaysInterval
        Enabled = $true
    }
    $Settings = [pscustomobject]@{
        MultipleInstances = 'IgnoreNew'
        StartWhenAvailable = $true
        DisallowStartIfOnBatteries = $false
        StopIfGoingOnBatteries = $false
        WakeToRun = $true
        ExecutionTimeLimit = 'PT30M'
        Hidden = $true
        Enabled = $false
        RestartCount = 0
        RestartInterval = $null
    }
    $State = 'Disabled'
    if ($global:MockMode -in @('action-drift', 'cleanup-errors')) {
        $Action.Arguments += ' drift'
    }
    if ($global:MockMode -eq 'trigger-drift') { $Trigger.DaysInterval = 2 }
    if ($global:MockMode -eq 'post-enabled') {
        $Settings.Enabled = $true
        $State = 'Ready'
    }
    return [pscustomobject]@{
        TaskName = 'guvolu-holdout-preflight'
        TaskPath = '\'
        State = $State
        Actions = @($Action)
        Triggers = @($Trigger)
        Principal = [pscustomobject]@{
            UserId = $global:MockPrincipal.UserId
            LogonType = 'Interactive'
            RunLevel = 'Limited'
        }
        Settings = $Settings
    }
}
function New-ScheduledTaskAction {
    param($Execute, $Argument, $WorkingDirectory)
    $global:MockAction = [pscustomobject]@{
        Execute = $Execute
        Arguments = $Argument
        WorkingDirectory = $WorkingDirectory
    }
    return $global:MockAction
}
function New-ScheduledTaskTrigger {
    param([switch]$Daily, [datetime]$At, [int]$DaysInterval)
    $global:MockTrigger = [pscustomobject]@{
        StartBoundary = $At.ToString('yyyy-MM-ddTHH:mm:sszzz')
        DaysInterval = $DaysInterval
        Enabled = $true
    }
    return $global:MockTrigger
}
function New-ScheduledTaskPrincipal {
    param($UserId, $LogonType, $RunLevel)
    $global:MockPrincipal = [pscustomobject]@{
        UserId = $UserId
        LogonType = 'Interactive'
        RunLevel = 'Limited'
    }
    return $global:MockPrincipal
}
function New-ScheduledTaskSettingsSet {
    param(
        $MultipleInstances,
        [switch]$StartWhenAvailable,
        [switch]$AllowStartIfOnBatteries,
        [switch]$DontStopIfGoingOnBatteries,
        [switch]$WakeToRun,
        $ExecutionTimeLimit,
        [switch]$Hidden,
        [switch]$Disable
    )
    $global:MockSettings = [pscustomobject]@{
        MultipleInstances = 'IgnoreNew'
        StartWhenAvailable = $true
        DisallowStartIfOnBatteries = $false
        StopIfGoingOnBatteries = $false
        WakeToRun = $true
        ExecutionTimeLimit = 'PT30M'
        Hidden = $true
        Enabled = $false
        RestartCount = 0
        RestartInterval = $null
    }
    return $global:MockSettings
}
function Register-ScheduledTask {
    param(
        $TaskName, $TaskPath, $Action, $Trigger, $Principal, $Settings,
        [switch]$Force
    )
    if ($global:MockMode -eq 'register-called-unsafely') {
        throw 'REGISTER_CALLED_UNSAFELY'
    }
}
function ConvertTo-XmlEscaped { param([string]$Text)
    return [System.Security.SecurityElement]::Escape($Text)
}
function Export-ScheduledTask {
    param($TaskName, $TaskPath, $ErrorAction)
    $Action = $global:MockAction
    $Trigger = $global:MockTrigger
    if ($global:MockMode -in @('action-drift', 'cleanup-errors', 'xml-action-drift')) {
        $Action = [pscustomobject]@{
            Execute = $Action.Execute
            Arguments = $Action.Arguments + ' drift'
            WorkingDirectory = $Action.WorkingDirectory
        }
    }
    if ($global:MockMode -in @('trigger-drift', 'xml-trigger-drift')) {
        $Trigger = [pscustomobject]@{
            StartBoundary = $Trigger.StartBoundary
            DaysInterval = 2
        }
    }
    $Sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $Command = ConvertTo-XmlEscaped ([string]$Action.Execute)
    $Arguments = ConvertTo-XmlEscaped ([string]$Action.Arguments)
    $WorkingDirectory = ConvertTo-XmlEscaped ([string]$Action.WorkingDirectory)
    $Enabled = if ($global:MockMode -eq 'post-enabled') { 'true' } else { 'false' }
    return @"
<?xml version="1.0" encoding="UTF-16"?>
<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers><CalendarTrigger>
    <StartBoundary>$($Trigger.StartBoundary)</StartBoundary>
    <Enabled>true</Enabled>
    <ScheduleByDay><DaysInterval>$($Trigger.DaysInterval)</DaysInterval></ScheduleByDay>
  </CalendarTrigger></Triggers>
  <Principals><Principal>
    <UserId>$Sid</UserId><LogonType>InteractiveToken</LogonType>
    <RunLevel>LeastPrivilege</RunLevel>
  </Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable><WakeToRun>true</WakeToRun>
    <ExecutionTimeLimit>PT30M</ExecutionTimeLimit><Hidden>true</Hidden>
    <Enabled>$Enabled</Enabled>
  </Settings>
  <Actions><Exec><Command>$Command</Command><Arguments>$Arguments</Arguments>
    <WorkingDirectory>$WorkingDirectory</WorkingDirectory></Exec></Actions>
</Task>
"@
}
function Disable-ScheduledTask {
    param($TaskName, $TaskPath, $ErrorAction)
    [Console]::Error.WriteLine('DISABLE_CLEANUP_CALLED')
    if ($global:MockMode -eq 'cleanup-errors') { throw 'DISABLE_CLEANUP_FAILED' }
}
function Stop-ScheduledTask {
    param($TaskName, $TaskPath, $ErrorAction)
    [Console]::Error.WriteLine('STOP_CLEANUP_CALLED')
    if ($global:MockMode -eq 'cleanup-errors') { throw 'STOP_CLEANUP_FAILED' }
}
& $args[0] -Repository $args[1] -RuntimeRoot $args[2] `
    -PythonExecutable $args[3] -GitExecutable $args[7] `
    -ExpectedCodeHead $args[4] -DailyAt $args[5]
"""


def _run_mock_registration(
    tmp_path: Path,
    mode: str,
) -> subprocess.CompletedProcess[str]:
    assert POWERSHELL is not None
    assert GIT is not None
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    harness = tmp_path / "scheduled-task-mock.ps1"
    harness.write_text(_mock_harness_text(), encoding="utf-8")
    return _run(
        [
            POWERSHELL,
            "-NoProfile",
            "-File",
            str(harness),
            str(code_root / "scripts" / "register_holdout_preflight_task.ps1"),
            str(live_repository),
            str(runtime),
            str(python),
            head,
            "08:40",
            mode,
            str(Path(GIT).resolve()),
        ],
    )


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_registration_rejects_unsafe_existing_task_before_force(
    tmp_path: Path,
) -> None:
    result = _run_mock_registration(tmp_path, "unsafe-existing")
    assert result.returncode != 0
    assert "not Disabled/Enabled=False" in result.stderr
    assert "DISABLE_CLEANUP_CALLED" not in result.stderr
    assert "STOP_CLEANUP_CALLED" not in result.stderr


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_registration_reads_back_exact_disabled_definition(
    tmp_path: Path,
) -> None:
    result = _run_mock_registration(tmp_path, "ok")
    assert result.returncode == 0, result.stderr
    actual = json.loads(result.stdout)
    assert actual["task_name"] == "guvolu-holdout-preflight"
    assert actual["task_path"] == "\\"
    assert actual["state"] == "Disabled"
    assert actual["enabled"] is False
    assert actual["working_directory"] == actual["definition"]["code_root"]
    assert actual["daily_at_local"] == "08:40"
    assert actual["trigger_enabled"] is True
    assert actual["trigger_days_interval"] == 1
    assert actual["principal_logon_type"] == "Interactive"
    assert actual["principal_run_level"] == "Limited"
    assert actual["settings"]["multiple_instances"] == "IgnoreNew"
    assert actual["settings"]["execution_time_limit"] == "PT30M"
    assert actual["definition"]["unattended_coverage_capable"] is False
    assert actual["exported_xml_sha256"]


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize(
    ("mode", "expected_error"),
    [
        ("action-drift", "action Arguments drifted"),
        ("trigger-drift", "daily trigger settings drifted"),
        ("post-enabled", "state is not Disabled"),
        ("xml-action-drift", "XML drifted"),
        ("xml-trigger-drift", "XML drifted"),
        ("cleanup-errors", "action Arguments drifted"),
    ],
)
def test_post_registration_drift_disables_and_stops_before_failing(
    tmp_path: Path,
    mode: str,
    expected_error: str,
) -> None:
    result = _run_mock_registration(tmp_path, mode)
    assert result.returncode != 0
    assert expected_error in result.stderr
    assert "DISABLE_CLEANUP_CALLED" in result.stderr
    assert "STOP_CLEANUP_CALLED" in result.stderr


def _wrapper_args(
    code_root: Path,
    python: Path,
    head: str,
    live_repository: Path,
    runtime: Path,
    *,
    timeout_seconds: int = 30,
) -> list[str]:
    assert POWERSHELL is not None
    assert GIT is not None
    wrapper = code_root / "scripts" / "run_holdout_preflight_task.ps1"
    runner = code_root / "scripts" / "preflight_holdout.py"
    pyvenv = code_root / ".venv" / "pyvenv.cfg"
    venv_manifest = _tree_identity(code_root / ".venv")
    code_manifest = _tree_identity(code_root / "src")
    runtime_manifest = _tree_identity(runtime / "src")
    base_manifest = _tree_identity(_pyvenv_home(pyvenv))
    return [
        POWERSHELL,
        "-NoProfile",
        "-File",
        str(wrapper),
        "-Repository",
        str(live_repository),
        "-RuntimeRoot",
        str(runtime),
        "-PythonExecutable",
        str(python),
        "-GitExecutable",
        str(Path(GIT).resolve()),
        "-ExpectedCodeHead",
        head,
        "-ExpectedWrapperSha256",
        _sha256(wrapper),
        "-ExpectedPythonSha256",
        _sha256(python),
        "-ExpectedPyVenvSha256",
        _sha256(pyvenv),
        "-ExpectedGovernanceRunnerSha256",
        _sha256(runner),
        "-ExpectedGitSha256",
        _sha256(Path(GIT)),
        "-ExpectedVenvTreeSha256",
        venv_manifest[0],
        "-ExpectedVenvFileCount",
        str(venv_manifest[1]),
        "-ExpectedVenvTotalBytes",
        str(venv_manifest[2]),
        "-ExpectedRuntimeSourceTreeSha256",
        runtime_manifest[0],
        "-ExpectedRuntimeSourceFileCount",
        str(runtime_manifest[1]),
        "-ExpectedRuntimeSourceTotalBytes",
        str(runtime_manifest[2]),
        "-ExpectedCodeSourceTreeSha256",
        code_manifest[0],
        "-ExpectedCodeSourceFileCount",
        str(code_manifest[1]),
        "-ExpectedCodeSourceTotalBytes",
        str(code_manifest[2]),
        "-ExpectedBaseRuntimeTreeSha256",
        base_manifest[0],
        "-ExpectedBaseRuntimeFileCount",
        str(base_manifest[1]),
        "-ExpectedBaseRuntimeTotalBytes",
        str(base_manifest[2]),
        "-ExecutionTimeoutSeconds",
        str(timeout_seconds),
        "-VintageId",
        VINTAGE_ID,
    ]


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize("condition", ["branch", "dirty", "head"])
def test_wrapper_rejects_unsealed_code_before_business_execution(
    tmp_path: Path,
    condition: str,
) -> None:
    code_root, python, head = _code_repository(
        tmp_path,
        detached=condition != "branch",
    )
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "must-not-exist.txt"
    if condition == "dirty":
        with (code_root / "scripts" / "preflight_holdout.py").open(
            "a", encoding="utf-8",
        ) as handle:
            handle.write("# tracked dirt\n")
    if condition == "head":
        head = "0" * 40
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    result = _run(
        _wrapper_args(
            code_root, python, head, live_repository, runtime,
        ),
        env=environment,
    )
    assert result.returncode == 3
    assert not capture.exists()
    expected = {
        "branch": "must be detached",
        "dirty": "tracked changes or non-ignored untracked files",
        "head": "HEAD mismatch",
    }[condition]
    assert expected in result.stderr


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_uses_live_data_and_runtime_pythonpath_and_restores_environment(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "fake-python-capture.txt"
    fake_main_registry = (
        live_repository / "data" / "research" / "governance.sqlite3"
    )
    fake_main_before = fake_main_registry.read_bytes()
    authority_registry = (
        runtime / "data" / "research" / "governance.sqlite3"
    )
    authority_before = _governance_store_bytes(authority_registry)
    assert authority_before["-wal"] == b""
    assert authority_before["-shm"] is not None
    environment = os.environ.copy()
    environment["PYTHONPATH"] = "ORIGINAL_SENTINEL"
    _write_test_controls(
        runtime,
        capture=str(capture),
        chinese_stderr=True,
        snapshot_query="SELECT value FROM authority_marker",
    )
    result = _run(
        _wrapper_args(
            code_root, python, head, live_repository, runtime,
        ),
        env=environment,
    )
    assert result.returncode == 0, result.stderr
    captured = capture.read_text(encoding="utf-8")
    assert f"ROOT={runtime.resolve()}" in captured
    captured_values = dict(
        line.split("=", maxsplit=1) for line in captured.splitlines()
    )
    snapshot = Path(captured_values["REGISTRY"])
    assert snapshot.parent == (
        live_repository.resolve()
        / "logs" / "research" / "frozen-forward" / "preflight"
    )
    assert snapshot.name.startswith("governance-snapshot-")
    assert not snapshot.exists()
    assert "PYTHONPATH=" in captured
    assert "FLAGS=(1, 1, 1, 1)" in captured
    assert "GOVERNANCE_ORIGIN=clean-code-root" in captured
    assert "SNAPSHOT_VALUE='D-authority'" in captured
    assert "SNAPSHOT_AUX_PRESENT=False" in captured
    assert fake_main_registry.read_bytes() == fake_main_before
    assert _governance_store_bytes(authority_registry) == authority_before
    assert str(fake_main_registry.resolve()) not in captured

    log_root = live_repository / "logs" / "research" / "frozen-forward"
    record_paths = list((log_root / "preflight").glob("scheduler-*.json"))
    assert len(record_paths) == 1
    record = json.loads(record_paths[0].read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    assert record["business_exit_code"] == 0
    assert record["exit_code"] == 0
    assert record["failure"] is None
    assert record["integrity_failure"] is None
    assert record["code_root"] == str(code_root.resolve())
    assert record["expected_code_head"] == head
    assert record["live_repository"] == str(live_repository.resolve())
    assert record["runtime_root"] == str(runtime.resolve())
    assert record["authoritative_data_root"] == str(runtime.resolve())
    assert record["authoritative_governance_registry"] == str(
        runtime.resolve() / "data" / "research" / "governance.sqlite3",
    )
    assert record["governance_store_before"] == record["governance_store_after"]
    assert os.path.normcase(record["registry_snapshot"]) == os.path.normcase(
        str(snapshot),
    )
    assert record["registry_snapshot_sha256"]
    assert (
        record["launcher_snapshot_sha256"]
        == record["registry_snapshot_sha256"]
    )
    assert record["registry_snapshot_disposable"] is True
    assert "隔离预检成功" in record["output"]
    assert "中文标准错误" in record["output"]
    assert record["expected_pythonpath"] is None
    assert record["child_pythonpath_present"] is False
    assert record["import_closure_tree_sha256"]
    assert record["import_closure_manifest_sha256"]
    assert record["import_closure_source_tree_sha256"] == (
        record["expected_code_source_tree_sha256"]
    )
    assert record["import_closure_entry_sha256"] == (
        record["expected_governance_runner_sha256"]
    )
    assert not Path(record["import_closure"]).exists()
    assert record["pythonpath_restored"] is True
    assert record["expected_python_sha256"] == _sha256(python)
    assert record["identity_before"] == record["identity_after"]
    assert record["environment_attestation"] == "partial"
    assert "create-suspended" in record["job_guard_strength"]
    assert "assign-kill-on-close-job" in record["job_guard_strength"]
    assert record["json_output_sha256"]
    assert record["json_output_published"] is True
    assert not Path(record["json_output_staging"]).exists()
    lines = (log_root / "preflight-scheduler.jsonl").read_text(
        encoding="utf-8",
    ).splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["scheduler_record"] == str(record_paths[0])


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_isolated_launcher_executes_only_bound_code_source_and_ignores_startup_attacks(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    authority_before = _governance_store_bytes(registry)
    runtime_marker = tmp_path / "runtime-bait-executed.txt"
    pth_marker = tmp_path / "pth-executed.txt"
    inherited_marker = tmp_path / "inherited-sitecustomize-executed.txt"

    runtime_package = runtime / "src" / "guvolu" / "research"
    runtime_package.mkdir(parents=True)
    (runtime_package.parent / "__init__.py").write_text("", encoding="utf-8")
    (runtime_package / "__init__.py").write_text("", encoding="utf-8")
    (runtime_package / "governance.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(runtime_marker)!r}).write_text('runtime bait')\n"
        "ORIGIN = 'unsafe-runtime-source'\n",
        encoding="utf-8",
    )
    site_packages = code_root / ".venv" / "Lib" / "site-packages"
    (site_packages / "malicious-startup.pth").write_text(
        f"import pathlib; pathlib.Path({str(pth_marker)!r}).write_text('pth')\n",
        encoding="utf-8",
    )
    inherited = tmp_path / "inherited malicious PYTHONPATH"
    inherited.mkdir()
    (inherited / "sitecustomize.py").write_text(
        f"from pathlib import Path; Path({str(inherited_marker)!r}).write_text('site')\n",
        encoding="utf-8",
    )
    capture = tmp_path / "isolated-capture.txt"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(inherited)
    _write_test_controls(runtime, capture=str(capture))

    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 0, result.stderr
    assert "GOVERNANCE_ORIGIN=clean-code-root" in capture.read_text(
        encoding="utf-8",
    )
    assert not runtime_marker.exists()
    assert not pth_marker.exists()
    assert not inherited_marker.exists()
    assert not list((code_root / "src").rglob("*.pyc"))
    assert not list((runtime / "src").rglob("*.pyc"))
    assert _governance_store_bytes(registry) == authority_before
    assert authority_before["-wal"] == b""
    assert authority_before["-shm"] is not None


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize(
    "attack",
    [
        "transient-code",
        "direct-pathfinder",
        "namespace-site",
        "zip-pathfinder",
        "native-pyd",
    ],
)
def test_guarded_import_closure_rejects_window_and_loader_bypasses(
    tmp_path: Path,
    attack: str,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    ready = tmp_path / f"{attack}-ready.txt"
    executed = tmp_path / f"{attack}-executed.txt"
    site_packages = code_root / ".venv" / "Lib" / "site-packages"
    code_source = code_root / "src"
    module_name = "late_attack"
    search_path = ""
    mode = "normal"
    target: Path
    if attack == "transient-code":
        target = code_source / "late_attack.py"
    elif attack == "direct-pathfinder":
        target = code_source / "late_attack.py"
        mode = "direct-pathfinder"
        search_path = str(code_source)
    elif attack == "namespace-site":
        target = site_packages / "late_namespace" / "payload.py"
        module_name = "late_namespace.payload"
    elif attack == "zip-pathfinder":
        target = code_source / "late-import.zip"
        mode = "direct-pathfinder"
        search_path = str(target)
    else:
        target = site_packages / "late_native.pyd"
        module_name = "late_native"
        mode = "direct-pathfinder"
        search_path = str(site_packages)
    _write_test_controls(
        runtime,
        import_attack_mode=mode,
        import_attack_ready=str(ready),
        import_attack_delay=1.2,
        import_attack_module=module_name,
        import_attack_search_path=search_path,
    )
    wrapper = subprocess.Popen(
        _wrapper_args(
            code_root,
            python,
            head,
            live_repository,
            runtime,
            timeout_seconds=20,
        ),
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    deadline = time.monotonic() + 20
    while not ready.exists() and time.monotonic() < deadline:
        if wrapper.poll() is not None:
            break
        time.sleep(0.02)
    assert ready.exists(), wrapper.communicate(timeout=5)
    payload = (
        "from pathlib import Path\n"
        f"Path({str(executed)!r}).write_text('bypass')\n"
    )
    if attack == "zip-pathfinder":
        with zipfile.ZipFile(target, "w") as archive:
            archive.writestr("late_attack.py", payload)
    elif attack == "native-pyd":
        target.write_bytes(b"not-a-native-extension")
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
    if attack == "transient-code":
        target.unlink()
    stdout, stderr = wrapper.communicate(timeout=40)
    assert wrapper.returncode == 3, (stdout, stderr)
    assert not executed.exists()
    records = list(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json")
    )
    assert len(records) == 1
    record = json.loads(records[0].read_text(encoding="utf-8"))
    assert record["business_invocation_attempted"] is True
    assert record["business_executed"] is True
    assert record["json_output_published"] is False
    assert not Path(record["json_output"]).exists()
    assert not Path(record["json_output_staging"]).exists()
    assert not Path(record["import_closure"]).exists()


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_business_child_receives_only_minimal_nonsecret_environment(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "minimal-environment-capture.txt"
    hostile_names = [
        "AWS_SECRET_ACCESS_KEY",
        "OPENAI_API_KEY",
        "IBKR_TOKEN",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "GIT_CONFIG_GLOBAL",
        "GIT_SSH_COMMAND",
        "PYTHONHOME",
        "PYTHONSTARTUP",
    ]
    _write_test_controls(
        runtime,
        capture=str(capture),
        probe_environment_names=hostile_names,
    )
    environment = os.environ.copy()
    for name in hostile_names:
        environment[name] = "HOSTILE_SECRET_OR_INJECTION"
    environment["PYTHONPATH"] = str(tmp_path / "hostile-pythonpath")
    (live_repository / ".env").write_text(
        "OPENAI_API_KEY=dotenv-secret\n",
        encoding="utf-8",
    )
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 0, result.stderr
    captured = capture.read_text(encoding="utf-8")
    assert "PROBED_ENVIRONMENT=[]" in captured
    assert f"PYTHONPATH={code_root.resolve() / 'src'}" in captured
    assert "HOSTILE_SECRET_OR_INJECTION" not in captured


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_snapshot_no_share_guard_blocks_write_delete_and_replace(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    attack_result = tmp_path / "snapshot-attack-result.json"
    capture = tmp_path / "snapshot-capture.txt"
    environment = os.environ.copy()
    _write_test_controls(
        runtime,
        capture=str(capture),
        snapshot_query="SELECT value FROM authority_marker",
        snapshot_attack_result=str(attack_result),
    )
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(attack_result.read_text(encoding="utf-8")) == {
        "write": False,
        "delete": False,
        "replace": False,
    }
    assert "SNAPSHOT_VALUE='D-authority'" in capture.read_text(
        encoding="utf-8",
    )
    assert "SNAPSHOT_AUX_PRESENT=False" in capture.read_text(encoding="utf-8")
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    assert record["registry_snapshot_sha256"]
    assert (
        record["registry_snapshot_sha256"]
        == record["launcher_snapshot_sha256"]
    )
    assert not Path(record["registry_snapshot"]).exists()


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize("precreate_kind", ["hardlink", "symlink"])
def test_snapshot_create_new_rejects_racing_link_without_overwrite(
    tmp_path: Path,
    precreate_kind: str,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "must-not-enter-business.txt"
    _write_test_controls(runtime, capture=str(capture))
    bait = tmp_path / "snapshot-link-bait.sqlite3"
    bait.write_bytes(b"DO_NOT_OVERWRITE_LINK_TARGET")
    wrapper = subprocess.Popen(
        _wrapper_args(
            code_root,
            python,
            head,
            live_repository,
            runtime,
            timeout_seconds=20,
        ),
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    run_directory = (
        live_repository
        / "logs" / "research" / "frozen-forward" / "preflight"
    )
    snapshot: Path | None = None
    deadline = time.monotonic() + 20
    while snapshot is None and time.monotonic() < deadline:
        if run_directory.exists():
            for pycache in run_directory.glob("pycache-*"):
                run_id = pycache.name.removeprefix("pycache-")
                candidate = run_directory / f"governance-snapshot-{run_id}.sqlite3"
                try:
                    if precreate_kind == "hardlink":
                        os.link(bait, candidate)
                    else:
                        candidate.symlink_to(bait)
                except FileExistsError:
                    continue
                except OSError as error:
                    wrapper.kill()
                    wrapper.communicate(timeout=10)
                    if precreate_kind == "symlink":
                        pytest.skip(f"Windows file symlink unavailable: {error}")
                    raise
                snapshot = candidate
                break
        if wrapper.poll() is not None:
            break
        time.sleep(0.005)
    assert snapshot is not None
    wrapper_stdout, wrapper_stderr = wrapper.communicate(timeout=30)
    assert wrapper.returncode == 3, (wrapper_stdout, wrapper_stderr)
    assert not capture.exists()
    assert bait.read_bytes() == b"DO_NOT_OVERWRITE_LINK_TARGET"
    record_path = next(run_directory.glob("scheduler-*.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_invocation_attempted"] is True
    assert record["business_executed"] is False
    assert "CREATE_NEW governance snapshot" in record["output"]


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_launcher_guard_cleanup_failure_does_not_hide_business_primary_error(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "primary-error-capture.txt"
    environment = os.environ.copy()
    _write_test_controls(
        runtime,
        capture=str(capture),
        force_guard_close_failure=True,
    )
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 1
    assert capture.exists()
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    assert record["business_exit_code"] == 1
    assert "PRIMARY_BUSINESS_FAILURE_MUST_WIN" in record["output"]
    assert "isolated launcher guard cleanup failed" not in record["output"]
    assert not Path(record["registry_snapshot"]).exists()


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_existing_authority_rollback_journal_fails_before_business(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    journal = Path(str(registry) + "-journal")
    journal.write_bytes(b"test-only-possible-hot-journal")
    before = {path: path.read_bytes() for path in (registry, journal)}
    capture = tmp_path / "must-not-run.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 3
    assert "rollback journal is present; fail closed" in result.stderr
    assert not capture.exists()
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_snapshot_foreign_key_violation_fails_closed_before_business(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    _install_fk_violation_store(tmp_path, registry)
    authority_before = _governance_store_bytes(registry)
    capture = tmp_path / "must-not-run.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 3
    assert not capture.exists()
    assert _governance_store_bytes(registry) == authority_before
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_executed"] is False
    assert "foreign_key_check failed" in record["output"]
    assert not Path(record["registry_snapshot"]).exists()


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize("zero_wal", [False, True], ids=["committed-wal", "zero-wal"])
def test_windows_sqlite_wal_snapshot_reads_committed_view_without_mutation(
    tmp_path: Path,
    zero_wal: bool,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    _install_copied_wal_store(tmp_path, registry, zero_wal=zero_wal)
    before = _governance_store_bytes(registry)
    assert before["-wal"] is not None
    assert before["-shm"] is not None
    if zero_wal:
        assert before["-wal"] == b""
    capture = tmp_path / "wal-snapshot-capture.txt"
    environment = os.environ.copy()
    _write_test_controls(
        runtime,
        capture=str(capture),
        snapshot_query="SELECT COUNT(*) FROM authority_marker",
    )
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 0, result.stderr
    assert "SNAPSHOT_VALUE=2" in capture.read_text(encoding="utf-8")
    assert _governance_store_bytes(registry) == before
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["governance_store_before"] == record["governance_store_after"]
    assert record["business_exit_code"] == 0
    assert "query_only" in record["registry_snapshot_validation"]
    assert "quick_check" in record["registry_snapshot_validation"]
    assert "foreign_key_check" in record["registry_snapshot_validation"]


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize(
    ("page_size", "initial_rows", "committed_wal_row"),
    [(4096, 0, False), (8192, 512, True)],
    ids=["empty-v9-zero-wal", "large-v9-committed-wal-nondefault-pages"],
)
def test_actual_v9_governance_reader_uses_delete_mode_snapshot_without_aux(
    tmp_path: Path,
    page_size: int,
    initial_rows: int,
    committed_wal_row: bool,
) -> None:
    code_root, python, head = _code_repository(
        tmp_path,
        runnable=True,
        real_governance=True,
    )
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    expected_rows = _install_v9_wal_store(
        tmp_path,
        registry,
        page_size=page_size,
        initial_rows=initial_rows,
        committed_wal_row=committed_wal_row,
    )
    before = _governance_store_bytes(registry)
    if committed_wal_row:
        assert before["-wal"] not in (None, b"")
    else:
        assert before["-wal"] == b""
    capture = tmp_path / "actual-v9-reader-capture.txt"
    _write_test_controls(
        runtime,
        capture=str(capture),
        snapshot_query="SELECT COUNT(*) FROM research_exposure",
        real_governance_reader=True,
    )
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=os.environ.copy(),
    )
    assert result.returncode == 0, result.stderr
    captured = capture.read_text(encoding="utf-8")
    assert f"SNAPSHOT_VALUE={expected_rows}" in captured
    assert f"REAL_GOVERNANCE_ROW_COUNT={expected_rows}" in captured
    assert "REAL_GOVERNANCE_JOURNAL_MODE=delete" in captured
    assert "SNAPSHOT_AUX_PRESENT=False" in captured
    assert "GOVERNANCE_ORIGIN=clean-code-root-real-governance" in captured
    assert _governance_store_bytes(registry) == before
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["authority_sidecar_precondition"] == (
        "db+wal+shm-preexisting;rollback-journal-absent"
    )
    assert record["registry_snapshot_journal_mode"] == "delete"
    assert record["registry_snapshot_auxiliaries"] == (
        "absent-before-and-after-business"
    )


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize("present_sidecar", ["wal", "shm"])
def test_incomplete_authority_sidecars_fail_closed_without_mutation(
    tmp_path: Path,
    present_sidecar: str,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    for suffix in ("-wal", "-shm"):
        Path(str(registry) + suffix).unlink()
    Path(str(registry) + f"-{present_sidecar}").write_bytes(b"")
    before = _governance_store_bytes(registry)
    capture = tmp_path / "must-not-run.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 3
    assert not capture.exists()
    assert _governance_store_bytes(registry) == before
    assert "business_invocation_attempted=False" in result.stderr
    assert "WAL/SHM sidecars are incomplete" in result.stderr


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_existing_db_wal_shm_cannot_be_written_or_deleted_during_business(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    _install_copied_wal_store(tmp_path, registry, zero_wal=False)
    before = _governance_store_bytes(registry)
    capture = tmp_path / "authority-attack-business-ready.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture), sleep_seconds=4)
    wrapper = subprocess.Popen(
        _wrapper_args(
            code_root,
            python,
            head,
            live_repository,
            runtime,
            timeout_seconds=20,
        ),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    deadline = time.monotonic() + 20
    while not capture.exists() and time.monotonic() < deadline:
        if wrapper.poll() is not None:
            break
        time.sleep(0.02)
    assert capture.exists(), wrapper.communicate(timeout=5)
    attacks: list[tuple[subprocess.Popen[str], Path]] = []
    for suffix in ("", "-wal", "-shm"):
        for operation in ("write", "delete"):
            result_path = tmp_path / (
                f"authority-{suffix or 'db'}-{operation}.txt"
            )
            attacks.append(
                (
                    _start_authority_file_attack(
                        Path(str(registry) + suffix), operation, result_path
                    ),
                    result_path,
                )
            )
    time.sleep(0.4)
    assert _governance_store_bytes(registry) == before
    for process, result_path in attacks:
        if process.poll() is not None:
            assert result_path.read_text(encoding="utf-8").startswith("denied:")
    wrapper_stdout, wrapper_stderr = wrapper.communicate(timeout=30)
    assert wrapper.returncode in (0, 3), (wrapper_stdout, wrapper_stderr)
    for process, result_path in attacks:
        attack_stdout, attack_stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (attack_stdout, attack_stderr)
        outcome = result_path.read_text(encoding="utf-8")
        assert outcome == "succeeded-after-release" or outcome.startswith("denied:")
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    if wrapper.returncode == 0:
        assert record["governance_store_before"] == record["governance_store_after"]
    else:
        assert "oplock was broken" in record["integrity_failure"]


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_sidecarless_store_fails_before_invocation_and_has_no_attack_window(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    for suffix in ("-wal", "-shm"):
        Path(str(registry) + suffix).unlink()
    before = _governance_store_bytes(registry)
    assert before["-wal"] is None and before["-shm"] is None
    capture = tmp_path / "must-not-run.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 3
    assert "business_invocation_attempted=False" in result.stderr
    assert "sidecarless authoritative governance store" in result.stderr
    assert not capture.exists()
    assert _governance_store_bytes(registry) == before

    # An attacker may create the absent names after this fail-closed return,
    # but there was no child/business window in which SQLite could observe it.
    attacks: list[tuple[subprocess.Popen[str], Path, Path]] = []
    for suffix in ("-wal", "-shm"):
        target = Path(str(registry) + suffix)
        result_path = tmp_path / f"create{suffix}.txt"
        attacks.append(
            (
                _start_authority_file_attack(target, "create", result_path),
                target,
                result_path,
            )
        )
    for process, target, result_path in attacks:
        attack_stdout, attack_stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (attack_stdout, attack_stderr)
        assert result_path.read_text(encoding="utf-8") == "succeeded-after-release"
        assert target.read_bytes() == b"TRANSIENT"


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize(
    "injection",
    [
        "nonignored-code",
        "nonignored-dotenv",
        "ignored-pyc",
        "ignored-src-pyc",
        "runtime-pyc",
    ],
)
def test_wrapper_rejects_code_injection_before_python(
    tmp_path: Path,
    injection: str,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    if injection == "nonignored-code":
        (code_root / "scripts" / "untracked_attack.py").write_text(
            "raise RuntimeError('attack')\n", encoding="utf-8",
        )
    elif injection == "nonignored-dotenv":
        (code_root / ".env").write_text(
            "OPENAI_API_KEY=must-not-load\n", encoding="utf-8"
        )
    elif injection == "ignored-pyc":
        cache = code_root / "scripts" / "__pycache__"
        cache.mkdir()
        (cache / "attack.pyc").write_bytes(b"malicious-pyc")
    elif injection == "ignored-src-pyc":
        cache = code_root / "src" / "guvolu" / "research" / "__pycache__"
        cache.mkdir()
        (cache / "legacy_attack.pyc").write_bytes(b"malicious-legacy-pyc")
    else:
        cache = runtime / "src" / "__pycache__"
        cache.mkdir()
        (cache / "attack.pyc").write_bytes(b"malicious-pyc")
    capture = tmp_path / "must-not-run.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    result = _run(
        _wrapper_args(code_root, python, head, live_repository, runtime),
        env=environment,
    )
    assert result.returncode == 3
    assert not capture.exists()
    assert "business_invocation_attempted=False" in result.stderr
    assert (
        "injection" in result.stderr.lower()
        or "untracked" in result.stderr.lower()
    )


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
@pytest.mark.parametrize("manifest", ["venv", "base"])
def test_wrapper_rejects_manifest_drift_before_python(
    tmp_path: Path,
    manifest: str,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    args = _wrapper_args(code_root, python, head, live_repository, runtime)
    if manifest == "venv":
        (code_root / ".venv" / "Lib" / "site-packages" / "late.py").write_text(
            "ATTACK = True\n", encoding="utf-8",
        )
    else:
        index = args.index("-ExpectedBaseRuntimeTreeSha256") + 1
        args[index] = "0" * 64
    capture = tmp_path / "must-not-run.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    result = _run(args, env=environment)
    assert result.returncode == 3
    assert not capture.exists()


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_timeout_kills_descendants_and_records_business_entry(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "timeout-capture.txt"
    orphan = tmp_path / "orphan-marker.txt"
    environment = os.environ.copy()
    _write_test_controls(
        runtime,
        capture=str(capture),
        child_marker=str(orphan),
        sleep_seconds=30,
    )
    started = time.monotonic()
    result = _run(
        _wrapper_args(
            code_root, python, head, live_repository, runtime,
            timeout_seconds=8,
        ),
        env=environment,
    )
    elapsed = time.monotonic() - started
    assert result.returncode == 3
    assert elapsed < 15
    time.sleep(3)
    assert not orphan.exists()
    record_path = next(
        (live_repository / "logs" / "research" / "frozen-forward" / "preflight").glob(
            "scheduler-*.json",
        ),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    assert record["business_exit_code"] == 124
    assert "exceeded 8 seconds" in record["failure"]
    assert record["registry_snapshot_sha256"]
    assert (
        record["launcher_snapshot_sha256"]
        == record["registry_snapshot_sha256"]
    )


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_terminates_job_when_output_exceeds_bounded_capture(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "output-limit-capture.txt"
    _write_test_controls(
        runtime,
        capture=str(capture),
        output_bytes=2 * 1024 * 1024,
        output_stream="stderr",
    )
    started = time.monotonic()
    result = _run(
        _wrapper_args(
            code_root,
            python,
            head,
            live_repository,
            runtime,
            timeout_seconds=20,
        ),
        env=os.environ.copy(),
    )
    elapsed = time.monotonic() - started
    assert result.returncode == 3
    assert elapsed < 15
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    assert record["business_exit_code"] == 126
    assert record["output_limit_exceeded"] is True
    assert record["output_limit_bytes_per_stream"] == 1048576
    assert "exceeded the 1048576-byte per-stream limit" in record["failure"]
    assert len(record["output"].encode("utf-8")) <= 1048576 + 1024


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_existing_sqlite_writer_prevents_business_start(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path)
    live_repository, runtime = _data_roots(tmp_path)
    registry = runtime / "data" / "research" / "governance.sqlite3"
    authority_before = registry.read_bytes()
    connection = sqlite3.connect(registry)
    connection.execute("BEGIN IMMEDIATE")
    capture = tmp_path / "must-not-run.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    try:
        result = _run(
            _wrapper_args(code_root, python, head, live_repository, runtime),
            env=environment,
        )
    finally:
        connection.rollback()
        connection.close()
    assert result.returncode == 3
    assert not capture.exists()
    assert "used by another process" in result.stderr.lower()
    assert registry.read_bytes() == authority_before


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_logging_failure_does_not_misreport_success_as_unexecuted(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "fake-python-capture.txt"
    log_root = live_repository / "logs" / "research" / "frozen-forward"
    log_root.mkdir(parents=True)
    scheduler_index = log_root / "preflight-scheduler.jsonl"
    scheduler_index.write_text("", encoding="utf-8")
    os.chmod(scheduler_index, stat.S_IREAD)
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture))
    try:
        result = _run(
            _wrapper_args(
                code_root, python, head, live_repository, runtime,
            ),
            env=environment,
        )
    finally:
        os.chmod(scheduler_index, stat.S_IWRITE | stat.S_IREAD)
    assert result.returncode == 0
    assert "scheduler evidence write failed after business_executed=True" in (
        result.stderr
    )
    records = list((log_root / "preflight").glob("scheduler-*.json"))
    assert len(records) == 1
    record = json.loads(records[0].read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    assert record["business_exit_code"] == 0
    assert record["exit_code"] == 0


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_directory_rh_oplock_repeated_cleanup_and_rename_break(
    tmp_path: Path,
) -> None:
    """真实 RH break 必须先挡住目录替换，关闭后再释放请求。"""
    assert POWERSHELL is not None
    guarded = tmp_path / "guarded tree"
    moved = tmp_path / "moved guarded tree"
    ready = tmp_path / "guard-ready.txt"
    broken = tmp_path / "guard-broken.txt"
    guarded.mkdir()
    (guarded / "trusted.txt").write_text("trusted", encoding="utf-8")
    environment = os.environ.copy()
    environment.update(
        {
            "GUVALU_TEST_WRAPPER": str(
                SOURCE_ROOT / "scripts" / "run_holdout_preflight_task.ps1"
            ),
            "GUVALU_TEST_GUARDED": str(guarded),
            "GUVALU_TEST_READY": str(ready),
            "GUVALU_TEST_BROKEN": str(broken),
        }
    )
    guard_script = r"""
$Source = Get-Content -LiteralPath $env:GUVALU_TEST_WRAPPER -Raw
$Match = [regex]::Match(
    $Source,
    "Add-Type -TypeDefinition @'\r?\n(?<body>.*?)\r?\n'@",
    [System.Text.RegularExpressions.RegexOptions]::Singleline
)
if (-not $Match.Success) { throw "native guard source not found" }
Add-Type -TypeDefinition $Match.Groups['body'].Value
for ($Index = 0; $Index -lt 64; $Index += 1) {
    $Handle = [Guvolu.PreflightNative]::OpenDirectoryGuard(
        $env:GUVALU_TEST_GUARDED
    )
    [Guvolu.PreflightNative]::CloseChecked($Handle, "stress RH oplock")
}
$Handle = [Guvolu.PreflightNative]::OpenDirectoryGuard(
    $env:GUVALU_TEST_GUARDED
)
[System.IO.File]::WriteAllText($env:GUVALU_TEST_READY, "ready")
$Deadline = [datetime]::UtcNow.AddSeconds(15)
while ([datetime]::UtcNow -lt $Deadline) {
    if ([Guvolu.PreflightNative]::DirectoryGuardBreakPending($Handle)) {
        [System.IO.File]::WriteAllText($env:GUVALU_TEST_BROKEN, "broken")
        Start-Sleep -Milliseconds 1000
        [Guvolu.PreflightNative]::CloseChecked($Handle, "broken RH oplock")
        exit 0
    }
    Start-Sleep -Milliseconds 10
}
[Guvolu.PreflightNative]::CloseChecked($Handle, "timed-out RH oplock")
exit 2
"""
    guard = subprocess.Popen(
        [POWERSHELL, "-NoProfile", "-Command", guard_script],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    deadline = time.monotonic() + 20
    while not ready.exists() and time.monotonic() < deadline:
        if guard.poll() is not None:
            break
        time.sleep(0.02)
    assert ready.exists(), guard.communicate(timeout=5)

    attack_script = (
        "import os,sys; from pathlib import Path; "
        "os.replace(sys.argv[1], sys.argv[2]); "
        "Path(sys.argv[1]).mkdir(); "
        "(Path(sys.argv[1])/'fake.txt').write_text('fake', encoding='utf-8')"
    )
    attacker = subprocess.Popen(
        [sys.executable, "-I", "-S", "-B", "-c", attack_script,
         str(guarded), str(moved)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    deadline = time.monotonic() + 10
    while not broken.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert broken.exists()
    assert attacker.poll() is None
    stdout, stderr = guard.communicate(timeout=20)
    assert guard.returncode == 0, (stdout, stderr)
    attack_stdout, attack_stderr = attacker.communicate(timeout=10)
    assert attacker.returncode == 0, (attack_stdout, attack_stderr)
    assert broken.read_text(encoding="utf-8") == "broken"
    assert (moved / "trusted.txt").read_text(encoding="utf-8") == "trusted"
    assert (guarded / "fake.txt").read_text(encoding="utf-8") == "fake"


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_forced_job_assignment_failure_never_resumes_child_process(
    tmp_path: Path,
) -> None:
    """真实 suspended process 在注入的 Assign 失败分支必须直接终止。"""
    assert POWERSHELL is not None
    marker = tmp_path / "must-not-be-created-by-unassigned-child.txt"
    error_record = tmp_path / "assign-failure.txt"
    child_code = (
        "from pathlib import Path; import sys,time; "
        "Path(sys.argv[1]).write_text('ran', encoding='utf-8'); time.sleep(30)"
    )
    command_line = subprocess.list2cmdline(
        [
            sys.executable,
            "-I",
            "-S",
            "-B",
            "-c",
            child_code,
            str(marker),
        ]
    )
    environment = os.environ.copy()
    environment.update(
        {
            "GUVALU_TEST_WRAPPER": str(
                SOURCE_ROOT / "scripts" / "run_holdout_preflight_task.ps1"
            ),
            "GUVALU_TEST_APPLICATION": sys.executable,
            "GUVALU_TEST_COMMAND_LINE": command_line,
            "GUVALU_TEST_WORKING_DIRECTORY": str(tmp_path),
            "GUVALU_TEST_ERROR_RECORD": str(error_record),
        }
    )
    script = r"""
$Source = Get-Content -LiteralPath $env:GUVALU_TEST_WRAPPER -Raw
$Match = [regex]::Match(
    $Source,
    "Add-Type -TypeDefinition @'\r?\n(?<body>.*?)\r?\n'@",
    [System.Text.RegularExpressions.RegexOptions]::Singleline
)
if (-not $Match.Success) { throw "native process source not found" }
Add-Type -TypeDefinition $Match.Groups['body'].Value
$SystemRootValue = Split-Path -Parent ([Environment]::SystemDirectory)
[string[]]$ChildEnvironment = @(
    "SYSTEMROOT=$SystemRootValue",
    "WINDIR=$SystemRootValue",
    "TEMP=$([IO.Path]::GetTempPath())",
    "TMP=$([IO.Path]::GetTempPath())",
    "PYTHONPATH="
)
try {
    [Guvolu.PreflightNative]::RunSuspendedCapped(
        $env:GUVALU_TEST_APPLICATION,
        $env:GUVALU_TEST_COMMAND_LINE,
        $env:GUVALU_TEST_WORKING_DIRECTORY,
        $ChildEnvironment,
        10,
        1048576,
        $true
    ) | Out-Null
    throw "forced assignment failure unexpectedly returned"
} catch {
    [IO.File]::WriteAllText(
        $env:GUVALU_TEST_ERROR_RECORD,
        $_.Exception.ToString()
    )
}
"""
    result = _run(
        [POWERSHELL, "-NoProfile", "-Command", script],
        env=environment,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert "cannot assign suspended isolated Python" in error_record.read_text(
        encoding="utf-8"
    )
    time.sleep(1)
    assert not marker.exists()


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_guards_all_bound_roots_against_rename_and_fake_tree_swap(
    tmp_path: Path,
) -> None:
    code_root, python, head = _code_repository(tmp_path, runnable=True)
    live_repository, runtime = _data_roots(tmp_path)
    capture = tmp_path / "business-ready.txt"
    environment = os.environ.copy()
    _write_test_controls(runtime, capture=str(capture), sleep_seconds=4)
    wrapper = subprocess.Popen(
        _wrapper_args(
            code_root,
            python,
            head,
            live_repository,
            runtime,
            timeout_seconds=20,
        ),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    deadline = time.monotonic() + 20
    while not capture.exists() and time.monotonic() < deadline:
        if wrapper.poll() is not None:
            break
        time.sleep(0.02)
    assert capture.exists(), wrapper.communicate(timeout=5)

    targets = [
        code_root / "src",
        runtime / "src",
        code_root / ".venv",
        runtime / "data" / "research",
    ]
    attack_script = r"""import os
import sys
from pathlib import Path

target = Path(sys.argv[1])
moved = Path(sys.argv[2])
result = Path(sys.argv[3])
try:
    os.replace(target, moved)
    target.mkdir()
    (target / "fake.txt").write_text("fake", encoding="utf-8")
    result.write_text("swapped-after-release", encoding="utf-8")
except OSError as error:
    result.write_text("denied:" + str(error.winerror), encoding="utf-8")
"""
    attacks: list[tuple[subprocess.Popen[str], Path, Path, Path]] = []
    for index, target in enumerate(targets):
        moved = target.parent / f"{target.name}.trusted-moved-{index}"
        attack_result = tmp_path / f"root-attack-{index}.txt"
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-S",
                "-B",
                "-c",
                attack_script,
                str(target),
                str(moved),
                str(attack_result),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        attacks.append((process, target, moved, attack_result))
    time.sleep(0.3)
    for process, _target, _moved, result_path in attacks:
        if process.poll() is not None and result_path.exists():
            assert result_path.read_text(encoding="utf-8").startswith("denied:")

    wrapper_stdout, wrapper_stderr = wrapper.communicate(timeout=30)
    assert wrapper.returncode == 3, (wrapper_stdout, wrapper_stderr)
    for process, target, moved, result_path in attacks:
        attack_stdout, attack_stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (attack_stdout, attack_stderr)
        outcome = result_path.read_text(encoding="utf-8")
        assert outcome == "swapped-after-release" or outcome.startswith("denied:")
        if outcome == "swapped-after-release":
            assert moved.is_dir()
            assert (target / "fake.txt").read_text(encoding="utf-8") == "fake"
    record_path = next(
        (
            live_repository
            / "logs" / "research" / "frozen-forward" / "preflight"
        ).glob("scheduler-*.json"),
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["business_executed"] is True
    assert "oplock was broken" in record["integrity_failure"]


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_powershell_sources_parse_without_ast_errors() -> None:
    assert POWERSHELL is not None
    for script in (
        SOURCE_ROOT / "scripts" / "run_holdout_preflight_task.ps1",
        SOURCE_ROOT / "scripts" / "register_holdout_preflight_task.ps1",
    ):
        escaped = str(script).replace("'", "''")
        command = (
            "$t=$null;$e=$null;"
            "[void][System.Management.Automation.Language.Parser]::ParseFile("
            f"'{escaped}',[ref]$t,[ref]$e);"
            "if($e.Count){$e|% Message;exit 1}"
        )
        result = _run(
            [POWERSHELL, "-NoProfile", "-Command", command],
        )
        assert result.returncode == 0, result.stdout + result.stderr


def test_registration_source_keeps_describe_before_task_cmdlets() -> None:
    source = (
        SOURCE_ROOT / "scripts" / "register_holdout_preflight_task.ps1"
    ).read_text(encoding="utf-8")
    describe = source.index("if ($DescribeOnly)")
    first_invocation = source.index("$Existing = @(Get-ScheduledTask")
    assert describe < first_invocation
    assert "-Hidden -Disable" in source
    assert "Disable-ScheduledTask" in source
    assert "Stop-ScheduledTask" in source
    assert "Export-ScheduledTask" in source
