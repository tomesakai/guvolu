"""刷新冻结运行根，以不可变来源快照驱动 dry-run 与 paper。"""
from __future__ import annotations

import argparse
import base64
import ctypes
import hashlib
import importlib
import json
import math
import os
import re
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Callable, Iterator, Mapping, Protocol, Sequence, cast

# 执行仓内 paper 相对路径
PAPER_CONFIG = "config/paper_executor.json"
PAPER_LEDGER_ROOT = "data"
PAPER_ROOT = "data/execution/paper"
TARGET_DIRECTORY = "data/execution/targets"
SOURCE_DIRECTORY = "data/execution/sources/frozen-forward"
CONFIG_SOURCE_DIRECTORY = "data/execution/sources/frozen-shadow-config"
SHADOW_ROOT = "data/execution/shadow/frozen-forward"
PAPER_MODE = "paper"
DRY_RUN_MODE = "dry-run"
DEFAULT_MAX_PREDICTION_AGE_MINUTES = 45
DEFAULT_CHILD_TIMEOUT_SECONDS = 300
GIT_CHILD_TIMEOUT_SECONDS = 60
MAX_CHILD_OUTPUT_BYTES = 1_000_000
CHILD_CLEANUP_GRACE_SECONDS = 2.0
RECEIPT_SCHEMA_VERSION = 6
RECEIPT_KIND = "frozen_forward_stage_receipt"
RECEIPT_STATUS = "succeeded"
RECEIPT_COMMIT_STATE = "committed"
ENVIRONMENT_ATTESTATION = "partial"
WINDOWS_GUARD_STRENGTH = "windows_no_share_write_delete"
POSIX_GUARD_STRENGTH = "posix_pre_post_only"
MAX_MANIFEST_FILES = 10_000
MAX_MANIFEST_BYTES = 1_000_000_000
PYCACHE_SENTINEL_BODY = b"guvolu-python-cache-disabled-v1\n"
EMPTY_CHILD_ENV_BODY = b"# governed empty child environment v1\n"
ORDER_ENDPOINT = "POST /v1/order"
_DRY_SKIP_REASONS = frozenset({
    "目标为零，无需委托",
    "折算数量低于最小委托量，无需委托",
})
_PAPER_SKIP_REASONS = frozenset({
    "差分为零，无需委托",
    "差分名义在不交易带内，无需委托",
    "差分数量低于最小委托量，无需委托",
})
_DRY_READ_ENDPOINTS = frozenset({
    "GET /v1/symbols", "GET /v1/ticker", "GET /v1/status",
})
_PAPER_READ_ENDPOINTS = frozenset({
    "GET /v1/symbols", "GET /v1/orderbooks", "GET /v1/status",
})
_PAPER_STATUSES = frozenset({
    "duplicate_prediction",
    "book_unavailable",
    "skipped",
    "sell_exceeds_position",
    "PAPER_FILLED",
    "PAPER_REJECTED",
})
_DRY_REPORT_KEYS = frozenset({
    "generated_at", "mode", "service_status", "artifact", "budget_jpy",
    "reference_price", "proposal", "skip_reason", "intent", "endpoints",
    "ledger_path",
})
_DRY_ARTIFACT_KEYS = frozenset({
    "path", "sha256", "run_id", "decision_time", "market_id", "unit",
    "aggregate_target",
})
_PROPOSAL_KEYS = frozenset({
    "symbol", "side", "size", "price", "notional_jpy",
})
_DRY_INTENT_KEYS = frozenset({
    "intent_id", "correlation_id", "state", "order_id", "reason",
})
_ENDPOINT_KEYS = frozenset({
    "read_touched", "write_planned", "write_touched",
})
_PAPER_LEDGER_KEYS = frozenset({
    "intent_ledger", "position_ledger", "difference_ledger", "claim_ledger",
})
_PAPER_DUPLICATE_KEYS = frozenset({
    "generated_at", "target_path", "target_sha256", "ledger_paths",
    "prediction_id", "status", "endpoints", "startup",
})
_PAPER_DECISION_KEYS = frozenset({
    "schema_version", "record", "at", "prediction_id", "decision_time",
    "valid_until", "correlation_id", "market_id", "symbol", "mode",
    "target_path", "target_sha256", "exposure_target", "risk_budget_jpy",
    "target_notional_jpy", "reference_price", "position_before",
    "position_after", "status", "book_error", "delta", "intent", "fill",
    "cost", "fee", "overlay", "service_status", "endpoints",
    "generated_at", "ledger_paths", "startup",
})
_PAPER_DELTA_KEYS = frozenset({
    "desired_size", "position_size", "delta_size", "proposal", "skip_reason",
})
_PAPER_INTENT_KEYS = frozenset({
    "intent_id", "side", "size", "price", "state", "reason",
})
_PAPER_FILL_KEYS = frozenset({
    "side", "fill_size", "expected_price", "model_fill_price",
    "notional_jpy", "fee_jpy", "levels_consumed", "fill_basis",
    "fee_source", "book_observed_at",
})
_PAPER_COST_KEYS = frozenset({
    "fee_bps", "half_spread_bps", "impact_bps",
    "slippage_vs_reference_bps", "total_cost_bps",
})
_PAPER_FEE_KEYS = frozenset({"bps", "source", "detail"})
_PAPER_STARTUP_KEYS = frozenset({"recovered_sends", "limit_usage"})
_PAPER_RECOVERY_KEYS = frozenset({"intent_ids", "state", "reason"})
_PAPER_LIMIT_KEYS = frozenset({
    "trading_day", "total_jpy", "order_count", "replayed_intents",
})
_TARGET_KEYS = frozenset({
    "artifact_kind", "bar_interval", "correlation_id",
    "correlation_id_source", "decision_time", "exposure_target", "lineage",
    "market_id", "method_version", "mode", "operational_target_contract",
    "quality", "risk_budget_jpy", "run_id", "schema_version", "symbol",
    "target_semantics", "valid_from", "valid_until", "valid_until_source",
})
_TARGET_LINEAGE_REQUIRED_KEYS = frozenset({
    "input_head_generation", "plan_id", "prediction_id",
    "source_prediction_path", "source_prediction_sha256",
})
_TARGET_CONTRACT_KEYS = frozenset({
    "aggregate_target", "families", "reserve", "unit",
})
_TARGET_SEMANTICS_KEYS = frozenset({
    "domain", "range", "reference", "short_allowed",
})
_PAPER_CONFIG_KEYS = frozenset({
    "schema_version", "market_id", "symbol", "bar_interval",
    "risk_budget_jpy", "no_trade_band", "taker_fee_fallback_bps",
    "taker_fee_cache_seconds", "overlay", "ledger_directory",
})
_PAPER_CONFIG_OVERLAY_KEYS = frozenset({
    "limit", "maximum_spread_bps", "minimum_top5_depth_base",
    "maximum_anchor_age_seconds",
})
_RECEIPT_KEYS = frozenset({
    "schema_version", "kind", "commit_state", "status", "stage",
    "plan_id", "prediction_id", "source_origin", "source_snapshot",
    "target", "report", "config_origin", "config_snapshot",
    "runner_python", "git_executable", "execution_environment",
    "code_repository", "runtime_repository",
    "execution_repository",
})
_IDENTITY_KEYS = frozenset({"path", "sha256"})
_EXECUTION_IDENTITY_KEYS = frozenset({"path", "head_commit"})
_ENVIRONMENT_IDENTITY_KEYS = frozenset({
    "attestation", "guard_strength", "python", "pyvenv_config",
    "manifest", "pycache_sentinel", "empty_child_env",
    "file_count", "total_bytes",
    "tree_sha256", "import_closures",
})
_IMPORT_CLOSURE_KEYS = frozenset({
    "role", "root", "manifest", "file_count", "total_bytes",
    "tree_sha256",
})
_IMPORT_CLOSURES_KEYS = frozenset({"code", "runtime", "execution"})
_PREDICTION_STDOUT_KEYS = frozenset({
    "prediction_id", "prediction_path", "prediction_sha256", "decision_time",
    "aggregate_target",
})
_ADAPTER_STDOUT_KEYS = frozenset({"path", "sha256", "status"})
_FROZEN_PLAN_ID = re.compile(r"^frozen-forward-plan-[0-9a-f]{64}$")
_FROZEN_PREDICTION_ID = re.compile(
    r"^frozen-forward-prediction-[0-9a-f]{64}$",
)


@dataclass(frozen=True)
class FileIdentity:
    """一次稳定读取所得的路径与字节散列。"""

    path: Path
    sha256: str
    # 系统可执行文件允许硬链接
    # 原生句柄防别名写入
    allow_hardlinks: bool = field(default=False, compare=False, repr=False)


class _FcntlModule(Protocol):
    LOCK_EX: int
    LOCK_UN: int

    def flock(self, descriptor: int, operation: int) -> None: ...


@dataclass(frozen=True)
class ExecutionIdentity:
    """一次 Git 仓受跟踪代码身份。"""

    path: Path
    head_commit: str


@dataclass(frozen=True)
class ImportClosureIdentity:
    """内容寻址且逐模块列举的 Python 导入闭包。"""

    role: str
    repository: Path
    source_root: Path
    root: Path
    manifest: FileIdentity
    file_count: int
    total_bytes: int
    tree_sha256: str
    files: tuple[FileIdentity, ...]
    relative_files: tuple[str, ...]


@dataclass(frozen=True)
class EnvironmentIdentity:
    """执行 venv 的完整清单；部分证明不覆盖 venv 外基础解释器。"""

    attestation: str
    guard_strength: str
    python: FileIdentity
    pyvenv_config: FileIdentity
    manifest: FileIdentity
    pycache_sentinel: FileIdentity
    empty_child_env: FileIdentity
    file_count: int
    total_bytes: int
    tree_sha256: str
    files: tuple[FileIdentity, ...]
    code_import: ImportClosureIdentity | None = None
    runtime_import: ImportClosureIdentity | None = None
    execution_import: ImportClosureIdentity | None = None


@dataclass(frozen=True)
class TargetExpectation:
    """预测、配置与调用参数共同封闭的目标合同。"""

    prediction_id: str
    plan_id: str
    decision_time: datetime
    valid_until: datetime
    valid_until_source: str
    correlation_id: str
    correlation_id_source: str
    market_id: str
    symbol: str
    mode: str
    bar_interval: str
    unit: str
    exposure_target: Decimal
    risk_budget_jpy: Decimal
    aggregate_target: Decimal
    source_snapshot: FileIdentity
    input_head_generation: str
    decision_input_sha256: str | None
    families: object
    reserve: object
    quality: object


@dataclass
class StageLockState:
    """将回执持久提交点传给阶段锁清理层。"""

    committed: bool = False


@dataclass(frozen=True)
class StageResult:
    """一个已由成功回执封口的执行阶段。"""

    target: FileIdentity
    report: FileIdentity
    receipt: FileIdentity
    report_payload: dict[str, object]
    reused: bool
    returncode: int | None


@dataclass(frozen=True)
class _DirectoryIdentity:
    """词法目录层级绑定到最终路径和文件 ID。"""

    lexical: str
    resolved: str
    device: int
    inode: int
    mode: int
    file_attributes: int
    reparse_tag: int


def _path_race_hook(_phase: str, _path: Path) -> None:
    """测试用无操作缝，用于确定性模拟检查/使用竞争。"""


def _absolute_lexical_path(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{name} 必须使用绝对规范路径")
    normalized = os.path.normpath(str(path))
    if os.path.normcase(str(path)) != os.path.normcase(normalized):
        raise ValueError(f"{name} 不得使用含 . 或 .. 的路径别名")
    return Path(os.path.abspath(path))


def _directory_components(directory: Path) -> tuple[Path, ...]:
    lexical = _absolute_lexical_path(directory, "目录")
    parents = list(lexical.parents)
    parents.reverse()
    return tuple((*parents, lexical))


def _directory_identity(path: Path, name: str) -> _DirectoryIdentity:
    lexical = _absolute_lexical_path(path, name)
    try:
        metadata = lexical.lstat()
        resolved = lexical.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"{name} 不存在或无法读取身份: {lexical}") from exc
    attributes = int(getattr(metadata, "st_file_attributes", 0))
    reparse_tag = int(getattr(metadata, "st_reparse_tag", 0))
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or attributes & reparse_flag
        or reparse_tag
    ):
        raise ValueError(f"{name} 不得是 symlink、junction 或 reparse point")
    if os.path.normcase(str(lexical)) != os.path.normcase(str(resolved)):
        raise ValueError(f"{name} 不得使用非规范路径或目录别名")
    return _DirectoryIdentity(
        str(lexical),
        str(resolved),
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_mode),
        attributes,
        reparse_tag,
    )


def _capture_directory_chain(
    directory: Path, name: str,
) -> tuple[_DirectoryIdentity, ...]:
    return tuple(
        _directory_identity(component, f"{name} 目录层级")
        for component in _directory_components(directory)
    )


def _revalidate_directory_chain(
    directory: Path,
    expected: tuple[_DirectoryIdentity, ...],
    name: str,
) -> None:
    if _capture_directory_chain(directory, name) != expected:
        raise ValueError(f"{name} 目录身份在关键操作期间发生变化")


class _WindowsFileTime(ctypes.Structure):
    _fields_ = (("low", ctypes.c_uint32), ("high", ctypes.c_uint32))


class _WindowsFileInformation(ctypes.Structure):
    _fields_ = (
        ("file_attributes", ctypes.c_uint32),
        ("creation_time", _WindowsFileTime),
        ("last_access_time", _WindowsFileTime),
        ("last_write_time", _WindowsFileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    )

    @property
    def file_index(self) -> int:
        return (int(self.file_index_high) << 32) | int(self.file_index_low)


class _WindowsJobBasicLimit(ctypes.Structure):
    _fields_ = (
        ("per_process_time", ctypes.c_int64),
        ("per_job_time", ctypes.c_int64),
        ("limit_flags", ctypes.c_uint32),
        ("minimum_working_set", ctypes.c_size_t),
        ("maximum_working_set", ctypes.c_size_t),
        ("active_process_limit", ctypes.c_uint32),
        ("affinity", ctypes.c_size_t),
        ("priority_class", ctypes.c_uint32),
        ("scheduling_class", ctypes.c_uint32),
    )


class _WindowsIoCounters(ctypes.Structure):
    _fields_ = tuple(
        (name, ctypes.c_uint64)
        for name in (
            "read_operations", "write_operations", "other_operations",
            "read_bytes", "write_bytes", "other_bytes",
        )
    )


class _WindowsJobExtendedLimit(ctypes.Structure):
    _fields_ = (
        ("basic", _WindowsJobBasicLimit),
        ("io", _WindowsIoCounters),
        ("process_memory_limit", ctypes.c_size_t),
        ("job_memory_limit", ctypes.c_size_t),
        ("peak_process_memory", ctypes.c_size_t),
        ("peak_job_memory", ctypes.c_size_t),
    )


class _WindowsJobBasicAccounting(ctypes.Structure):
    _fields_ = (
        ("per_process_user_time", ctypes.c_int64),
        ("per_job_user_time", ctypes.c_int64),
        ("this_period_total_user_time", ctypes.c_int64),
        ("this_period_total_kernel_time", ctypes.c_int64),
        ("total_user_time", ctypes.c_int64),
        ("total_kernel_time", ctypes.c_int64),
        ("total_page_fault_count", ctypes.c_uint32),
        ("total_processes", ctypes.c_uint32),
        ("active_processes", ctypes.c_uint32),
        ("total_terminated_processes", ctypes.c_uint32),
    )


@dataclass(frozen=True)
class _PinnedDirectory:
    path: Path
    identity: tuple[_DirectoryIdentity, ...]
    descriptor: int | None
    windows_handles: tuple[int, ...]


def _windows_kernel32() -> Any:
    loader = cast(Any, getattr(ctypes, "WinDLL"))
    return loader("kernel32", use_last_error=True)


def _windows_open_directory_handle(
    path: Path, expected: _DirectoryIdentity,
) -> int:
    kernel = _windows_kernel32()
    create_file = kernel.CreateFileW
    create_file.argtypes = (
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    )
    create_file.restype = ctypes.c_void_p
    handle_value = create_file(
        str(path),
        0x80,
        0x1 | 0x2,
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle_value is None or int(handle_value) == invalid:
        error = ctypes.get_last_error()
        raise OSError(error, f"无法固定目录句柄: {path}")
    handle = int(handle_value)
    get_information = kernel.GetFileInformationByHandle
    get_information.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsFileInformation),
    )
    get_information.restype = ctypes.c_int
    information = _WindowsFileInformation()
    if not get_information(ctypes.c_void_p(handle), ctypes.byref(information)):
        error = ctypes.get_last_error()
        kernel.CloseHandle(ctypes.c_void_p(handle))
        raise OSError(error, f"无法读取目录句柄身份: {path}")
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    directory_flag = int(getattr(stat, "FILE_ATTRIBUTE_DIRECTORY", 0x10))
    if (
        information.file_index != expected.inode
        or not int(information.file_attributes) & directory_flag
        or int(information.file_attributes) & reparse_flag
    ):
        kernel.CloseHandle(ctypes.c_void_p(handle))
        raise ValueError(f"目录句柄与已验证路径身份不一致: {path}")
    return handle


def _windows_close_handle(handle: int) -> None:
    kernel = _windows_kernel32()
    close_handle = kernel.CloseHandle
    close_handle.argtypes = (ctypes.c_void_p,)
    close_handle.restype = ctypes.c_int
    if not close_handle(ctypes.c_void_p(handle)):
        error = ctypes.get_last_error()
        raise OSError(error, "无法关闭目录句柄")


def _cleanup_pinned_resource(
    action: Callable[[], None],
    *,
    state: StageLockState | None,
    body_failed: bool,
    phase: str,
    path: Path,
) -> None:
    try:
        action()
        _path_race_hook(phase, path)
    except OSError:
        if not body_failed and not (state is not None and state.committed):
            raise


@contextmanager
def _pin_directory_chain(
    directory: Path,
    name: str,
    *,
    transaction_state: StageLockState | None = None,
) -> Iterator[_PinnedDirectory]:
    """固定整条 Windows 目录链；POSIX 固定直接父并绑定全链身份。"""
    lexical = _absolute_lexical_path(directory, name)
    identity = _capture_directory_chain(lexical, name)
    descriptor: int | None = None
    handles: list[int] = []
    try:
        if os.name == "nt":
            for item in identity:
                handles.append(
                    _windows_open_directory_handle(Path(item.lexical), item),
                )
        else:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(lexical, flags)
            metadata = os.fstat(descriptor)
            expected = identity[-1]
            if (
                int(metadata.st_dev) != expected.device
                or int(metadata.st_ino) != expected.inode
                or not stat.S_ISDIR(metadata.st_mode)
            ):
                raise ValueError(f"{name} 固定句柄与路径身份不一致")
        _revalidate_directory_chain(lexical, identity, name)
        yield _PinnedDirectory(lexical, identity, descriptor, tuple(handles))
    finally:
        body_failed = sys.exc_info()[0] is not None
        cleanup_error: OSError | None = None
        if descriptor is not None:
            try:
                _cleanup_pinned_resource(
                    lambda: os.close(descriptor),
                    state=transaction_state,
                    body_failed=body_failed,
                    phase="directory-handle-close-after-effect",
                    path=lexical,
                )
            except OSError as exc:
                cleanup_error = exc
        for handle in reversed(handles):
            try:
                def close_current(handle_value: int = handle) -> None:
                    _windows_close_handle(handle_value)

                _cleanup_pinned_resource(
                    close_current,
                    state=transaction_state,
                    body_failed=body_failed or cleanup_error is not None,
                    phase="directory-handle-close-after-effect",
                    path=lexical,
                )
            except OSError as exc:
                if cleanup_error is None:
                    cleanup_error = exc
        if cleanup_error is not None:
            raise cleanup_error


def _ensure_canonical_directory_tree(directory: Path, name: str) -> Path:
    """逐级新建目录，并在每一级拒绝 symlink/junction/reparse。"""
    lexical = _absolute_lexical_path(directory, name)
    missing: list[Path] = []
    current = lexical
    while True:
        try:
            current.lstat()
        except FileNotFoundError:
            missing.append(current)
            parent = current.parent
            if parent == current:
                raise ValueError(f"{name} 无法定位既有父目录")
            current = parent
            continue
        break
    _capture_directory_chain(current, name)
    for candidate in reversed(missing):
        expected = current / candidate.name
        if os.path.normcase(str(expected)) != os.path.normcase(str(candidate)):
            raise ValueError(f"{name} 包含非规范目录层级")
        with _pin_directory_chain(current, name) as pinned:
            _path_race_hook("directory-before-mkdir", candidate)
            _revalidate_directory_chain(current, pinned.identity, name)
            try:
                if pinned.descriptor is None:
                    candidate.mkdir()
                else:
                    os.mkdir(candidate.name, dir_fd=pinned.descriptor)
            except FileExistsError:
                pass
            if pinned.descriptor is None:
                _fsync_directory(current)
            else:
                os.fsync(pinned.descriptor)
            _path_race_hook("directory-after-mkdir", candidate)
            _revalidate_directory_chain(current, pinned.identity, name)
            _capture_directory_chain(candidate, name)
        current = candidate
    return Path(_directory_identity(lexical, name).lexical)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} 不是 JSON 对象")
    return {str(key): item for key, item in value.items()}


def _exact_object(
    value: object, expected: frozenset[str], name: str,
) -> dict[str, object]:
    result = _object(value, name)
    if set(result) != expected:
        raise ValueError(f"{name} 字段不符合精确合同")
    return result


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 缺失")
    return value


def _digest(value: object, name: str) -> str:
    digest = _text(value, name)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"{name} 不是小写 SHA-256")
    return digest


def _frozen_id(value: object, pattern: re.Pattern[str], name: str) -> str:
    identifier = _text(value, name)
    if pattern.fullmatch(identifier) is None:
        raise ValueError(f"{name} 不是规范冻结标识")
    return identifier


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(str(left)) == os.path.normcase(str(right))


def _absolute_canonical_path(path: Path, name: str) -> Path:
    """保留词法身份，拒绝相对、``.``、``..`` 与目录别名。"""
    if not path.is_absolute():
        raise ValueError(f"{name} 必须是绝对路径")
    lexical = Path(os.path.abspath(path))
    if not _same_path(path, lexical):
        raise ValueError(f"{name} 含非规范路径层级")
    resolved = lexical.resolve(strict=False)
    if not _same_path(lexical, resolved):
        raise ValueError(f"{name} 使用 symlink、junction 或目录别名")
    return lexical


def _execution_root(path: Path) -> Path:
    """要求执行仓已存在且整条父链无别名。"""
    if not path.is_absolute() or not path.is_dir():
        raise ValueError("执行仓必须是已存在的绝对目录")
    return _ensure_canonical_directory_tree(path, "执行仓")


def _managed_directory(execution: Path, relative: str, name: str) -> Path:
    """在执行仓内建立无 symlink/junction 的受管目录。"""
    relative_path = Path(relative)
    if relative_path.is_absolute() or any(
        part in {"", ".", ".."} for part in relative_path.parts
    ):
        raise ValueError(f"{name} 相对路径非法")
    candidate = execution.joinpath(*relative_path.parts)
    if not candidate.is_relative_to(execution):
        raise ValueError(f"{name} 越出执行仓")
    canonical = _ensure_canonical_directory_tree(candidate, name)
    if not canonical.is_relative_to(execution):
        raise ValueError(f"{name} 物理路径越出执行仓")
    return canonical


def _managed_file_path(parent: Path, filename: str, name: str) -> Path:
    """在已固定受管目录内构造一个直接子文件路径。"""
    if (
        not filename
        or Path(filename).name != filename
        or filename in {".", ".."}
    ):
        raise ValueError(f"{name} 文件名非法")
    path = _absolute_canonical_path(parent / filename, name)
    if not _same_path(path.parent, parent):
        raise ValueError(f"{name} 越出受管目录")
    return path


def _reported_managed_file(
    value: object, parent: Path, name: str,
) -> Path:
    raw = Path(_text(value, name))
    path = _absolute_canonical_path(raw, name)
    if not _same_path(path.parent, parent):
        raise ValueError(f"{name} 越出受管目录")
    return path


def _sequence_of_text(value: object, name: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ValueError(f"{name} 必须是非空文本组成的数组")
    if len(value) != len(set(value)):
        raise ValueError(f"{name} 不得含重复项")
    return list(value)


def _decimal(
    value: object,
    name: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须是 Decimal 字符串")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} 不是合法 Decimal") from exc
    if not number.is_finite():
        raise ValueError(f"{name} 必须是有限 Decimal")
    if positive and number <= 0:
        raise ValueError(f"{name} 必须为正")
    if nonnegative and number < 0:
        raise ValueError(f"{name} 不得为负")
    return number


def _finite_number(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} 必须是有限 JSON 数值")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{name} 必须是有限 JSON 数值") from exc
    if not number.is_finite():
        raise ValueError(f"{name} 必须是有限 JSON 数值")
    return number


def _timestamp(value: object, name: str) -> datetime:
    text = _text(value, name)
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} 不是合法 ISO-8601 时间") from exc
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"{name} 必须带时区")
    return moment.astimezone(UTC)


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _text(value, name)


def _assert_distinct_files(left: Path, right: Path, name: str) -> None:
    try:
        same = os.path.samefile(left, right)
    except OSError as exc:
        raise ValueError(f"{name} 无法比较文件身份") from exc
    if same:
        raise ValueError(f"{name} 不得是同一文件")


def _validate_optional_single_file(path: Path, name: str) -> None:
    """若受管叶文件存在，则要求它是稳定、常规、单链接文件。"""
    if _entry_exists(path):
        _stable_file_bytes(path, name)


_PINNED_GIT: ContextVar[FileIdentity | None] = ContextVar(
    "frozen_shadow_git", default=None,
)


@contextmanager
def _use_git_executable(identity: FileIdentity) -> Iterator[None]:
    if _PINNED_GIT.get() is not None:
        raise RuntimeError("Git 执行身份发生嵌套")
    token = _PINNED_GIT.set(identity)
    try:
        yield
    finally:
        _PINNED_GIT.reset(token)


def _git_command(repository: Path, *arguments: str) -> tuple[str, ...]:
    identity = _PINNED_GIT.get()
    if identity is None:
        raise RuntimeError("Git 执行文件尚未绑定")
    _stable_identity(
        identity.path,
        "Git 执行文件",
        identity.sha256,
        allow_hardlinks=identity.allow_hardlinks,
    )
    return (
        str(identity.path),
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
        "-C",
        str(repository),
        *arguments,
    )


def _git_environment() -> dict[str, str]:
    """仅保留 Git 启动所需 OS 环境，并禁用全部外部 Git 重定向。"""
    child: dict[str, str] = {}
    for key in ("SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP"):
        value = os.environ.get(key)
        if value is not None:
            child[key] = value
    child.update({
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_CONFIG_COUNT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_TERMINAL_PROMPT": "0",
        "LC_ALL": "C",
    })
    return child


def _business_child_environment(mode: str | None = None) -> dict[str, str]:
    """构造不含密钥、代理、Python/Git 注入项的业务 child 环境。"""
    child: dict[str, str] = {}
    for key in (
        "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "TMPDIR",
    ):
        value = os.environ.get(key)
        if value is not None:
            child[key] = value
    if mode is not None:
        child["GUVOLU_MODE"] = mode
    return child


def _git_output(repository: Path, *arguments: str) -> str:
    result = _run(
        _git_command(repository, *arguments),
        cwd=repository,
        env=_git_environment(),
        timeout_seconds=GIT_CHILD_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ValueError(f"无法验证 Git 身份: {detail}")
    return result.stdout.strip()


def _repository_identity(
    repository: Path,
    name: str,
    expected: ExecutionIdentity | None = None,
    *,
    require_detached: bool = False,
    reject_untracked_scopes: Sequence[str] = (),
) -> ExecutionIdentity:
    """绑定 Git 根、HEAD 与 tracked-clean 状态。"""
    root = _absolute_canonical_path(
        Path(_git_output(repository, "rev-parse", "--show-toplevel")),
        f"{name} Git 根",
    )
    if not _same_path(root, repository):
        raise ValueError(f"{name} 路径不是 Git 工作树根")
    head = _git_output(repository, "rev-parse", "HEAD")
    if re.fullmatch(r"[0-9a-f]{40,64}", head) is None:
        raise ValueError(f"{name} HEAD 不是规范提交标识")
    dirty = _git_output(
        repository, "status", "--porcelain=v1", "--untracked-files=no",
    )
    if dirty:
        raise ValueError(f"{name} 含受跟踪未提交改动")
    if reject_untracked_scopes:
        untracked = _git_output(
            repository, "ls-files", "--others", "--exclude-standard", "--",
            *reject_untracked_scopes,
        )
        if untracked:
            raise ValueError(f"{name} 代码路径含未跟踪文件")
        ignored = _git_output(
            repository,
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--",
            *reject_untracked_scopes,
        )
        if ignored:
            raise ValueError(f"{name} 代码路径含 ignored 注入文件")
    if require_detached:
        symbolic = _run(
            _git_command(repository, "symbolic-ref", "-q", "HEAD"),
            cwd=repository,
            env=_git_environment(),
            timeout_seconds=GIT_CHILD_TIMEOUT_SECONDS,
        )
        if symbolic.returncode == 0:
            raise ValueError(f"{name} 必须是 detached worktree")
        if symbolic.returncode != 1:
            raise ValueError(f"无法验证 {name} detached 状态")
    identity = ExecutionIdentity(repository, head)
    if expected is not None and identity != expected:
        raise ValueError(f"{name} tracked-clean HEAD 已变化")
    return identity


def _execution_identity(
    execution: Path,
    expected: ExecutionIdentity | None = None,
) -> ExecutionIdentity:
    return _repository_identity(
        execution,
        "执行仓",
        expected,
        reject_untracked_scopes=("src", "scripts"),
    )


def _execution_payload(identity: ExecutionIdentity) -> dict[str, object]:
    return {"path": str(identity.path), "head_commit": identity.head_commit}


def _git_tracked_identities(repository: Path, name: str) -> tuple[FileIdentity, ...]:
    raw = _git_output(repository, "ls-files", "-z")
    relative_paths = [item for item in raw.split("\0") if item]
    if len(relative_paths) > MAX_MANIFEST_FILES:
        raise ValueError(f"{name} 跟踪文件超过上限")
    identities: list[FileIdentity] = []
    total_bytes = 0
    for relative in relative_paths:
        lexical = Path(relative)
        if lexical.is_absolute() or ".." in lexical.parts:
            raise ValueError(f"{name} Git 路径非法")
        path = _absolute_canonical_path(repository / lexical, f"{name} 跟踪文件")
        body = _stable_file_bytes(path, f"{name} 跟踪文件")
        total_bytes += len(body)
        if total_bytes > MAX_MANIFEST_BYTES:
            raise ValueError(f"{name} 跟踪字节超过上限")
        identities.append(FileIdentity(path, _sha256_bytes(body)))
    return tuple(identities)


def _venv_inventory(
    root: Path,
) -> tuple[tuple[FileIdentity, ...], int, int, str, bytes]:
    root = _absolute_canonical_path(root, "执行 venv 根")
    _capture_directory_chain(root, "执行 venv 根")
    paths: list[Path] = []
    for path in root.rglob("*"):
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            _directory_identity(path, "执行 venv 目录")
        else:
            paths.append(path)
    paths.sort(key=lambda item: item.relative_to(root).as_posix())
    if len(paths) > MAX_MANIFEST_FILES:
        raise ValueError("执行 venv 文件数超过上限")
    files: list[FileIdentity] = []
    entries: list[dict[str, object]] = []
    tree_parts: list[bytes] = []
    total_bytes = 0
    for path in paths:
        body = _stable_file_bytes(
            path, "执行 venv 文件", allow_hardlinks=True,
        )
        total_bytes += len(body)
        if total_bytes > MAX_MANIFEST_BYTES:
            raise ValueError("执行 venv 总字节超过上限")
        identity = FileIdentity(path, _sha256_bytes(body), True)
        files.append(identity)
        entries.append({
            "path": path.relative_to(root).as_posix(),
            "size": len(body),
            "sha256": identity.sha256,
        })
        tree_parts.append(
            path.relative_to(root).as_posix().encode("utf-8")
            + b"\0"
            + str(len(body)).encode("ascii")
            + b"\0"
            + identity.sha256.encode("ascii")
            + b"\n"
        )
    entry_bytes = b"".join(tree_parts)
    tree_sha256 = _sha256_bytes(entry_bytes)
    manifest_body = (json.dumps({
        "schema_version": 1,
        "root": str(root),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "tree_sha256": tree_sha256,
        "files": entries,
    }, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8",
    )
    return tuple(files), len(files), total_bytes, tree_sha256, manifest_body


def _execution_environment_identity(
    execution: Path,
    expected: EnvironmentIdentity | None = None,
) -> EnvironmentIdentity:
    """绑定完整执行 venv，基础标准库仍是 partial。"""
    files, count, total, tree_sha256, manifest_body = _venv_inventory(
        execution / ".venv",
    )
    by_path = {identity.path: identity for identity in files}
    python_path = _absolute_canonical_path(
        execution / ".venv/Scripts/python.exe", "执行 Python",
    )
    pyvenv_path = _absolute_canonical_path(
        execution / ".venv/pyvenv.cfg", "执行 pyvenv.cfg",
    )
    if python_path not in by_path or pyvenv_path not in by_path:
        raise ValueError("执行 venv 缺少启动文件")
    manifest_root = _managed_directory(
        execution,
        f"{SHADOW_ROOT}/environment-manifests",
        "执行 venv 清单目录",
    )
    manifest_path = _managed_file_path(
        manifest_root, f"venv-{tree_sha256}.json", "执行 venv 清单",
    )
    pycache_path = _managed_file_path(
        manifest_root, "pycache-disabled.sentinel", "Python 缓存哨兵",
    )
    empty_env_path = _managed_file_path(
        manifest_root,
        f"child-env-{_sha256_bytes(EMPTY_CHILD_ENV_BODY)}.env",
        "空 child 环境文件",
    )
    lock_root = _managed_directory(
        execution, f"{SHADOW_ROOT}/locks", "执行环境锁目录",
    )
    lock_path = _managed_file_path(
        lock_root, f"environment-{tree_sha256}.lock", "执行环境清单锁",
    )
    lock_state = StageLockState()
    with _exclusive_stage_lock(
        lock_path, "执行环境清单锁", lock_state,
    ):
        _atomic_publish_new(
            manifest_path, manifest_body, allow_existing_identical=True,
        )
        manifest = _stable_identity(
            manifest_path,
            "执行 venv 清单",
            _sha256_bytes(manifest_body),
        )
        _atomic_publish_new(
            pycache_path,
            PYCACHE_SENTINEL_BODY,
            allow_existing_identical=True,
        )
        pycache_sentinel = _stable_identity(
            pycache_path,
            "Python 缓存哨兵",
            _sha256_bytes(PYCACHE_SENTINEL_BODY),
        )
        _atomic_publish_new(
            empty_env_path,
            EMPTY_CHILD_ENV_BODY,
            allow_existing_identical=True,
        )
        empty_child_env = _stable_identity(
            empty_env_path,
            "空 child 环境文件",
            _sha256_bytes(EMPTY_CHILD_ENV_BODY),
        )
        lock_state.committed = True
    identity = EnvironmentIdentity(
        ENVIRONMENT_ATTESTATION,
        WINDOWS_GUARD_STRENGTH if os.name == "nt" else POSIX_GUARD_STRENGTH,
        by_path[python_path],
        by_path[pyvenv_path],
        manifest,
        pycache_sentinel,
        empty_child_env,
        count,
        total,
        tree_sha256,
        files,
        None if expected is None else expected.code_import,
        None if expected is None else expected.runtime_import,
        None if expected is None else expected.execution_import,
    )
    if expected is not None:
        for closure in _import_closures(identity):
            _revalidate_import_closure(closure)
    if expected is not None and identity != expected:
        raise ValueError("执行 venv 完整清单已变化")
    return identity


def _import_closures(
    identity: EnvironmentIdentity,
) -> tuple[ImportClosureIdentity, ...]:
    closures = (
        identity.code_import,
        identity.runtime_import,
        identity.execution_import,
    )
    if any(closure is None for closure in closures):
        raise ValueError("执行环境缺少完整 import closure")
    return cast(tuple[ImportClosureIdentity, ...], closures)


def _module_candidate(
    relative: PurePosixPath,
) -> tuple[str, str, bool] | None:
    suffix = relative.suffix.lower()
    parts = list(relative.parts)
    if suffix == ".py":
        name = relative.stem
        module_parts = parts[:-1]
        is_package = name == "__init__"
        if not is_package:
            module_parts.append(name)
        kind = "source"
    elif suffix in {".pyc", ".pyo"}:
        name = relative.name.split(".", 1)[0]
        if len(parts) >= 2 and parts[-2] == "__pycache__":
            module_parts = [*parts[:-2], name]
        else:
            module_parts = [*parts[:-1], name]
        is_package = name == "__init__"
        if is_package:
            module_parts = module_parts[:-1]
        kind = "bytecode"
    elif suffix in {".pyd", ".so"}:
        name = relative.name.split(".", 1)[0]
        module_parts = [*parts[:-1], name]
        is_package = name == "__init__"
        if is_package:
            module_parts = module_parts[:-1]
        kind = "extension"
    else:
        return None
    if not module_parts or any(not part.isidentifier() for part in module_parts):
        return None
    return ".".join(module_parts), kind, is_package


def _build_import_closure(
    execution: Path,
    environment: EnvironmentIdentity,
    *,
    role: str,
    repository: Path,
    tracked: Sequence[FileIdentity],
) -> ImportClosureIdentity:
    if role not in _IMPORT_CLOSURES_KEYS:
        raise ValueError("import closure role 非法")
    source_root = _absolute_canonical_path(repository / "src", "受控源码根")
    scripts_root = _absolute_canonical_path(repository / "scripts", "受控脚本根")
    site_root = _absolute_canonical_path(
        environment.python.path.parents[1] / "Lib/site-packages",
        "执行 site-packages",
    )
    captured: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    for identity in tracked:
        if not (
            identity.path.is_relative_to(source_root)
            or identity.path.is_relative_to(scripts_root)
        ):
            continue
        relative = "repo/" + identity.path.relative_to(repository).as_posix()
        if relative in seen:
            raise ValueError("import closure 仓文件路径重复")
        body = _stable_file_bytes(identity.path, "import closure 仓文件")
        if _sha256_bytes(body) != identity.sha256:
            raise ValueError("import closure 仓文件身份变化")
        seen.add(relative)
        captured.append((relative, body))
    for identity in environment.files:
        if not identity.path.is_relative_to(site_root):
            continue
        relative = "site/" + identity.path.relative_to(site_root).as_posix()
        if relative in seen:
            raise ValueError("import closure site 文件路径重复")
        body = _stable_file_bytes(
            identity.path,
            "import closure site 文件",
            allow_hardlinks=identity.allow_hardlinks,
        )
        if _sha256_bytes(body) != identity.sha256:
            raise ValueError("import closure site 文件身份变化")
        seen.add(relative)
        captured.append((relative, body))
    captured.sort(key=lambda item: item[0])
    if not captured:
        raise ValueError("import closure 不能为空")
    total_bytes = sum(len(body) for _, body in captured)
    if len(captured) > MAX_MANIFEST_FILES or total_bytes > MAX_MANIFEST_BYTES:
        raise ValueError("import closure 超过清单上限")
    tree_body = b"".join(
        role.encode("ascii") + b"\0" + relative.encode("utf-8") + b"\0"
        + str(len(body)).encode("ascii") + b"\0"
        + _sha256_bytes(body).encode("ascii") + b"\n"
        for relative, body in captured
    )
    tree_sha256 = _sha256_bytes(tree_body)
    closure_root = _managed_directory(
        execution,
        f"{SHADOW_ROOT}/import-closures/{role}-{tree_sha256}",
        f"{role} import closure 根",
    )
    lock_root = _managed_directory(
        execution, f"{SHADOW_ROOT}/locks", "import closure 锁目录",
    )
    lock_path = _managed_file_path(
        lock_root, f"import-{role}-{tree_sha256}.lock", "import closure 锁",
    )
    identities: list[FileIdentity] = []
    modules: dict[str, dict[str, object]] = {}
    namespace_paths: dict[str, set[str]] = {}
    lock_state = StageLockState()
    with _exclusive_stage_lock(lock_path, "import closure 锁", lock_state):
        for relative, body in captured:
            logical = PurePosixPath(relative)
            parent = _ensure_canonical_directory_tree(
                closure_root.joinpath(*logical.parts[:-1]),
                f"{role} import closure 目录",
            )
            target = _managed_file_path(
                parent, logical.name, f"{role} import closure 文件",
            )
            _atomic_publish_new(target, body, allow_existing_identical=True)
            identity = _stable_identity(
                target, f"{role} import closure 文件", _sha256_bytes(body),
            )
            identities.append(identity)
            if relative.startswith("repo/src/"):
                import_relative = PurePosixPath(relative.removeprefix("repo/src/"))
                import_root = closure_root / "repo/src"
            elif relative.startswith("site/"):
                import_relative = PurePosixPath(relative.removeprefix("site/"))
                import_root = closure_root / "site"
            else:
                continue
            candidate = _module_candidate(import_relative)
            if candidate is None:
                continue
            fullname, kind, is_package = candidate
            top_level = fullname.split(".", 1)[0]
            if top_level in sys.stdlib_module_names:
                raise ValueError(
                    f"import closure 不得遮蔽标准库模块: {top_level}",
                )
            record = {
                "kind": kind,
                "path": str(target),
                "is_package": is_package,
            }
            existing = modules.get(fullname)
            if existing is None:
                modules[fullname] = record
            elif existing["kind"] == "source" and kind == "bytecode":
                pass
            elif existing["kind"] == "bytecode" and kind == "source":
                modules[fullname] = record
            else:
                raise ValueError(f"import closure 模块身份冲突: {fullname}")
            module_parts = fullname.split(".")
            for index in range(1, len(module_parts)):
                namespace = ".".join(module_parts[:index])
                namespace_paths.setdefault(namespace, set()).add(
                    str(import_root.joinpath(*module_parts[:index])),
                )
        for fullname, paths in sorted(namespace_paths.items()):
            if fullname not in modules:
                modules[fullname] = {
                    "kind": "namespace",
                    "paths": sorted(paths),
                    "is_package": True,
                }
        manifest_root = _managed_directory(
            execution,
            f"{SHADOW_ROOT}/environment-manifests",
            "import closure 清单目录",
        )
        manifest_path = _managed_file_path(
            manifest_root,
            f"import-{role}-{tree_sha256}.json",
            "import closure 清单",
        )
        file_entries = [
            {
                "path": relative,
                "size": len(body),
                "sha256": _sha256_bytes(body),
            }
            for relative, body in captured
        ]
        manifest_body = (json.dumps({
            "schema_version": 1,
            "role": role,
            "repository": str(repository),
            "source_root": str(source_root),
            "original_site_packages": str(site_root),
            "root": str(closure_root),
            "site_root": str(closure_root / "site"),
            "file_count": len(captured),
            "total_bytes": total_bytes,
            "tree_sha256": tree_sha256,
            "controlled_top_levels": sorted({
                name.split(".", 1)[0] for name in modules
            }),
            "modules": {name: modules[name] for name in sorted(modules)},
            "files": file_entries,
        }, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode(
            "utf-8",
        )
        _atomic_publish_new(
            manifest_path, manifest_body, allow_existing_identical=True,
        )
        manifest = _stable_identity(
            manifest_path, "import closure 清单", _sha256_bytes(manifest_body),
        )
        lock_state.committed = True
    closure = ImportClosureIdentity(
        role,
        repository,
        source_root,
        closure_root,
        manifest,
        len(captured),
        total_bytes,
        tree_sha256,
        tuple(identities),
        tuple(relative for relative, _ in captured),
    )
    _revalidate_import_closure(closure)
    return closure


def _revalidate_import_closure(identity: ImportClosureIdentity) -> None:
    _stable_identity(
        identity.manifest.path,
        f"{identity.role} import closure 清单",
        identity.manifest.sha256,
    )
    for item in identity.files:
        _stable_identity(item.path, f"{identity.role} import closure 文件", item.sha256)
    actual: list[str] = []
    for path in identity.root.rglob("*"):
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            _directory_identity(path, f"{identity.role} import closure 目录")
        else:
            _stable_file_bytes(path, f"{identity.role} import closure 枚举文件")
            actual.append(path.relative_to(identity.root).as_posix())
    if tuple(sorted(actual)) != tuple(sorted(identity.relative_files)):
        raise ValueError(f"{identity.role} import closure 文件集合已变化")


def _attach_import_closures(
    execution: Path,
    environment: EnvironmentIdentity,
    *,
    code_root: Path,
    code_tracked: Sequence[FileIdentity],
    runtime: Path,
    runtime_tracked: Sequence[FileIdentity],
    execution_tracked: Sequence[FileIdentity],
) -> EnvironmentIdentity:
    return replace(
        environment,
        code_import=_build_import_closure(
            execution, environment, role="code",
            repository=code_root, tracked=code_tracked,
        ),
        runtime_import=_build_import_closure(
            execution, environment, role="runtime",
            repository=runtime, tracked=runtime_tracked,
        ),
        execution_import=_build_import_closure(
            execution, environment, role="execution",
            repository=execution, tracked=execution_tracked,
        ),
    )


def _import_closure_payload(
    identity: ImportClosureIdentity,
) -> dict[str, object]:
    return {
        "role": identity.role,
        "root": str(identity.root),
        "manifest": _identity_payload(identity.manifest),
        "file_count": identity.file_count,
        "total_bytes": identity.total_bytes,
        "tree_sha256": identity.tree_sha256,
    }


def _environment_payload(
    identity: EnvironmentIdentity,
) -> dict[str, object]:
    closures = _import_closures(identity)
    return {
        "attestation": identity.attestation,
        "guard_strength": identity.guard_strength,
        "python": _identity_payload(identity.python),
        "pyvenv_config": _identity_payload(identity.pyvenv_config),
        "manifest": _identity_payload(identity.manifest),
        "pycache_sentinel": _identity_payload(identity.pycache_sentinel),
        "empty_child_env": _identity_payload(identity.empty_child_env),
        "file_count": identity.file_count,
        "total_bytes": identity.total_bytes,
        "tree_sha256": identity.tree_sha256,
        "import_closures": {
            closure.role: _import_closure_payload(closure)
            for closure in closures
        },
    }


def _windows_open_read_guard(identity: FileIdentity, name: str) -> int:
    """以只共享读取的 native 句柄禁止 child-window 改写/删除。"""
    _stable_identity(
        identity.path,
        name,
        identity.sha256,
        allow_hardlinks=identity.allow_hardlinks,
    )
    _path_race_hook("file-guard-before-open", identity.path)
    kernel = _windows_kernel32()
    create_file = kernel.CreateFileW
    create_file.argtypes = (
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    )
    create_file.restype = ctypes.c_void_p
    handle_value = create_file(
        str(identity.path), 0x80000000, 0x1, None, 3, 0x00200000, None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle_value is None or int(handle_value) == invalid:
        error = ctypes.get_last_error()
        raise OSError(error, f"无法固定只读文件: {identity.path}")
    handle = int(handle_value)
    try:
        get_information = kernel.GetFileInformationByHandle
        get_information.argtypes = (
            ctypes.c_void_p,
            ctypes.POINTER(_WindowsFileInformation),
        )
        get_information.restype = ctypes.c_int
        information = _WindowsFileInformation()
        if not get_information(
            ctypes.c_void_p(handle), ctypes.byref(information),
        ):
            raise OSError(
                ctypes.get_last_error(), f"无法读取文件句柄: {identity.path}",
            )
        metadata = identity.path.lstat()
        reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        directory_flag = int(getattr(stat, "FILE_ATTRIBUTE_DIRECTORY", 0x10))
        size = (int(information.file_size_high) << 32) | int(
            information.file_size_low,
        )
        if (
            information.file_index != metadata.st_ino
            or (
                not identity.allow_hardlinks
                and information.number_of_links != 1
            )
            or int(information.file_attributes) & reparse_flag
            or int(information.file_attributes) & directory_flag
            or size != metadata.st_size
        ):
            raise ValueError(f"{name} native 句柄身份不符")
        _stable_identity(
            identity.path,
            name,
            identity.sha256,
            allow_hardlinks=identity.allow_hardlinks,
        )
        return handle
    except BaseException:
        kernel.CloseHandle(ctypes.c_void_p(handle))
        raise


@contextmanager
def _guard_file_identities(
    identities: Sequence[FileIdentity],
    name: str,
) -> Iterator[None]:
    """跨子进程固定输入；POSIX 仅提供前后检查。"""
    unique: dict[str, FileIdentity] = {}
    for identity in identities:
        key = os.path.normcase(str(identity.path))
        previous = unique.get(key)
        if previous is not None and (
            previous != identity
            or previous.allow_hardlinks != identity.allow_hardlinks
        ):
            raise ValueError(f"{name} 同路径出现不同身份或链接策略")
        unique[key] = identity
    handles: list[int] = []
    descriptors: list[int] = []
    ordered = tuple(unique.values())
    try:
        for identity in ordered:
            if os.name == "nt":
                handles.append(_windows_open_read_guard(identity, name))
            else:
                flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(identity.path, flags)
                metadata = os.fstat(descriptor)
                current = identity.path.lstat()
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or (not identity.allow_hardlinks and metadata.st_nlink != 1)
                    or (metadata.st_dev, metadata.st_ino)
                    != (current.st_dev, current.st_ino)
                ):
                    os.close(descriptor)
                    raise ValueError(f"{name} POSIX 句柄身份不符")
                descriptors.append(descriptor)
                _stable_identity(
                    identity.path,
                    name,
                    identity.sha256,
                    allow_hardlinks=identity.allow_hardlinks,
                )
        yield
        for identity in ordered:
            _stable_identity(
                identity.path,
                name,
                identity.sha256,
                allow_hardlinks=identity.allow_hardlinks,
            )
    finally:
        body_failed = sys.exc_info()[0] is not None
        cleanup_error: OSError | None = None
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError as exc:
                if cleanup_error is None and not body_failed:
                    cleanup_error = exc
        for handle in reversed(handles):
            try:
                _windows_close_handle(handle)
            except OSError as exc:
                if cleanup_error is None and not body_failed:
                    cleanup_error = exc
        if cleanup_error is not None:
            raise cleanup_error


def _run_pinned(
    command: Sequence[str],
    *,
    cwd: Path,
    directories: Sequence[tuple[Path, str]],
    guarded_files: Sequence[FileIdentity] = (),
    repository_checks: Sequence[
        tuple[Path, str, ExecutionIdentity, bool]
    ] = (),
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """在受管目录固定句柄覆盖整个子进程关键窗。"""
    with ExitStack() as stack:
        pinned = [
            stack.enter_context(_pin_directory_chain(path, name))
            for path, name in directories
        ]
        for repository, name, expected, detached in repository_checks:
            _repository_identity(
                repository,
                name,
                expected,
                require_detached=detached,
                reject_untracked_scopes=("src", "scripts"),
            )
        stack.enter_context(_guard_file_identities(
            guarded_files, "子进程输入",
        ))
        result = _run(command, cwd=cwd, env=env)
        for repository, name, expected, detached in repository_checks:
            _repository_identity(
                repository,
                name,
                expected,
                require_detached=detached,
                reject_untracked_scopes=("src", "scripts"),
            )
        for item, (_, name) in zip(pinned, directories, strict=True):
            _revalidate_directory_chain(item.path, item.identity, name)
        return result


@contextmanager
def _exclusive_stage_lock(
    path: Path, name: str, state: StageLockState,
) -> Iterator[None]:
    """以本地 OS 排他锁串行化同 prediction/stage 的复用到提交窗口。"""
    path = _absolute_canonical_path(path, name)
    with _pin_directory_chain(
        path.parent, f"{name} 父目录", transaction_state=state,
    ) as pinned:
        flags = os.O_RDWR | os.O_CREAT
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags, 0o600)
        locked = False
        try:
            metadata = os.fstat(descriptor)
            if metadata.st_size == 0:
                os.lseek(descriptor, 0, os.SEEK_SET)
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
                metadata = os.fstat(descriptor)
            current = path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or not stat.S_ISREG(current.st_mode)
                or stat.S_ISLNK(current.st_mode)
                or current.st_nlink != 1
                or (metadata.st_dev, metadata.st_ino)
                != (current.st_dev, current.st_ino)
            ):
                raise ValueError(f"{name} 不是唯一常规文件")
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(descriptor, msvcrt.LK_LOCK, 1)
            else:
                fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            locked = True
            _revalidate_directory_chain(path.parent, pinned.identity, name)
            current = path.lstat()
            metadata = os.fstat(descriptor)
            if (
                current.st_nlink != 1
                or metadata.st_nlink != 1
                or (current.st_dev, current.st_ino)
                != (metadata.st_dev, metadata.st_ino)
            ):
                raise ValueError(f"{name} 在加锁期间发生变化")
            yield
            _revalidate_directory_chain(path.parent, pinned.identity, name)
            current = path.lstat()
            metadata = os.fstat(descriptor)
            if (
                current.st_nlink != 1
                or metadata.st_nlink != 1
                or (current.st_dev, current.st_ino)
                != (metadata.st_dev, metadata.st_ino)
            ):
                raise ValueError(f"{name} 在持锁期间发生变化")
        finally:
            body_failed = sys.exc_info()[0] is not None
            cleanup_error: OSError | None = None
            if locked:
                try:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl = cast(
                            _FcntlModule, importlib.import_module("fcntl"),
                        )
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                except OSError as exc:
                    if not body_failed and not state.committed:
                        cleanup_error = exc
            try:
                os.close(descriptor)
            except OSError as exc:
                if (
                    cleanup_error is None
                    and not body_failed
                    and not state.committed
                ):
                    cleanup_error = exc
            if cleanup_error is not None:
                raise cleanup_error


def _decode_object(body: bytes, name: str) -> dict[str, object]:
    try:
        return _object(json.loads(body), name)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name} 不是有效 UTF-8 JSON") from exc


def _reject_nonfinite_json(value: object, name: str) -> None:
    """递归拒绝 Python JSON 解码器默认接受的 NaN/Infinity。"""
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{name} 含非有限 JSON 数值")
    if isinstance(value, dict):
        for key, item in value.items():
            _reject_nonfinite_json(item, f"{name}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_nonfinite_json(item, f"{name}[{index}]")


def _json_stdout(result: subprocess.CompletedProcess[str], name: str) -> dict[str, object]:
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"{name} 失败({result.returncode}): {detail}")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"{name} 没有 JSON 输出")
    try:
        return _object(json.loads(lines[-1]), name)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} 最后一行不是 JSON") from exc


class ChildOutputLimitError(RuntimeError):
    """子进程输出超过受管内存上限。"""


class ChildOutputReadError(RuntimeError):
    """子进程输出管道无法完整读取。"""


class ChildTimeoutError(subprocess.TimeoutExpired):
    """携带截断输出计数与散列的子进程超时。"""

    def __init__(
        self,
        command: Sequence[str],
        timeout_seconds: float,
        stdout: "_BoundedOutput",
        stderr: "_BoundedOutput",
    ) -> None:
        super().__init__(
            list(command), timeout_seconds,
            output=stdout.body.decode("utf-8", errors="replace"),
            stderr=stderr.body.decode("utf-8", errors="replace"),
        )
        self.stdout_identity = stdout
        self.stderr_identity = stderr

    def __str__(self) -> str:
        return (
            f"{super().__str__()};"
            f"stdout_bytes={self.stdout_identity.byte_count},"
            f"stdout_sha256={self.stdout_identity.sha256},"
            f"stderr_bytes={self.stderr_identity.byte_count},"
            f"stderr_sha256={self.stderr_identity.sha256}"
        )


@dataclass(frozen=True)
class _BoundedOutput:
    body: bytes
    byte_count: int
    sha256: str
    exceeded: bool
    reader_error: str | None = None


def _empty_bounded_output() -> _BoundedOutput:
    return _BoundedOutput(b"", 0, hashlib.sha256(b"").hexdigest(), False)


def _read_bounded_pipe(
    pipe: BinaryIO,
    result: list[_BoundedOutput],
    overflow: threading.Event,
    reader_failed: threading.Event,
) -> None:
    digest = hashlib.sha256()
    buffer = bytearray()
    count = 0
    reader_error: str | None = None
    try:
        while True:
            chunk = os.read(pipe.fileno(), 65_536)
            if not chunk:
                break
            count += len(chunk)
            digest.update(chunk)
            remaining = MAX_CHILD_OUTPUT_BYTES - len(buffer)
            if remaining > 0:
                buffer.extend(chunk[:remaining])
            if count > MAX_CHILD_OUTPUT_BYTES:
                overflow.set()
    except BaseException as exc:
        reader_error = f"{type(exc).__name__}: {exc}"
        reader_failed.set()
    finally:
        try:
            pipe.close()
        except BaseException as exc:
            close_error = f"{type(exc).__name__}: {exc}"
            reader_error = (
                close_error if reader_error is None
                else f"{reader_error}; close: {close_error}"
            )
            reader_failed.set()
        result.append(_BoundedOutput(
            bytes(buffer), count, digest.hexdigest(),
            count > MAX_CHILD_OUTPUT_BYTES,
            reader_error,
        ))


def _request_process_tree_termination(
    process: subprocess.Popen[bytes],
    kill_tree: Callable[[], None],
) -> str | None:
    """请求进程树退出；树级失败时仍尽力终止直接 child。"""
    try:
        kill_tree()
        return None
    except BaseException as exc:
        tree_error = f"{type(exc).__name__}: {exc}"
        try:
            if process.poll() is None:
                process.kill()
        except BaseException as direct_exc:
            return (
                f"tree={tree_error}; direct="
                f"{type(direct_exc).__name__}: {direct_exc}"
            )
        return f"tree={tree_error}"


def _wait_process_until(
    process: subprocess.Popen[bytes], deadline: float, name: str,
) -> None:
    if process.poll() is not None:
        return
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError(f"{name} 超出总硬时限")
    try:
        process.wait(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{name} 超出总硬时限") from exc


def _annotate_failure(
    primary: BaseException, secondary: Sequence[str],
) -> BaseException:
    for item in secondary:
        primary.add_note(f"secondary cleanup failure: {item}")
    return primary


def _close_process_pipes(process: subprocess.Popen[bytes]) -> list[str]:
    failures: list[str] = []
    for name, pipe in (("stdout", process.stdout), ("stderr", process.stderr)):
        if pipe is None or pipe.closed:
            continue
        try:
            pipe.close()
        except BaseException as exc:
            failures.append(f"{name} close: {type(exc).__name__}: {exc}")
    return failures


def _bounded_process_output(
    process: subprocess.Popen[bytes],
    *,
    command: Sequence[str],
    timeout_seconds: float,
    execution_deadline: float,
    hard_deadline: float,
    kill_tree: Callable[[], None],
) -> subprocess.CompletedProcess[str]:
    stdout_pipe = cast(BinaryIO, process.stdout)
    stderr_pipe = cast(BinaryIO, process.stderr)
    stdout_result: list[_BoundedOutput] = []
    stderr_result: list[_BoundedOutput] = []
    overflow = threading.Event()
    reader_failed = threading.Event()
    threads = (
        threading.Thread(
            target=_read_bounded_pipe,
            args=(stdout_pipe, stdout_result, overflow, reader_failed),
            daemon=True,
            name=f"frozen-shadow-pipe-{process.pid}-stdout",
        ),
        threading.Thread(
            target=_read_bounded_pipe,
            args=(stderr_pipe, stderr_result, overflow, reader_failed),
            daemon=True,
            name=f"frozen-shadow-pipe-{process.pid}-stderr",
        ),
    )
    started_threads: list[threading.Thread] = []
    thread_start_error: BaseException | None = None
    for thread in threads:
        try:
            thread.start()
            started_threads.append(thread)
        except BaseException as exc:
            thread_start_error = exc
            break
    trigger: str | None = None
    termination_error: str | None = None
    monitor_error: BaseException | None = None
    if thread_start_error is not None:
        termination_error = _request_process_tree_termination(
            process, kill_tree,
        )
    else:
        try:
            while process.poll() is None:
                if reader_failed.is_set():
                    trigger = "reader"
                    termination_error = _request_process_tree_termination(
                        process, kill_tree,
                    )
                    break
                if overflow.is_set():
                    trigger = "output-limit"
                    termination_error = _request_process_tree_termination(
                        process, kill_tree,
                    )
                    break
                remaining = execution_deadline - time.monotonic()
                if remaining <= 0:
                    trigger = "timeout"
                    termination_error = _request_process_tree_termination(
                        process, kill_tree,
                    )
                    break
                try:
                    process.wait(timeout=min(0.05, remaining))
                except subprocess.TimeoutExpired:
                    pass
        except BaseException as exc:
            monitor_error = exc
            if process.poll() is None:
                termination_error = _request_process_tree_termination(
                    process, kill_tree,
                )

    cleanup_deadline = min(
        hard_deadline, time.monotonic() + CHILD_CLEANUP_GRACE_SECONDS,
    )
    secondary: list[str] = []
    try:
        _wait_process_until(process, cleanup_deadline, "child 进程树清理")
    except BaseException as exc:
        secondary.append(f"process wait: {type(exc).__name__}: {exc}")
    reader_soft_deadline = min(
        cleanup_deadline,
        time.monotonic() + CHILD_CLEANUP_GRACE_SECONDS / 2,
    )
    for thread in started_threads:
        remaining = reader_soft_deadline - time.monotonic()
        if remaining > 0:
            thread.join(timeout=remaining)
    forced_pipe_close = any(
        thread.is_alive() for thread in started_threads
    ) or len(started_threads) != len(threads)
    if forced_pipe_close:
        secondary.append("child 输出读取线程需要强制关闭")
        secondary.extend(_close_process_pipes(process))
        for thread in started_threads:
            remaining = cleanup_deadline - time.monotonic()
            if remaining > 0:
                thread.join(timeout=remaining)
    if any(thread.is_alive() for thread in started_threads):
        secondary.append("child 输出读取线程超出总硬时限")
    if termination_error is not None:
        secondary.append(f"process-tree termination: {termination_error}")

    stdout = stdout_result[0] if len(stdout_result) == 1 else _empty_bounded_output()
    stderr = stderr_result[0] if len(stderr_result) == 1 else _empty_bounded_output()
    primary: BaseException | None = thread_start_error
    if monitor_error is not None:
        primary = monitor_error
    if len(stdout_result) != 1 or len(stderr_result) != 1:
        secondary.append("child 输出读取线程未产生唯一结果")
    reader_errors = [
        error for error in (stdout.reader_error, stderr.reader_error)
        if error is not None
    ]
    if primary is None and (
        trigger == "reader" or (trigger is None and reader_errors)
    ):
        primary = ChildOutputReadError(
            "child 输出读取失败: " + "; ".join(reader_errors)
        )
    if primary is None and trigger == "timeout":
        primary = ChildTimeoutError(
            command, timeout_seconds, stdout, stderr,
        )
    if primary is None and (
        trigger == "output-limit" or stdout.exceeded or stderr.exceeded
    ):
        primary = ChildOutputLimitError(
            "child 输出超限: "
            f"stdout_bytes={stdout.byte_count},stdout_sha256={stdout.sha256},"
            f"stderr_bytes={stderr.byte_count},stderr_sha256={stderr.sha256}"
        )
    if primary is None and secondary:
        primary = RuntimeError("child 清理合同失败")
    if primary is not None:
        raise _annotate_failure(primary, secondary).with_traceback(
            primary.__traceback__,
        )
    stdout_text = stdout.body.decode("utf-8")
    stderr_text = stderr.body.decode("utf-8")
    return subprocess.CompletedProcess(
        list(command), process.returncode, stdout_text, stderr_text,
    )


def _windows_kill_on_close_job() -> int:
    kernel = _windows_kernel32()
    create_job = kernel.CreateJobObjectW
    create_job.argtypes = (ctypes.c_void_p, ctypes.c_wchar_p)
    create_job.restype = ctypes.c_void_p
    raw = create_job(None, None)
    if raw is None:
        raise OSError(ctypes.get_last_error(), "无法创建 child Job Object")
    handle = int(raw)
    limits = _WindowsJobExtendedLimit()
    limits.basic.limit_flags = 0x00002000
    set_information = kernel.SetInformationJobObject
    set_information.argtypes = (
        ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32,
    )
    set_information.restype = ctypes.c_int
    if not set_information(
        ctypes.c_void_p(handle),
        9,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    ):
        error = ctypes.get_last_error()
        kernel.CloseHandle(ctypes.c_void_p(handle))
        raise OSError(error, "无法设置 child Job Object")
    return handle


def _close_windows_handle(handle: int) -> None:
    kernel = _windows_kernel32()
    if not kernel.CloseHandle(ctypes.c_void_p(handle)):
        raise OSError(ctypes.get_last_error(), "无法关闭 child Job Object")


@dataclass
class _OutputAccumulator:
    body: bytearray = field(default_factory=bytearray)
    byte_count: int = 0
    digest: Any = field(default_factory=hashlib.sha256)

    def add(self, chunk: bytes) -> None:
        self.byte_count += len(chunk)
        self.digest.update(chunk)
        remaining = MAX_CHILD_OUTPUT_BYTES - len(self.body)
        if remaining > 0:
            self.body.extend(chunk[:remaining])

    def identity(self, reader_error: str | None = None) -> _BoundedOutput:
        return _BoundedOutput(
            bytes(self.body),
            self.byte_count,
            self.digest.hexdigest(),
            self.byte_count > MAX_CHILD_OUTPUT_BYTES,
            reader_error,
        )


def _windows_drain_pipe(
    pipe: BinaryIO, accumulator: _OutputAccumulator,
) -> bool:
    """无阻塞地吸收当前可用字节；返回 writer 是否已全部关闭。"""
    import msvcrt

    kernel = _windows_kernel32()
    peek = kernel.PeekNamedPipe
    peek.argtypes = (
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_void_p,
    )
    peek.restype = ctypes.c_int
    handle = int(msvcrt.get_osfhandle(pipe.fileno()))
    while True:
        available = ctypes.c_uint32()
        if not peek(
            ctypes.c_void_p(handle), None, 0, None,
            ctypes.byref(available), None,
        ):
            error = ctypes.get_last_error()
            if error in {109, 232}:  # ERROR_BROKEN_PIPE / ERROR_NO_DATA
                return True
            raise OSError(error, "无法读取 child 输出管道状态")
        if available.value == 0:
            return False
        chunk = os.read(pipe.fileno(), min(int(available.value), 65_536))
        if not chunk:
            return True
        accumulator.add(chunk)


def _windows_job_active_processes(handle: int) -> int:
    kernel = _windows_kernel32()
    query = kernel.QueryInformationJobObject
    query.argtypes = (
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_void_p,
    )
    query.restype = ctypes.c_int
    accounting = _WindowsJobBasicAccounting()
    if not query(
        ctypes.c_void_p(handle), 1, ctypes.byref(accounting),
        ctypes.sizeof(accounting), None,
    ):
        raise OSError(ctypes.get_last_error(), "无法查询 child Job Object")
    return int(accounting.active_processes)


def _terminate_windows_job(handle: int) -> None:
    kernel = _windows_kernel32()
    terminate = kernel.TerminateJobObject
    terminate.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    terminate.restype = ctypes.c_int
    if not terminate(ctypes.c_void_p(handle), 1):
        raise OSError(ctypes.get_last_error(), "无法终止 child Job Object")


_WINDOWS_TARGET_LAUNCHER = r"""
import base64
import json
import os
import subprocess
import sys
import time

payload = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
delay = float(payload["setup_delay_seconds"])
if delay:
    time.sleep(delay)
child = subprocess.Popen(
    payload["command"], cwd=payload["cwd"], env=dict(os.environ),
    close_fds=False,
)
raise SystemExit(child.wait())
"""


def _windows_launcher_command(
    command: Sequence[str], cwd: Path, setup_delay_seconds: float,
) -> tuple[str, ...]:
    payload = json.dumps({
        "command": list(command),
        "cwd": str(cwd),
        "setup_delay_seconds": setup_delay_seconds,
    }, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    encoded = base64.b64encode(payload).decode("ascii")
    return (
        sys.executable,
        "-I",
        "-S",
        "-B",
        "-X",
        "utf8",
        "-c",
        _WINDOWS_TARGET_LAUNCHER,
        encoded,
    )


def _run_windows_child(
    command: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout_seconds: float,
    execution_deadline: float,
    hard_deadline: float,
    setup_delay_seconds: float = 0.0,
) -> subprocess.CompletedProcess[str]:
    """用受 Job 约束的 launcher 外部监视业务 CreateProcess 与整棵树。"""
    job = _windows_kill_on_close_job()
    process: subprocess.Popen[bytes] | None = None
    primary: BaseException | None = None
    assigned = False
    resumed = False
    trigger: str | None = None
    stdout = _OutputAccumulator()
    stderr = _OutputAccumulator()
    returncode: int | None = None
    try:
        if time.monotonic() >= execution_deadline:
            trigger = "timeout"
            raise ChildTimeoutError(
                command, timeout_seconds, stdout.identity(), stderr.identity(),
            )
        launcher = _windows_launcher_command(
            command, cwd, setup_delay_seconds,
        )
        process = subprocess.Popen(
            list(launcher),
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=0x00000004,
        )
        kernel = _windows_kernel32()
        assign = kernel.AssignProcessToJobObject
        assign.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
        assign.restype = ctypes.c_int
        process_handle = int(cast(Any, process)._handle)
        if not assign(
            ctypes.c_void_p(job), ctypes.c_void_p(process_handle),
        ):
            assign_error = ctypes.get_last_error()
            raise OSError(
                assign_error, "无法把 child 纳入 Job Object",
            )
        assigned = True
        loader = cast(Any, getattr(ctypes, "WinDLL"))
        ntdll = loader("ntdll", use_last_error=True)
        resume = ntdll.NtResumeProcess
        resume.argtypes = (ctypes.c_void_p,)
        resume.restype = ctypes.c_long

        if time.monotonic() < execution_deadline:
            resume_status = int(resume(ctypes.c_void_p(process_handle)))
            if resume_status != 0:
                raise OSError(resume_status, "无法恢复已纳入 Job 的 child")
            resumed = True
        else:
            trigger = "timeout"

        while trigger is None:
            # 截止优先于 poll：边沿退出不能在 deadline 后被误判为成功。
            now = time.monotonic()
            if now >= execution_deadline:
                trigger = "timeout"
                break
            _windows_drain_pipe(cast(BinaryIO, process.stdout), stdout)
            _windows_drain_pipe(cast(BinaryIO, process.stderr), stderr)
            if (
                stdout.byte_count > MAX_CHILD_OUTPUT_BYTES
                or stderr.byte_count > MAX_CHILD_OUTPUT_BYTES
            ):
                trigger = "output-limit"
                break
            returncode = process.poll()
            if returncode is not None:
                break
            time.sleep(min(0.005, max(0.0, execution_deadline - now)))
    except BaseException as exc:
        primary = exc

    secondary: list[str] = []
    # 无论成功或失败，都先显式终止 Job、验证零 active，再关闭句柄。
    try:
        _terminate_windows_job(job)
    except BaseException as exc:
        secondary.append(f"Job terminate: {type(exc).__name__}: {exc}")
    if process is not None and not assigned and process.poll() is None:
        try:
            process.kill()
        except BaseException as exc:
            secondary.append(f"direct child kill: {type(exc).__name__}: {exc}")
    cleanup_deadline = min(
        hard_deadline, time.monotonic() + CHILD_CLEANUP_GRACE_SECONDS,
    )
    if process is not None:
        try:
            _wait_process_until(process, cleanup_deadline, "child Job 终止清理")
        except BaseException as exc:
            secondary.append(f"process wait: {type(exc).__name__}: {exc}")
        try:
            _windows_drain_pipe(cast(BinaryIO, process.stdout), stdout)
            _windows_drain_pipe(cast(BinaryIO, process.stderr), stderr)
        except BaseException as exc:
            if primary is None:
                primary = ChildOutputReadError(
                    f"child 输出读取失败: {type(exc).__name__}: {exc}",
                )
            else:
                secondary.append(
                    f"pipe drain: {type(exc).__name__}: {exc}",
                )
    active: int | None = None
    while time.monotonic() < cleanup_deadline:
        try:
            active = _windows_job_active_processes(job)
        except BaseException as exc:
            secondary.append(f"Job query: {type(exc).__name__}: {exc}")
            break
        if active == 0:
            break
        time.sleep(min(0.005, max(0.0, cleanup_deadline - time.monotonic())))
    if active != 0:
        secondary.append(f"Job active process count is not zero: {active!r}")
    try:
        _close_windows_handle(job)
    except BaseException as exc:
        secondary.append(f"Job CloseHandle: {type(exc).__name__}: {exc}")
    if process is not None:
        secondary.extend(_close_process_pipes(process))
    if time.monotonic() > hard_deadline:
        secondary.append("Windows child 总清理预算已超时")
    stdout_identity = stdout.identity()
    stderr_identity = stderr.identity()
    if primary is None and trigger == "timeout":
        primary = ChildTimeoutError(
            command, timeout_seconds, stdout_identity, stderr_identity,
        )
    if primary is None and (
        trigger == "output-limit"
        or stdout_identity.exceeded
        or stderr_identity.exceeded
    ):
        primary = ChildOutputLimitError(
            "child 输出超限: "
            f"stdout_bytes={stdout_identity.byte_count},"
            f"stdout_sha256={stdout_identity.sha256},"
            f"stderr_bytes={stderr_identity.byte_count},"
            f"stderr_sha256={stderr_identity.sha256}"
        )
    if primary is not None:
        raise _annotate_failure(primary, secondary).with_traceback(
            primary.__traceback__,
        )
    if secondary:
        cleanup = RuntimeError("Windows child 清理合同失败")
        raise _annotate_failure(cleanup, secondary)
    if process is None or returncode is None or not resumed:
        raise RuntimeError("Windows child 未返回执行结果")
    return subprocess.CompletedProcess(
        list(command),
        returncode,
        stdout_identity.body.decode("utf-8"),
        stderr_identity.body.decode("utf-8"),
    )


def _run_posix_child(
    command: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout_seconds: float,
    execution_deadline: float,
    hard_deadline: float,
) -> subprocess.CompletedProcess[str]:
    kill_process_group = cast(Callable[[int, int], None], getattr(os, "killpg"))
    sigkill = cast(int, getattr(signal, "SIGKILL"))
    if time.monotonic() >= execution_deadline:
        empty = _empty_bounded_output()
        raise ChildTimeoutError(command, timeout_seconds, empty, empty)
    process = subprocess.Popen(
        list(command), cwd=cwd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True,
    )
    return _bounded_process_output(
        process,
        command=command,
        timeout_seconds=timeout_seconds,
        execution_deadline=execution_deadline,
        hard_deadline=hard_deadline,
        kill_tree=lambda: kill_process_group(process.pid, sigkill),
    )


def _run(
    command: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    timeout_seconds: float = DEFAULT_CHILD_TIMEOUT_SECONDS,
    _windows_setup_delay_seconds: float = 0.0,
) -> subprocess.CompletedProcess[str]:
    """从入口计时；Windows 业务 native setup 位于受监视 launcher 内。"""
    child_env = dict(os.environ if env is None else env)
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("child timeout 必须为有限正数")
    started_at = time.monotonic()
    execution_deadline = started_at + timeout_seconds
    hard_deadline = execution_deadline + CHILD_CLEANUP_GRACE_SECONDS
    runner = _run_windows_child if os.name == "nt" else _run_posix_child
    arguments: dict[str, object] = {
        "cwd": cwd,
        "env": child_env,
        "timeout_seconds": timeout_seconds,
        "execution_deadline": execution_deadline,
        "hard_deadline": hard_deadline,
    }
    if os.name == "nt":
        arguments["setup_delay_seconds"] = _windows_setup_delay_seconds
    return runner(
        command,
        **cast(Any, arguments),
    )


_IMPORT_CLOSURE_RUNPY = r"""
import importlib.abc
import importlib.machinery
import importlib.util
import json
import os
import sys
import tokenize

manifest_path = sys.argv.pop(1)
entry = sys.argv.pop(1)
with open(manifest_path, "r", encoding="utf-8") as stream:
    manifest = json.load(stream)
modules = manifest["modules"]
controlled = frozenset(manifest["controlled_top_levels"])
stdlib = frozenset(sys.stdlib_module_names)
base_paths = tuple(item for item in sys.path if item)

class ClosureFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        del target
        record = modules.get(fullname)
        if record is not None:
            kind = record["kind"]
            if kind == "namespace":
                spec = importlib.machinery.ModuleSpec(
                    fullname, loader=None, is_package=True,
                )
                spec.submodule_search_locations = list(record["paths"])
                return spec
            filename = record["path"]
            if kind == "source":
                loader = importlib.machinery.SourceFileLoader(fullname, filename)
            elif kind == "bytecode":
                loader = importlib.machinery.SourcelessFileLoader(fullname, filename)
            elif kind == "extension":
                loader = importlib.machinery.ExtensionFileLoader(fullname, filename)
            else:
                raise ImportError("unknown governed loader kind")
            locations = [os.path.dirname(filename)] if record["is_package"] else None
            return importlib.util.spec_from_file_location(
                fullname, filename, loader=loader,
                submodule_search_locations=locations,
            )
        top = fullname.partition(".")[0]
        if top in controlled or top not in stdlib:
            raise ModuleNotFoundError(
                "module is outside the governed import closure: " + fullname,
            )
        search = base_paths if path is None else path
        spec = importlib.machinery.PathFinder.find_spec(fullname, search)
        if spec is None:
            raise ModuleNotFoundError("stdlib module is unavailable: " + fullname)
        return spec

path_finder_index = sys.meta_path.index(importlib.machinery.PathFinder)
sys.meta_path.insert(path_finder_index, ClosureFinder())
sys.path[:] = [item for item in sys.path if item]
sys.path.append(manifest["site_root"])
sys.argv[0] = entry
with tokenize.open(entry) as stream:
    source = stream.read()
code = compile(source, entry, "exec")
namespace = {
    "__name__": "__main__",
    "__file__": entry,
    "__package__": None,
    "__spec__": None,
    "__cached__": None,
}
exec(code, namespace, namespace)
"""


def _closure_for_source(
    environment: EnvironmentIdentity, source_root: Path,
) -> ImportClosureIdentity:
    source = _absolute_canonical_path(source_root, "Python 受验源码根")
    for closure in _import_closures(environment):
        if _same_path(source, closure.source_root):
            return closure
    raise ValueError("Python 源码根未绑定到 import closure")


def _environment_guarded_files(
    environment: EnvironmentIdentity,
) -> tuple[FileIdentity, ...]:
    closure_files: list[FileIdentity] = []
    for closure in _import_closures(environment):
        closure_files.extend((closure.manifest, *closure.files))
    return (
        environment.pycache_sentinel,
        environment.empty_child_env,
        *environment.files,
        *closure_files,
    )


def _isolated_python_command(
    environment: EnvironmentIdentity,
    source_root: Path,
    script: Path,
    arguments: Sequence[str],
) -> tuple[str, ...]:
    """只从显式模块映射和标准库执行内容寻址入口。"""
    closure = _closure_for_source(environment, source_root)
    entry = _absolute_canonical_path(script, "Python 受验入口")
    if not entry.is_relative_to(closure.repository):
        raise ValueError("Python 入口越出 import closure 仓")
    snapshot_entry = _absolute_canonical_path(
        closure.root / "repo" / entry.relative_to(closure.repository),
        "Python closure 入口",
    )
    if not any(_same_path(item.path, snapshot_entry) for item in closure.files):
        raise ValueError("Python 入口不在 import closure 文件集")
    return (
        str(environment.python.path),
        "-I",
        "-S",
        "-B",
        "-X",
        "utf8",
        "-X",
        f"pycache_prefix={environment.pycache_sentinel.path}",
        "-c",
        _IMPORT_CLOSURE_RUNPY,
        str(closure.manifest.path),
        str(snapshot_entry),
        *arguments,
    )


def _append_record(path: Path, record: Mapping[str, object]) -> None:
    body = (json.dumps(
        dict(record), ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ) + "\n").encode("utf-8")
    path = _absolute_canonical_path(path, "任务日志")
    log_state = StageLockState()
    with _pin_directory_chain(
        path.parent, "任务日志父目录", transaction_state=log_state,
    ) as pinned:
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags, 0o600)
        try:
            opened = os.fstat(descriptor)
            current = path.lstat()
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or not stat.S_ISREG(current.st_mode)
                or stat.S_ISLNK(current.st_mode)
                or current.st_nlink != 1
                or _stat_cross_fingerprint(opened)
                != _stat_cross_fingerprint(current)
            ):
                raise ValueError("任务日志不是唯一常规文件")
            written = os.write(descriptor, body)
            if written != len(body):
                raise OSError("任务日志发生短写")
            os.fsync(descriptor)
            _revalidate_directory_chain(
                path.parent, pinned.identity, "任务日志父目录",
            )
            final = os.fstat(descriptor)
            final_path = path.lstat()
            if (
                final.st_nlink != 1
                or final_path.st_nlink != 1
                or _stat_cross_fingerprint(final)
                != _stat_cross_fingerprint(final_path)
            ):
                raise ValueError("任务日志在追加期间发生变化")
            log_state.committed = True
        finally:
            body_failed = sys.exc_info()[0] is not None
            try:
                os.close(descriptor)
            except OSError:
                if not body_failed and not log_state.committed:
                    raise


def _sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _sha256(path: Path) -> str:
    """兼容调用方的稳定文件散列。"""
    return _sha256_bytes(_stable_file_bytes(path, "散列文件"))


def _stat_fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev, value.st_ino, value.st_size,
        value.st_mtime_ns, value.st_ctime_ns,
    )


def _stat_cross_fingerprint(value: os.stat_result) -> tuple[int, int, int, int]:
    """返回 Windows 路径 stat 与句柄 fstat 可一致比较的字段。"""
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _stable_file_bytes(
    path: Path,
    name: str,
    *,
    allow_hardlinks: bool = False,
) -> bytes:
    """从同一常规文件句柄读取两次，并证明路径仍指向该版本。"""
    lexical = _absolute_canonical_path(path, name)
    try:
        before_path = lexical.lstat()
        reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if (
            stat.S_ISLNK(before_path.st_mode)
            or not stat.S_ISREG(before_path.st_mode)
            or (not allow_hardlinks and before_path.st_nlink != 1)
            or int(getattr(before_path, "st_file_attributes", 0)) & reparse_flag
            or int(getattr(before_path, "st_reparse_tag", 0))
        ):
            raise ValueError(f"{name} 不是常规文件")
        with lexical.open("rb") as stream:
            before = os.fstat(stream.fileno())
            first = stream.read()
            middle = os.fstat(stream.fileno())
            stream.seek(0)
            second = stream.read()
            after = os.fstat(stream.fileno())
        after_path = lexical.lstat()
    except FileNotFoundError as exc:
        raise ValueError(f"{name} 不存在: {path}") from exc
    except OSError as exc:
        raise ValueError(f"{name} 无法稳定读取: {path}: {exc}") from exc
    path_stable = _stat_fingerprint(before_path) == _stat_fingerprint(after_path)
    handle_stable = len({
        _stat_fingerprint(before), _stat_fingerprint(middle),
        _stat_fingerprint(after),
    }) == 1
    same_file = (
        _stat_cross_fingerprint(before_path)
        == _stat_cross_fingerprint(before)
        == _stat_cross_fingerprint(after_path)
    )
    if (
        not path_stable or not handle_stable or not same_file
        or first != second or len(first) != after.st_size
        or (
            not allow_hardlinks
            and any(
                item.st_nlink != 1
                for item in (before_path, before, middle, after, after_path)
            )
        )
    ):
        raise ValueError(f"{name} 在读取期间发生变化: {lexical}")
    return first


def _stable_identity(
    path: Path,
    name: str,
    expected_sha256: str | None = None,
    *,
    allow_hardlinks: bool = False,
) -> FileIdentity:
    digest = _sha256_bytes(_stable_file_bytes(
        path, name, allow_hardlinks=allow_hardlinks,
    ))
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValueError(f"{name} SHA-256 已变化")
    return FileIdentity(
        _absolute_canonical_path(path, name), digest, allow_hardlinks,
    )


def _revalidate_identity(identity: FileIdentity, name: str) -> None:
    """按身份捕获时的链接策略复核同一文件。"""
    _stable_identity(
        identity.path,
        name,
        identity.sha256,
        allow_hardlinks=identity.allow_hardlinks,
    )


def _entry_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _preflight_receipt_schema(execution_repository: Path) -> None:
    """在任何 child 或受管写入前拒绝旧版回执。"""
    execution = _execution_root(execution_repository)
    for relative, name in (
        (f"{SHADOW_ROOT}/receipts", "shadow 回执目录"),
        (f"{PAPER_ROOT}/receipts", "paper 回执目录"),
    ):
        root = _absolute_canonical_path(execution / relative, name)
        if not _entry_exists(root):
            continue
        if not root.is_relative_to(execution):
            raise ValueError(f"{name} 越出执行仓")
        with _pin_directory_chain(root, name) as pinned:
            entries = sorted(root.iterdir(), key=lambda item: item.name)
            for entry in entries:
                if (
                    entry.suffix != ".json"
                    or _FROZEN_PREDICTION_ID.fullmatch(entry.stem) is None
                ):
                    raise ValueError(f"{name} 含非规范回执文件")
                payload = _decode_object(
                    _stable_file_bytes(entry, "启动前回执"),
                    "启动前回执",
                )
                if (
                    type(payload.get("schema_version")) is not int
                    or payload.get("schema_version") != RECEIPT_SCHEMA_VERSION
                ):
                    raise ValueError(
                        "旧版回执不迁移、不复用；必须隔离后重新生成",
                    )
            _revalidate_directory_chain(root, pinned.identity, name)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_publish_new(
    path: Path, body: bytes, *, allow_existing_identical: bool,
) -> None:
    """以同目录硬链接提交完整临时文件，永不覆盖已存在路径。"""
    path = _absolute_canonical_path(path, "待提交制品")
    if not path.parent.is_dir():
        raise ValueError(f"待提交制品父目录不存在: {path.parent}")
    temporary = _managed_file_path(
        path.parent,
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp",
        "制品临时文件",
    )
    publish_state = StageLockState()
    with _pin_directory_chain(
        path.parent,
        "制品提交父目录",
        transaction_state=publish_state,
    ) as pinned:
        if _entry_exists(path):
            if (
                allow_existing_identical
                and _stable_file_bytes(path, "已存在制品") == body
            ):
                return
            raise ValueError(f"拒绝覆盖已存在制品: {path}")
        try:
            with temporary.open("xb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError as exc:
                if (
                    allow_existing_identical
                    and _stable_file_bytes(path, "并发提交制品") == body
                ):
                    publish_state.committed = True
                    return
                raise ValueError(f"制品提交时目标已存在: {path}") from exc
            _fsync_directory(path.parent)
            _revalidate_directory_chain(
                path.parent, pinned.identity, "制品提交父目录",
            )
            publish_state.committed = True
        finally:
            body_failed = sys.exc_info()[0] is not None
            try:
                temporary.unlink(missing_ok=True)
            except OSError as first_cleanup_error:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass
                except OSError:
                    # 临时链接残留时不能成功
                    if not body_failed:
                        raise first_cleanup_error
    if _stable_file_bytes(path, "已提交制品") != body:
        raise ValueError(f"制品提交后字节不符: {path}")


def _identity_payload(identity: FileIdentity) -> dict[str, object]:
    return {"path": str(identity.path), "sha256": identity.sha256}


def _resolve_child_path(value: object, cwd: Path, name: str) -> Path:
    path = Path(_text(value, name))
    if not path.is_absolute():
        path = cwd / path
    path = _absolute_canonical_path(path, name)
    if not path.is_relative_to(cwd):
        raise ValueError(f"{name} 越出受管根")
    return path


def _capture_prediction(
    execution: Path,
    runtime: Path,
    prediction: Mapping[str, object],
    plan_id: str,
) -> tuple[FileIdentity, FileIdentity, dict[str, object]]:
    """按 predictor 声明散列捕获官方预测，并发布不可变执行快照。"""
    prediction = _exact_object(
        prediction, _PREDICTION_STDOUT_KEYS, "frozen prediction stdout",
    )
    _reject_nonfinite_json(prediction, "frozen prediction stdout")
    prediction_id = _frozen_id(
        prediction.get("prediction_id"), _FROZEN_PREDICTION_ID, "prediction_id",
    )
    claimed_sha256 = _digest(
        prediction.get("prediction_sha256"), "prediction_sha256",
    )
    origin_path = _resolve_child_path(
        prediction.get("prediction_path"), runtime, "prediction_path",
    )
    source_root = _managed_directory(
        execution, SOURCE_DIRECTORY, "冻结来源快照目录",
    )
    with ExitStack() as stack:
        origin_parent = stack.enter_context(_pin_directory_chain(
            origin_path.parent, "冻结来源原件目录",
        ))
        source_parent = stack.enter_context(_pin_directory_chain(
            source_root, "冻结来源快照目录",
        ))
        origin_body = _stable_file_bytes(origin_path, "官方冻结预测")
        origin_sha256 = _sha256_bytes(origin_body)
        if origin_sha256 != claimed_sha256:
            raise ValueError("官方冻结预测与 predictor 声明 SHA-256 不符")
        official = _decode_object(origin_body, "官方冻结预测")
        _reject_nonfinite_json(official, "官方冻结预测")
        checks = {
            "prediction_id": prediction_id,
            "plan_id": plan_id,
            "decision_time": prediction.get("decision_time"),
            "aggregate_target": prediction.get("aggregate_target"),
        }
        if any(official.get(key) != value for key, value in checks.items()):
            raise ValueError("官方冻结预测与 predictor stdout 身份字段不符")
        _finite_number(official.get("aggregate_target"), "官方冻结预测 aggregate_target")
        snapshot_path = _managed_file_path(
            source_root, f"source-{origin_sha256}.json", "执行来源快照",
        )
        _atomic_publish_new(
            snapshot_path, origin_body, allow_existing_identical=True,
        )
        snapshot = _stable_identity(
            snapshot_path, "执行来源快照", origin_sha256,
        )
        origin = FileIdentity(origin_path, origin_sha256)
        _assert_distinct_files(origin.path, snapshot.path, "来源原件与执行快照")
        _revalidate_directory_chain(
            origin_path.parent, origin_parent.identity, "冻结来源原件目录",
        )
        _revalidate_directory_chain(
            source_root, source_parent.identity, "冻结来源快照目录",
        )
        return origin, snapshot, official


def _capture_config(
    execution: Path,
    config_path: Path,
) -> tuple[FileIdentity, FileIdentity, dict[str, object]]:
    """将可变配置原件捕获为内容寻址快照。"""
    root = _managed_directory(
        execution, CONFIG_SOURCE_DIRECTORY, "执行配置快照目录",
    )
    with ExitStack() as stack:
        origin_parent = stack.enter_context(_pin_directory_chain(
            config_path.parent, "执行配置原件目录",
        ))
        snapshot_parent = stack.enter_context(_pin_directory_chain(
            root, "执行配置快照目录",
        ))
        body = _stable_file_bytes(config_path, "执行配置原件")
        digest = _sha256_bytes(body)
        payload = _decode_object(body, "执行配置")
        _reject_nonfinite_json(payload, "执行配置")
        snapshot_path = _managed_file_path(
            root, f"config-{digest}.json", "执行配置快照",
        )
        lock_root = _managed_directory(
            execution, f"{SHADOW_ROOT}/locks", "执行配置快照锁目录",
        )
        lock_path = _managed_file_path(
            lock_root, f"config-{digest}.lock", "执行配置快照锁",
        )
        lock_state = StageLockState()
        with _exclusive_stage_lock(
            lock_path, "执行配置快照锁", lock_state,
        ):
            _atomic_publish_new(
                snapshot_path, body, allow_existing_identical=True,
            )
            lock_state.committed = True
        origin = _stable_identity(
            config_path, "执行配置原件", digest,
        )
        snapshot = _stable_identity(
            snapshot_path, "执行配置快照", digest,
        )
        _assert_distinct_files(origin.path, snapshot.path, "配置原件与快照")
        _revalidate_directory_chain(
            config_path.parent,
            origin_parent.identity,
            "执行配置原件目录",
        )
        _revalidate_directory_chain(
            root, snapshot_parent.identity, "执行配置快照目录",
        )
        return origin, snapshot, payload


def _positive_integer(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _nonnegative_integer(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} 必须为非负整数")
    return value


def _interval_duration(text: str) -> timedelta:
    units = {
        "min": 60,
        "hour": 3600,
        "day": 86_400,
        "week": 604_800,
    }
    for unit, seconds in units.items():
        if text.endswith(unit):
            digits = text[:-len(unit)]
            if digits.isdigit() and int(digits) > 0:
                return timedelta(seconds=int(digits) * seconds)
    raise ValueError("执行配置 bar_interval 非法")


def _paper_config_contract(
    payload: object,
    *,
    market_id: str,
    symbol: str,
) -> dict[str, object]:
    config = _exact_object(payload, _PAPER_CONFIG_KEYS, "paper 执行配置")
    if (
        config.get("schema_version") != 1
        or config.get("market_id") != market_id
        or config.get("symbol") != symbol
        or config.get("ledger_directory") != "execution/paper"
    ):
        raise ValueError("paper 执行配置身份或受管路径不符")
    _interval_duration(_text(config.get("bar_interval"), "paper bar_interval"))
    _decimal(config.get("risk_budget_jpy"), "paper risk_budget_jpy", positive=True)
    no_trade = _decimal(
        config.get("no_trade_band"), "paper no_trade_band", nonnegative=True,
    )
    if no_trade >= 1:
        raise ValueError("paper no_trade_band 必须小于 1")
    _decimal(
        config.get("taker_fee_fallback_bps"),
        "paper taker_fee_fallback_bps",
        nonnegative=True,
    )
    _nonnegative_integer(
        config.get("taker_fee_cache_seconds"),
        "paper taker_fee_cache_seconds",
    )
    overlay = _exact_object(
        config.get("overlay"), _PAPER_CONFIG_OVERLAY_KEYS, "paper overlay 配置",
    )
    limit = _decimal(overlay.get("limit"), "paper overlay.limit", nonnegative=True)
    if limit > 1:
        raise ValueError("paper overlay.limit 必须不超过 1")
    _decimal(
        overlay.get("maximum_spread_bps"),
        "paper overlay.maximum_spread_bps",
        nonnegative=True,
    )
    _decimal(
        overlay.get("minimum_top5_depth_base"),
        "paper overlay.minimum_top5_depth_base",
        nonnegative=True,
    )
    _nonnegative_integer(
        overlay.get("maximum_anchor_age_seconds"),
        "paper overlay.maximum_anchor_age_seconds",
    )
    return config


def _derive_correlation_id(prediction_id: str) -> str:
    digest = hashlib.sha256(
        f"guvolu-prediction:{prediction_id}".encode("utf-8"),
    ).hexdigest()
    return f"co{digest[:16]}"


def _target_expectation(
    official: Mapping[str, object],
    source_snapshot: FileIdentity,
    config: Mapping[str, object],
    *,
    plan_id: str,
    market_id: str,
    symbol: str,
    mode: str,
    budget_jpy: str | None,
) -> TargetExpectation:
    prediction_id = _frozen_id(
        official.get("prediction_id"), _FROZEN_PREDICTION_ID, "prediction_id",
    )
    if official.get("plan_id") != plan_id:
        raise ValueError("预测 plan_id 不符")
    decision_time = _timestamp(official.get("decision_time"), "decision_time")
    configured_interval = _text(
        config.get("bar_interval"), "paper bar_interval",
    )
    bar_interval = (
        configured_interval
        if official.get("bar_interval") is None
        else _text(official.get("bar_interval"), "prediction bar_interval")
    )
    _interval_duration(bar_interval)
    given_valid_until = official.get("valid_until")
    if given_valid_until is None:
        valid_until = decision_time + _interval_duration(bar_interval)
        valid_until_source = "derived"
    else:
        valid_until = _timestamp(given_valid_until, "prediction valid_until")
        valid_until_source = "prediction"
    if valid_until <= decision_time:
        raise ValueError("预测有效期不晚于决策时间")
    inherited_correlation = official.get("correlation_id")
    if inherited_correlation is None:
        correlation_id = _derive_correlation_id(prediction_id)
        correlation_source = "adapter"
    else:
        correlation_id = _text(inherited_correlation, "prediction correlation_id")
        correlation_source = "prediction"
    aggregate = _finite_number(
        official.get("aggregate_target"), "prediction aggregate_target",
    )
    exposure_value = official.get("exposure_target")
    exposure = (
        aggregate
        if exposure_value is None
        else _finite_number(exposure_value, "prediction exposure_target")
    )
    if aggregate != exposure or exposure < 0 or exposure > 1:
        raise ValueError("预测暴露与纯多目标域不符")
    unit = _text(official.get("unit"), "prediction unit")
    if unit != "risk_weighted_directional_target":
        raise ValueError("预测 unit 不受支持")
    families = official.get("families")
    quality = official.get("quality")
    if not isinstance(families, list) or not isinstance(quality, dict):
        raise ValueError("预测缺少 family 或 quality 合同")
    risk_budget = _decimal(
        config.get("risk_budget_jpy") if budget_jpy is None else budget_jpy,
        "target risk_budget_jpy",
        positive=True,
    )
    return TargetExpectation(
        prediction_id,
        plan_id,
        decision_time,
        valid_until,
        valid_until_source,
        correlation_id,
        correlation_source,
        market_id,
        symbol,
        mode,
        bar_interval,
        unit,
        exposure,
        risk_budget,
        aggregate,
        source_snapshot,
        _text(official.get("input_head_generation"), "prediction input_head_generation"),
        _optional_text(
            official.get("decision_input_sha256"),
            "prediction decision_input_sha256",
        ),
        families,
        official.get("reserve"),
        quality,
    )


def _validate_target_bytes(
    body: bytes,
    expectation: TargetExpectation,
) -> dict[str, object]:
    decoded = _decode_object(body, "执行目标")
    _reject_nonfinite_json(decoded, "执行目标")
    target = _exact_object(decoded, _TARGET_KEYS, "执行目标")
    if (
        target.get("schema_version") != 2
        or target.get("artifact_kind") != "operational_target_snapshot"
        or target.get("method_version") != "frozen-forward-operational-target-v2"
        or target.get("run_id") != expectation.prediction_id
        or target.get("market_id") != expectation.market_id
        or target.get("symbol") != expectation.symbol
        or target.get("mode") != expectation.mode
        or target.get("correlation_id") != expectation.correlation_id
        or target.get("correlation_id_source") != expectation.correlation_id_source
        or target.get("valid_until_source") != expectation.valid_until_source
    ):
        raise ValueError("执行目标身份或调用参数不符")
    if (
        _timestamp(target.get("decision_time"), "target decision_time")
        != expectation.decision_time
        or _timestamp(target.get("valid_from"), "target valid_from")
        != expectation.decision_time
        or _timestamp(target.get("valid_until"), "target valid_until")
        != expectation.valid_until
        or target.get("bar_interval") != expectation.bar_interval
    ):
        raise ValueError("执行目标决策或有效期不符")
    exposure = _finite_number(target.get("exposure_target"), "target exposure_target")
    if exposure != expectation.exposure_target or exposure < 0 or exposure > 1:
        raise ValueError("执行目标 exposure_target 不符")
    if _decimal(
        target.get("risk_budget_jpy"), "target risk_budget_jpy", positive=True,
    ) != expectation.risk_budget_jpy:
        raise ValueError("执行目标 risk_budget_jpy 不符")
    semantics = _exact_object(
        target.get("target_semantics"), _TARGET_SEMANTICS_KEYS,
        "target semantics",
    )
    if semantics != {
        "domain": "long_only_spot",
        "range": [0, 1],
        "reference": "fraction_of_risk_budget",
        "short_allowed": False,
    }:
        raise ValueError("执行目标语义不符")
    contract = _exact_object(
        target.get("operational_target_contract"), _TARGET_CONTRACT_KEYS,
        "target operational contract",
    )
    if (
        _finite_number(
            contract.get("aggregate_target"), "target aggregate_target",
        ) != expectation.aggregate_target
        or contract.get("unit") != expectation.unit
        or contract.get("families") != expectation.families
        or contract.get("reserve") != expectation.reserve
        or target.get("quality") != expectation.quality
    ):
        raise ValueError("执行目标经济语义不符")
    lineage = _object(target.get("lineage"), "target lineage")
    allowed_lineage = set(_TARGET_LINEAGE_REQUIRED_KEYS)
    if expectation.decision_input_sha256 is not None:
        allowed_lineage.add("decision_input_sha256")
    if set(lineage) != allowed_lineage:
        raise ValueError("target lineage 字段不符")
    _reported_path(
        lineage.get("source_prediction_path"),
        expectation.source_snapshot.path,
        "target source_prediction_path",
    )
    if (
        lineage.get("source_prediction_sha256")
        != expectation.source_snapshot.sha256
        or lineage.get("prediction_id") != expectation.prediction_id
        or lineage.get("plan_id") != expectation.plan_id
        or lineage.get("input_head_generation")
        != expectation.input_head_generation
        or lineage.get("decision_input_sha256")
        != expectation.decision_input_sha256
    ):
        raise ValueError("target lineage 身份不符")
    return target


def _reported_path(value: object, expected: Path, name: str) -> None:
    raw = Path(_text(value, name))
    if not raw.is_absolute():
        raise ValueError(f"{name} 不是绝对路径")
    path = _absolute_canonical_path(raw, name)
    if not _same_path(path, expected):
        raise ValueError(f"{name} 不符")


def _validate_endpoints(
    value: object,
    *,
    write_planned: list[str],
    read_allowlist: frozenset[str],
    name: str,
) -> dict[str, object]:
    endpoints = _exact_object(value, _ENDPOINT_KEYS, name)
    reads = _sequence_of_text(
        endpoints.get("read_touched"), f"{name}.read_touched",
    )
    if any(item not in read_allowlist for item in reads):
        raise ValueError(f"{name}.read_touched 含非允许 GET 端点")
    planned = _sequence_of_text(
        endpoints.get("write_planned"), f"{name}.write_planned",
    )
    touched = _sequence_of_text(
        endpoints.get("write_touched"), f"{name}.write_touched",
    )
    if planned != write_planned:
        raise ValueError(f"{name}.write_planned 与终态不符")
    if touched != []:
        raise ValueError(f"{name} 触及了写端点")
    return endpoints


def _validate_proposal(value: object, name: str) -> dict[str, object]:
    proposal = _exact_object(value, _PROPOSAL_KEYS, name)
    _text(proposal.get("symbol"), f"{name}.symbol")
    if proposal.get("side") not in {"BUY", "SELL"}:
        raise ValueError(f"{name}.side 非法")
    size = _decimal(proposal.get("size"), f"{name}.size", positive=True)
    price = _decimal(proposal.get("price"), f"{name}.price", positive=True)
    notional = _decimal(
        proposal.get("notional_jpy"), f"{name}.notional_jpy", positive=True,
    )
    if size * price != notional:
        raise ValueError(f"{name}.notional_jpy 与 size*price 不符")
    return proposal


def _validate_report_bytes(
    body: bytes,
    target: FileIdentity,
    expectation: TargetExpectation,
    ledger_path: Path,
) -> dict[str, object]:
    decoded = _decode_object(body, "shadow report")
    _reject_nonfinite_json(decoded, "shadow report")
    report = _exact_object(
        decoded,
        _DRY_REPORT_KEYS,
        "shadow report",
    )
    artifact = _exact_object(
        report.get("artifact"), _DRY_ARTIFACT_KEYS, "shadow report artifact",
    )
    if report.get("mode") != DRY_RUN_MODE:
        raise ValueError("shadow report 不是 dry-run")
    if report.get("service_status") not in {"MAINTENANCE", "PREOPEN", "OPEN"}:
        raise ValueError("shadow report service_status 非法")
    _timestamp(report.get("generated_at"), "shadow report generated_at")
    if artifact.get("run_id") != expectation.prediction_id:
        raise ValueError("shadow report 预测身份不符")
    _reported_path(artifact.get("path"), target.path, "shadow target path")
    if artifact.get("sha256") != target.sha256:
        raise ValueError("shadow report 目标身份不符")
    if (
        _timestamp(
            artifact.get("decision_time"),
            "shadow report artifact.decision_time",
        ) != expectation.decision_time
        or artifact.get("market_id") != expectation.market_id
        or artifact.get("unit") != expectation.unit
        or _finite_number(
        artifact.get("aggregate_target"),
        "shadow report artifact.aggregate_target",
        ) != expectation.aggregate_target
    ):
        raise ValueError("shadow report artifact 经济血缘不符")
    if _decimal(
        report.get("budget_jpy"), "shadow report budget_jpy", positive=True,
    ) != expectation.risk_budget_jpy:
        raise ValueError("shadow report budget_jpy 与目标不符")
    reference_price = _decimal(
        report.get("reference_price"),
        "shadow report reference_price",
        positive=True,
    )
    _reported_path(report.get("ledger_path"), ledger_path, "shadow ledger path")
    proposal_value = report.get("proposal")
    intent_value = report.get("intent")
    if proposal_value is None:
        if intent_value is not None or report.get("skip_reason") not in _DRY_SKIP_REASONS:
            raise ValueError("shadow report 空提案终态关系不符")
        _validate_endpoints(
            report.get("endpoints"), write_planned=[],
            read_allowlist=_DRY_READ_ENDPOINTS,
            name="shadow report endpoints",
        )
    else:
        proposal = _validate_proposal(proposal_value, "shadow report proposal")
        proposal_price = _decimal(
            proposal.get("price"), "shadow report proposal.price", positive=True,
        )
        if (
            proposal.get("symbol") != expectation.symbol
            or (
                proposal.get("side") == "BUY"
                and proposal_price > reference_price
            )
            or (
                proposal.get("side") == "SELL"
                and proposal_price < reference_price
            )
        ):
            raise ValueError("shadow report 提案品种或取整方向不符")
        intent = _exact_object(
            intent_value, _DRY_INTENT_KEYS, "shadow report intent",
        )
        if (
            intent.get("state") != "DRY_RUN_BLOCKED"
            or intent.get("order_id") is not None
            or report.get("skip_reason") is not None
            or intent.get("correlation_id") != expectation.correlation_id
        ):
            raise ValueError("shadow report 模拟拦截终态关系不符")
        _text(intent.get("intent_id"), "shadow report intent.intent_id")
        _text(
            intent.get("correlation_id"),
            "shadow report intent.correlation_id",
        )
        _text(intent.get("reason"), "shadow report intent.reason")
        if proposal.get("side") not in {"BUY", "SELL"}:
            raise ValueError("shadow report proposal.side 非法")
        _validate_endpoints(
            report.get("endpoints"), write_planned=[ORDER_ENDPOINT],
            read_allowlist=_DRY_READ_ENDPOINTS,
            name="shadow report endpoints",
        )
    return report


def _validate_paper_startup(value: object) -> None:
    startup = _exact_object(value, _PAPER_STARTUP_KEYS, "paper startup")
    recovered = _exact_object(
        startup.get("recovered_sends"),
        _PAPER_RECOVERY_KEYS,
        "paper recovered_sends",
    )
    _sequence_of_text(
        recovered.get("intent_ids"), "paper recovered_sends.intent_ids",
    )
    if recovered.get("state") != "PAPER_REJECTED":
        raise ValueError("paper recovered_sends.state 非法")
    _text(recovered.get("reason"), "paper recovered_sends.reason")
    usage = _exact_object(
        startup.get("limit_usage"), _PAPER_LIMIT_KEYS, "paper limit_usage",
    )
    _text(usage.get("trading_day"), "paper limit_usage.trading_day")
    _decimal(
        usage.get("total_jpy"), "paper limit_usage.total_jpy", nonnegative=True,
    )
    count = usage.get("order_count")
    if type(count) is not int or count < 0:
        raise ValueError("paper limit_usage.order_count 非法")
    replayed = _sequence_of_text(
        usage.get("replayed_intents"), "paper limit_usage.replayed_intents",
    )
    if len(replayed) != count:
        raise ValueError("paper limit_usage 重放数量不符")


def _validate_paper_ledgers(value: object, execution: Path) -> None:
    ledgers = _exact_object(value, _PAPER_LEDGER_KEYS, "paper ledger_paths")
    root = execution / PAPER_LEDGER_ROOT / "execution" / "paper"
    expected = {
        "intent_ledger": root / "intent_ledger.jsonl",
        "position_ledger": root / "positions.jsonl",
        "difference_ledger": root / "difference_ledger.jsonl",
        "claim_ledger": root / "prediction_claims.jsonl",
    }
    for key, path in expected.items():
        _reported_path(ledgers.get(key), path, f"paper ledger_paths.{key}")


def _validate_paper_delta(value: object) -> tuple[dict[str, object], Decimal, Decimal]:
    delta = _exact_object(value, _PAPER_DELTA_KEYS, "paper delta")
    desired = _decimal(
        delta.get("desired_size"), "paper delta.desired_size", nonnegative=True,
    )
    position = _decimal(
        delta.get("position_size"), "paper delta.position_size", nonnegative=True,
    )
    difference = _decimal(delta.get("delta_size"), "paper delta.delta_size")
    if desired - position != difference:
        raise ValueError("paper delta.delta_size 与 desired-position 不符")
    return delta, desired, position


def _validate_paper_fill(
    value: object,
    proposal: Mapping[str, object],
) -> tuple[dict[str, object], Decimal, Decimal, Decimal]:
    fill = _exact_object(value, _PAPER_FILL_KEYS, "paper fill")
    side = fill.get("side")
    if side != proposal.get("side"):
        raise ValueError("paper fill.side 与提案不符")
    fill_size = _decimal(fill.get("fill_size"), "paper fill.fill_size", positive=True)
    proposal_size = _decimal(proposal.get("size"), "paper proposal.size", positive=True)
    if fill_size != proposal_size:
        raise ValueError("paper fill.fill_size 与提案不符")
    expected_price = _decimal(
        fill.get("expected_price"), "paper fill.expected_price", positive=True,
    )
    proposal_price = _decimal(
        proposal.get("price"), "paper proposal.price", positive=True,
    )
    if expected_price != proposal_price:
        raise ValueError("paper fill.expected_price 与提案不符")
    model_price = _decimal(
        fill.get("model_fill_price"), "paper fill.model_fill_price", positive=True,
    )
    notional = _decimal(
        fill.get("notional_jpy"), "paper fill.notional_jpy", positive=True,
    )
    if fill_size * model_price != notional:
        raise ValueError("paper fill.notional_jpy 与成交数量价格不符")
    _decimal(fill.get("fee_jpy"), "paper fill.fee_jpy", nonnegative=True)
    levels = fill.get("levels_consumed")
    if type(levels) is not int or levels <= 0:
        raise ValueError("paper fill.levels_consumed 非法")
    _text(fill.get("fill_basis"), "paper fill.fill_basis")
    _text(fill.get("fee_source"), "paper fill.fee_source")
    _timestamp(fill.get("book_observed_at"), "paper fill.book_observed_at")
    return fill, fill_size, expected_price, model_price


def _validate_paper_cost(value: object) -> dict[str, Decimal]:
    cost = _exact_object(value, _PAPER_COST_KEYS, "paper cost")
    numbers = {
        key: _decimal(
            cost.get(key), f"paper cost.{key}", nonnegative=True,
        )
        for key in _PAPER_COST_KEYS
    }
    if (
        numbers["fee_bps"] + numbers["slippage_vs_reference_bps"]
        != numbers["total_cost_bps"]
    ):
        raise ValueError("paper cost.total_cost_bps 分解不符")
    return numbers


def _validate_paper_report_bytes(
    body: bytes,
    target: FileIdentity,
    expectation: TargetExpectation,
    execution: Path,
) -> dict[str, object]:
    """校验执行仓真实 paper 报告的精确字段与终态关系。"""
    report = _decode_object(body, "paper report")
    _reject_nonfinite_json(report, "paper report")
    status = report.get("status")
    if status not in _PAPER_STATUSES:
        raise ValueError("paper report status 不在允许终态")
    expected_keys = (
        _PAPER_DUPLICATE_KEYS
        if status == "duplicate_prediction"
        else _PAPER_DECISION_KEYS
    )
    report = _exact_object(report, expected_keys, "paper report")
    if report.get("prediction_id") != expectation.prediction_id:
        raise ValueError("paper report 预测身份不符")
    _reported_path(report.get("target_path"), target.path, "paper target path")
    if report.get("target_sha256") != target.sha256:
        raise ValueError("paper report 目标身份不符")
    _timestamp(report.get("generated_at"), "paper generated_at")
    _validate_paper_ledgers(report.get("ledger_paths"), execution)
    _validate_paper_startup(report.get("startup"))
    _validate_endpoints(
        report.get("endpoints"),
        write_planned=[],
        read_allowlist=_PAPER_READ_ENDPOINTS,
        name="paper report endpoints",
    )
    if status == "duplicate_prediction":
        return report
    if (
        report.get("mode") != PAPER_MODE
        or report.get("record") != "paper_decision"
        or type(report.get("schema_version")) is not int
        or report.get("schema_version") != 1
    ):
        raise ValueError("paper report 差异行身份不符")
    _timestamp(report.get("at"), "paper at")
    if (
        _timestamp(report.get("decision_time"), "paper decision_time")
        != expectation.decision_time
        or _timestamp(report.get("valid_until"), "paper valid_until")
        != expectation.valid_until
        or report.get("correlation_id") != expectation.correlation_id
        or report.get("market_id") != expectation.market_id
        or report.get("symbol") != expectation.symbol
    ):
        raise ValueError("paper report 经济血缘与目标不符")
    exposure = _finite_number(
        report.get("exposure_target"), "paper exposure_target",
    )
    if exposure != expectation.exposure_target or exposure < 0 or exposure > 1:
        raise ValueError("paper exposure_target 与目标不符")
    risk_budget = _decimal(
        report.get("risk_budget_jpy"), "paper risk_budget_jpy", positive=True,
    )
    if risk_budget != expectation.risk_budget_jpy:
        raise ValueError("paper risk_budget_jpy 与目标不符")
    position_before = _decimal(
        report.get("position_before"), "paper position_before", nonnegative=True,
    )
    position_after = _decimal(
        report.get("position_after"), "paper position_after", nonnegative=True,
    )
    _optional_text(report.get("book_error"), "paper book_error")
    if report.get("service_status") not in {"MAINTENANCE", "PREOPEN", "OPEN"}:
        raise ValueError("paper service_status 非法")
    # 诊断对象仍须有限
    _object(report.get("overlay"), "paper overlay")

    delta_value = report.get("delta")
    intent_value = report.get("intent")
    fill_value = report.get("fill")
    cost_value = report.get("cost")
    fee_value = report.get("fee")
    if status == "book_unavailable":
        if any(item is not None for item in (
            delta_value, intent_value, fill_value, cost_value, fee_value,
            report.get("reference_price"), report.get("target_notional_jpy"),
        )) or position_after != position_before:
            raise ValueError("paper book_unavailable 字段关系不符")
        if not isinstance(report.get("book_error"), str):
            raise ValueError("paper book_unavailable 缺少 book_error")
        return report

    if report.get("book_error") is not None:
        raise ValueError("paper 非 book_unavailable 不得含 book_error")

    delta, desired_size, delta_position = _validate_paper_delta(delta_value)
    if delta_position != position_before:
        raise ValueError("paper delta.position_size 与 position_before 不符")
    reference_price = _decimal(
        report.get("reference_price"), "paper reference_price", positive=True,
    )
    target_notional = _decimal(
        report.get("target_notional_jpy"), "paper target_notional_jpy",
        nonnegative=True,
    )
    if target_notional != desired_size * reference_price:
        raise ValueError("paper target_notional_jpy 与目标数量价格不符")
    expected_notional = exposure * risk_budget
    if abs(target_notional - expected_notional) > Decimal("1e-18"):
        raise ValueError("paper desired_size 与 exposure/risk budget 不符")
    proposal_value = delta.get("proposal")
    if status == "skipped":
        if (
            proposal_value is not None
            or delta.get("skip_reason") not in _PAPER_SKIP_REASONS
            or any(item is not None for item in (
                intent_value, fill_value, cost_value, fee_value,
            ))
            or position_after != position_before
        ):
            raise ValueError("paper skipped 字段关系不符")
        return report

    proposal = _validate_proposal(proposal_value, "paper delta.proposal")
    if proposal.get("symbol") != expectation.symbol:
        raise ValueError("paper proposal.symbol 与目标不符")
    if delta.get("skip_reason") is not None:
        raise ValueError("paper 有提案时 skip_reason 必须为空")
    proposal_price = _decimal(
        proposal.get("price"), "paper proposal.price", positive=True,
    )
    if (
        (proposal.get("side") == "BUY" and proposal_price > reference_price)
        or (proposal.get("side") == "SELL" and proposal_price < reference_price)
    ):
        raise ValueError("paper proposal.price 取整方向与 reference_price 不符")
    delta_size = _decimal(delta.get("delta_size"), "paper delta.delta_size")
    proposal_size = _decimal(
        proposal.get("size"), "paper proposal.size", positive=True,
    )
    if (
        (proposal.get("side") == "BUY" and delta_size <= 0)
        or (proposal.get("side") == "SELL" and delta_size >= 0)
        or (
            status != "sell_exceeds_position"
            and proposal_size > abs(delta_size)
        )
    ):
        raise ValueError("paper proposal side/size 与 delta 不符")
    if status == "sell_exceeds_position":
        size = _decimal(proposal.get("size"), "paper proposal.size", positive=True)
        if (
            proposal.get("side") != "SELL"
            or size <= position_before
            or any(item is not None for item in (
                intent_value, fill_value, cost_value, fee_value,
            ))
            or position_after != position_before
        ):
            raise ValueError("paper sell_exceeds_position 字段关系不符")
        return report

    intent = _exact_object(intent_value, _PAPER_INTENT_KEYS, "paper intent")
    if intent.get("state") != status:
        raise ValueError("paper intent.state 与 status 不符")
    _text(intent.get("intent_id"), "paper intent.intent_id")
    _optional_text(intent.get("reason"), "paper intent.reason")
    if (
        intent.get("side") != proposal.get("side")
        or _decimal(intent.get("size"), "paper intent.size", positive=True)
        != _decimal(proposal.get("size"), "paper proposal.size", positive=True)
        or _decimal(intent.get("price"), "paper intent.price", positive=True)
        != proposal_price
    ):
        raise ValueError("paper intent 与提案不符")
    if status == "PAPER_FILLED":
        fill, fill_size, expected_price, model_price = _validate_paper_fill(
            fill_value, proposal,
        )
        costs = _validate_paper_cost(cost_value)
        fee = _exact_object(fee_value, _PAPER_FEE_KEYS, "paper fee")
        fee_bps = _decimal(
            fee.get("bps"), "paper fee.bps", nonnegative=True,
        )
        _text(fee.get("source"), "paper fee.source")
        _optional_text(fee.get("detail"), "paper fee.detail")
        if (
            fee_bps != costs["fee_bps"]
            or fill.get("fee_source") != fee.get("source")
        ):
            raise ValueError("paper fee 与 fill/cost 不符")
        fill_notional = _decimal(
            fill.get("notional_jpy"), "paper fill.notional_jpy", positive=True,
        )
        fill_fee = _decimal(
            fill.get("fee_jpy"), "paper fill.fee_jpy", nonnegative=True,
        )
        if fill_fee != fill_notional * fee_bps / Decimal("10000"):
            raise ValueError("paper fill.fee_jpy 与 fee_bps 不符")
        side = proposal.get("side")
        sign = Decimal("1") if side == "BUY" else Decimal("-1")
        if (
            (side == "BUY" and model_price < expected_price)
            or (side == "SELL" and model_price > expected_price)
        ):
            raise ValueError("paper model_fill_price 不是不利成交")
        expected_slippage = (
            sign * (model_price - expected_price)
            / expected_price * Decimal("10000")
        )
        if costs["slippage_vs_reference_bps"] != expected_slippage:
            raise ValueError("paper slippage 与成交公式不符")
        expected_position = (
            position_before + fill_size
            if proposal.get("side") == "BUY"
            else position_before - fill_size
        )
        if position_after != expected_position:
            raise ValueError("paper PAPER_FILLED 持仓变化不符")
        raise ValueError("PAPER_FILLED 成本 provenance 未绑定，禁止提交回执")
    else:
        if fill_value is not None or cost_value is not None:
            raise ValueError("paper PAPER_REJECTED 不得包含模型成交")
        if fee_value is not None:
            fee = _exact_object(fee_value, _PAPER_FEE_KEYS, "paper fee")
            _decimal(fee.get("bps"), "paper fee.bps", nonnegative=True)
            _text(fee.get("source"), "paper fee.source")
            _optional_text(fee.get("detail"), "paper fee.detail")
        if position_after != position_before:
            raise ValueError("paper PAPER_REJECTED 不得改变持仓")
    return report


def _adapt_target(
    execution: Path,
    environment_identity: EnvironmentIdentity,
    prediction_snapshot: FileIdentity,
    config: FileIdentity,
    expectation: TargetExpectation,
    execution_identity: ExecutionIdentity,
    execution_tracked: Sequence[FileIdentity],
    *,
    market_id: str,
    symbol: str,
    mode: str,
    budget_jpy: str | None,
) -> FileIdentity:
    """经执行仓适配器生成并验证内容寻址目标快照。"""
    target_root = _managed_directory(
        execution, TARGET_DIRECTORY, f"{mode} 目标目录",
    )
    arguments = [
        "--prediction", str(prediction_snapshot.path),
        "--output-directory", str(target_root),
        "--config", str(config.path),
        "--market-id", market_id, "--symbol", symbol, "--mode", mode,
    ]
    if budget_jpy is not None:
        arguments.extend(("--risk-budget-jpy", budget_jpy))
    command = _isolated_python_command(
        environment_identity,
        execution / "src",
        execution / "scripts/adapt_frozen_target.py",
        arguments,
    )
    _stable_identity(
        prediction_snapshot.path,
        f"{mode} 适配前来源快照",
        prediction_snapshot.sha256,
    )
    _stable_identity(config.path, f"{mode} 适配前配置", config.sha256)
    _execution_environment_identity(execution, environment_identity)
    _execution_identity(execution, execution_identity)
    adapted = _exact_object(_json_stdout(_run_pinned(
        command,
        cwd=execution,
        directories=(
            (target_root, f"{mode} 目标目录"),
            (prediction_snapshot.path.parent, f"{mode} 来源目录"),
            (config.path.parent, f"{mode} 配置目录"),
            (
                environment_identity.python.path.parent,
                f"{mode} 执行 Python 目录",
            ),
            (
                environment_identity.pyvenv_config.path.parent,
                f"{mode} pyvenv 配置目录",
            ),
        ),
        guarded_files=(
            prediction_snapshot,
            config,
            *_environment_guarded_files(environment_identity),
            *execution_tracked,
        ),
        repository_checks=((
            execution, "执行仓", execution_identity, False,
        ),),
        env=_business_child_environment(),
    ), f"{mode} target adapter"), _ADAPTER_STDOUT_KEYS, f"{mode} target adapter")
    _stable_identity(
        prediction_snapshot.path,
        f"{mode} 适配后来源快照",
        prediction_snapshot.sha256,
    )
    _stable_identity(config.path, f"{mode} 适配后配置", config.sha256)
    _execution_environment_identity(execution, environment_identity)
    _execution_identity(execution, execution_identity)
    expected = f"ready_for_{mode.replace('-', '_')}"
    if adapted.get("status") != expected:
        raise ValueError(f"{mode} 目标状态不符: {adapted.get('status')!r}")
    declared_sha256 = _digest(adapted.get("sha256"), "target adapter sha256")
    target_path = _reported_managed_file(
        adapted.get("path"), target_root, "target path",
    )
    if target_path.name != f"target-{declared_sha256}.json":
        raise ValueError("执行目标文件名不是声明散列的内容地址")
    target = _stable_identity(target_path, "执行目标", declared_sha256)
    _validate_target_bytes(
        _stable_file_bytes(target.path, "执行目标"), expectation,
    )
    return target


def _run_refresh_process(
    source_root: Path,
    runtime: Path,
    code_root: Path,
    environment: EnvironmentIdentity,
    code_identity: ExecutionIdentity,
    code_tracked: Sequence[FileIdentity],
    market_id: str,
) -> dict[str, object]:
    command = _isolated_python_command(
        environment,
        code_root / "src",
        code_root / "scripts/refresh_frozen_runtime.py",
        (
            "--source-data-root", str(source_root / "data"),
            "--runtime-root", str(runtime),
            "--market-id", market_id,
        ),
    )
    return _json_stdout(_run_pinned(
        command,
        cwd=code_root,
        directories=(
            (code_root, "刷新代码根"),
            (runtime, "冻结运行根"),
            (source_root, "live 数据根"),
            (environment.python.path.parent, "刷新 Python 目录"),
        ),
        guarded_files=(
            *code_tracked,
            *_environment_guarded_files(environment),
        ),
        repository_checks=((
            code_root, "runner 代码仓", code_identity, True,
        ),),
        env=_business_child_environment(),
    ), "frozen runtime refresh")


def _receipt_payload(
    stage: str,
    plan_id: str,
    prediction_id: str,
    source_origin: FileIdentity,
    source_snapshot: FileIdentity,
    target: FileIdentity,
    report: FileIdentity,
    config_origin: FileIdentity,
    config_snapshot: FileIdentity,
    runner_python: FileIdentity,
    git_identity: FileIdentity,
    environment_identity: EnvironmentIdentity,
    code_identity: ExecutionIdentity,
    runtime_identity: ExecutionIdentity,
    execution_identity: ExecutionIdentity,
) -> dict[str, object]:
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "kind": RECEIPT_KIND,
        "commit_state": RECEIPT_COMMIT_STATE,
        "status": RECEIPT_STATUS,
        "stage": stage,
        "plan_id": plan_id,
        "prediction_id": prediction_id,
        "source_origin": _identity_payload(source_origin),
        "source_snapshot": _identity_payload(source_snapshot),
        "target": _identity_payload(target),
        "report": _identity_payload(report),
        "config_origin": _identity_payload(config_origin),
        "config_snapshot": _identity_payload(config_snapshot),
        "runner_python": _identity_payload(runner_python),
        "git_executable": _identity_payload(git_identity),
        "execution_environment": _environment_payload(environment_identity),
        "code_repository": _execution_payload(code_identity),
        "runtime_repository": _execution_payload(runtime_identity),
        "execution_repository": _execution_payload(execution_identity),
    }


def _identity_from_receipt(value: object, name: str) -> FileIdentity:
    raw = _exact_object(value, _IDENTITY_KEYS, name)
    path = Path(_text(raw.get("path"), f"{name}.path"))
    if not path.is_absolute():
        raise ValueError(f"{name}.path 不是绝对路径")
    return FileIdentity(
        _absolute_canonical_path(path, f"{name}.path"),
        _digest(raw.get("sha256"), f"{name}.sha256"),
    )


def _execution_from_receipt(value: object, name: str) -> ExecutionIdentity:
    raw = _exact_object(
        value, _EXECUTION_IDENTITY_KEYS, f"stage receipt {name}",
    )
    path = _absolute_canonical_path(
        Path(_text(raw.get("path"), f"stage receipt {name}.path")),
        f"stage receipt {name}.path",
    )
    head = _text(
        raw.get("head_commit"), f"stage receipt {name}.head_commit",
    )
    if re.fullmatch(r"[0-9a-f]{40,64}", head) is None:
        raise ValueError(f"stage receipt {name}.head_commit 非法")
    return ExecutionIdentity(path, head)


def _validate_environment_receipt(
    value: object,
    expected: EnvironmentIdentity,
) -> None:
    raw = _exact_object(
        value, _ENVIRONMENT_IDENTITY_KEYS,
        "stage receipt execution_environment",
    )
    attestation = _text(
        raw.get("attestation"),
        "stage receipt execution_environment.attestation",
    )
    if attestation != ENVIRONMENT_ATTESTATION:
        raise ValueError("执行环境证明级别不符")
    guard_strength = _text(
        raw.get("guard_strength"),
        "stage receipt execution_environment.guard_strength",
    )
    python = _identity_from_receipt(
            raw.get("python"), "stage receipt execution_environment.python",
    )
    pyvenv_config = _identity_from_receipt(
            raw.get("pyvenv_config"),
            "stage receipt execution_environment.pyvenv_config",
    )
    manifest = _identity_from_receipt(
        raw.get("manifest"), "stage receipt execution_environment.manifest",
    )
    pycache_sentinel = _identity_from_receipt(
        raw.get("pycache_sentinel"),
        "stage receipt execution_environment.pycache_sentinel",
    )
    empty_child_env = _identity_from_receipt(
        raw.get("empty_child_env"),
        "stage receipt execution_environment.empty_child_env",
    )
    file_count = _positive_integer(
        raw.get("file_count"), "stage receipt environment.file_count",
    )
    total_bytes = _positive_integer(
        raw.get("total_bytes"), "stage receipt environment.total_bytes",
    )
    tree_sha256 = _digest(
        raw.get("tree_sha256"), "stage receipt environment.tree_sha256",
    )
    closure_value = _exact_object(
        raw.get("import_closures"),
        _IMPORT_CLOSURES_KEYS,
        "stage receipt import_closures",
    )
    expected_closures = {
        closure.role: closure for closure in _import_closures(expected)
    }
    for role, closure in expected_closures.items():
        recorded = _exact_object(
            closure_value.get(role),
            _IMPORT_CLOSURE_KEYS,
            f"stage receipt {role} import closure",
        )
        root = _absolute_canonical_path(
            Path(_text(recorded.get("root"), f"{role} closure.root")),
            f"{role} closure.root",
        )
        manifest_identity = _identity_from_receipt(
            recorded.get("manifest"), f"stage receipt {role} closure.manifest",
        )
        if (
            recorded.get("role") != role
            or root != closure.root
            or manifest_identity != closure.manifest
            or _positive_integer(
                recorded.get("file_count"), f"{role} closure.file_count",
            ) != closure.file_count
            or _positive_integer(
                recorded.get("total_bytes"), f"{role} closure.total_bytes",
            ) != closure.total_bytes
            or _digest(
                recorded.get("tree_sha256"), f"{role} closure.tree_sha256",
            ) != closure.tree_sha256
        ):
            raise ValueError(f"{role} import closure 回执身份不符")
    if (
        guard_strength != expected.guard_strength
        or python != expected.python
        or pyvenv_config != expected.pyvenv_config
        or manifest != expected.manifest
        or pycache_sentinel != expected.pycache_sentinel
        or empty_child_env != expected.empty_child_env
        or file_count != expected.file_count
        or total_bytes != expected.total_bytes
        or tree_sha256 != expected.tree_sha256
    ):
        raise ValueError("执行环境回执身份不符")


def _receipt_contract(
    value: object,
    *,
    stage: str,
    plan_id: str,
    prediction_id: str,
    source_origin: FileIdentity,
    source_snapshot: FileIdentity,
    config_origin: FileIdentity,
    config_snapshot: FileIdentity,
    runner_python: FileIdentity,
    git_identity: FileIdentity,
    environment_identity: EnvironmentIdentity,
    code_identity: ExecutionIdentity,
    runtime_identity: ExecutionIdentity,
    execution_identity: ExecutionIdentity,
    execution: Path,
    report_path: Path,
) -> tuple[FileIdentity, FileIdentity]:
    receipt = _exact_object(value, _RECEIPT_KEYS, "stage receipt")
    if (
        type(receipt.get("schema_version")) is not int
        or receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or receipt.get("kind") != RECEIPT_KIND
        or receipt.get("commit_state") != RECEIPT_COMMIT_STATE
        or receipt.get("status") != RECEIPT_STATUS
        or receipt.get("stage") != stage
        or receipt.get("plan_id") != plan_id
        or receipt.get("prediction_id") != prediction_id
    ):
        raise ValueError("stage receipt 顶层身份不符")
    recorded_origin = _identity_from_receipt(
        receipt.get("source_origin"), "stage receipt source_origin",
    )
    recorded_snapshot = _identity_from_receipt(
        receipt.get("source_snapshot"), "stage receipt source_snapshot",
    )
    if recorded_origin != source_origin or recorded_snapshot != source_snapshot:
        raise ValueError("stage receipt 来源身份不符")
    recorded_config_origin = _identity_from_receipt(
        receipt.get("config_origin"), "stage receipt config_origin",
    )
    recorded_config_snapshot = _identity_from_receipt(
        receipt.get("config_snapshot"), "stage receipt config_snapshot",
    )
    recorded_runner_python = _identity_from_receipt(
        receipt.get("runner_python"), "stage receipt runner_python",
    )
    recorded_git = _identity_from_receipt(
        receipt.get("git_executable"), "stage receipt git_executable",
    )
    _validate_environment_receipt(
        receipt.get("execution_environment"), environment_identity,
    )
    recorded_code = _execution_from_receipt(
        receipt.get("code_repository"), "code_repository",
    )
    recorded_runtime = _execution_from_receipt(
        receipt.get("runtime_repository"), "runtime_repository",
    )
    recorded_execution = _execution_from_receipt(
        receipt.get("execution_repository"), "execution_repository",
    )
    if (
        recorded_config_origin != config_origin
        or recorded_config_snapshot != config_snapshot
        or recorded_runner_python != runner_python
        or recorded_git != git_identity
        or recorded_code != code_identity
        or recorded_runtime != runtime_identity
        or recorded_execution != execution_identity
    ):
        raise ValueError("stage receipt 配置或代码仓身份不符")
    target = _identity_from_receipt(receipt.get("target"), "stage receipt target")
    report = _identity_from_receipt(receipt.get("report"), "stage receipt report")
    target_root = _managed_directory(
        execution, TARGET_DIRECTORY, "receipt 目标目录",
    )
    if (
        target.path.parent != target_root
        or target.path.name != f"target-{target.sha256}.json"
    ):
        raise ValueError("stage receipt 目标路径非法")
    if not _same_path(report.path, report_path):
        raise ValueError("stage receipt 报告路径不符")
    return target, report


def _load_committed_stage_unpinned(
    *,
    stage: str,
    receipt_path: Path,
    report_path: Path,
    plan_id: str,
    prediction_id: str,
    source_origin: FileIdentity,
    source_snapshot: FileIdentity,
    config_origin: FileIdentity,
    config_snapshot: FileIdentity,
    runner_python: FileIdentity,
    git_identity: FileIdentity,
    expectation: TargetExpectation,
    environment_identity: EnvironmentIdentity,
    code_identity: ExecutionIdentity,
    runtime_identity: ExecutionIdentity,
    execution_identity: ExecutionIdentity,
    runtime_tracked: Sequence[FileIdentity],
    execution_tracked: Sequence[FileIdentity],
    code_root: Path,
    runtime: Path,
    execution: Path,
    dry_ledger_path: Path | None,
    verify_origin_current: bool,
) -> StageResult | None:
    if stage == DRY_RUN_MODE and dry_ledger_path is None:
        raise ValueError("dry-run 缺少意图账路径合同")
    report_exists = _entry_exists(report_path)
    receipt_exists = _entry_exists(receipt_path)
    if report_exists != receipt_exists:
        raise ValueError(f"{stage} report/receipt 单边存在，拒绝继续")
    if not report_exists:
        return None
    receipt_body = _stable_file_bytes(
        receipt_path, f"{stage} committed receipt",
    )
    receipt_identity = FileIdentity(
        _absolute_canonical_path(receipt_path, "stage receipt"),
        _sha256_bytes(receipt_body),
    )
    receipt_value = _decode_object(receipt_body, f"{stage} committed receipt")
    target, recorded_report = _receipt_contract(
        receipt_value,
        stage=stage,
        plan_id=plan_id,
        prediction_id=prediction_id,
        source_origin=source_origin,
        source_snapshot=source_snapshot,
        config_origin=config_origin,
        config_snapshot=config_snapshot,
        runner_python=runner_python,
        git_identity=git_identity,
        environment_identity=environment_identity,
        code_identity=code_identity,
        runtime_identity=runtime_identity,
        execution_identity=execution_identity,
        execution=execution,
        report_path=report_path,
    )
    if verify_origin_current:
        _stable_identity(
            source_origin.path, "receipt 来源原件", source_origin.sha256,
        )
    _stable_identity(
        source_snapshot.path, "receipt 来源快照", source_snapshot.sha256,
    )
    _assert_distinct_files(
        source_origin.path, source_snapshot.path, "receipt 来源原件与快照",
    )
    _stable_identity(
        config_origin.path, "receipt 执行配置原件", config_origin.sha256,
    )
    _stable_identity(
        config_snapshot.path,
        "receipt 执行配置快照",
        config_snapshot.sha256,
    )
    _assert_distinct_files(
        config_origin.path, config_snapshot.path, "receipt 配置原件与快照",
    )
    _revalidate_identity(runner_python, "receipt runner Python")
    _revalidate_identity(git_identity, "receipt Git 执行文件")
    _execution_environment_identity(execution, environment_identity)
    _repository_identity(
        code_root, "runner 代码仓", code_identity,
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    _repository_identity(
        runtime, "冻结运行仓", runtime_identity,
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    _execution_identity(execution, execution_identity)
    target = _stable_identity(target.path, "receipt 执行目标", target.sha256)
    _validate_target_bytes(
        _stable_file_bytes(target.path, "receipt 执行目标"), expectation,
    )
    report_body = _stable_file_bytes(report_path, f"{stage} report")
    report = FileIdentity(
        _absolute_canonical_path(report_path, f"{stage} report"),
        _sha256_bytes(report_body),
    )
    if report != recorded_report:
        raise ValueError(f"{stage} report SHA-256 已变化")
    payload = (
        _validate_report_bytes(
            report_body,
            target,
            expectation,
            dry_ledger_path
            if dry_ledger_path is not None
            else execution / SHADOW_ROOT / "intent_ledger.jsonl",
        )
        if stage == DRY_RUN_MODE
        else _validate_paper_report_bytes(
            report_body, target, expectation, execution,
        )
    )
    _stable_identity(receipt_path, f"{stage} committed receipt", receipt_identity.sha256)
    if verify_origin_current:
        _stable_identity(
            source_origin.path, "receipt 来源原件", source_origin.sha256,
        )
    _stable_identity(source_snapshot.path, "receipt 来源快照", source_snapshot.sha256)
    _assert_distinct_files(
        source_origin.path, source_snapshot.path, "receipt 来源原件与快照",
    )
    _stable_identity(
        config_origin.path, "receipt 执行配置原件", config_origin.sha256,
    )
    _stable_identity(
        config_snapshot.path,
        "receipt 执行配置快照",
        config_snapshot.sha256,
    )
    _assert_distinct_files(
        config_origin.path, config_snapshot.path, "receipt 配置原件与快照",
    )
    _revalidate_identity(runner_python, "receipt runner Python")
    _revalidate_identity(git_identity, "receipt Git 执行文件")
    _execution_environment_identity(execution, environment_identity)
    _stable_identity(target.path, "receipt 执行目标", target.sha256)
    _stable_identity(report.path, f"{stage} report", report.sha256)
    _repository_identity(
        code_root, "runner 代码仓", code_identity,
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    _repository_identity(
        runtime, "冻结运行仓", runtime_identity,
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    _execution_identity(execution, execution_identity)
    return StageResult(target, report, receipt_identity, payload, True, None)


def _load_committed_stage(
    *,
    stage: str,
    receipt_path: Path,
    report_path: Path,
    plan_id: str,
    prediction_id: str,
    source_origin: FileIdentity,
    source_snapshot: FileIdentity,
    config_origin: FileIdentity,
    config_snapshot: FileIdentity,
    runner_python: FileIdentity,
    git_identity: FileIdentity,
    expectation: TargetExpectation,
    environment_identity: EnvironmentIdentity,
    code_identity: ExecutionIdentity,
    runtime_identity: ExecutionIdentity,
    execution_identity: ExecutionIdentity,
    runtime_tracked: Sequence[FileIdentity],
    execution_tracked: Sequence[FileIdentity],
    code_root: Path,
    runtime: Path,
    execution: Path,
    dry_ledger_path: Path | None,
    verify_origin_current: bool,
    transaction_state: StageLockState,
) -> StageResult | None:
    """在所有证据父目录固定句柄的窗口内完成复用验证。"""
    target_root = _managed_directory(
        execution, TARGET_DIRECTORY, f"{stage} 目标目录",
    )
    directories = (
        (receipt_path.parent, f"{stage} 回执目录"),
        (report_path.parent, f"{stage} 报告目录"),
        (source_origin.path.parent, f"{stage} 来源原件目录"),
        (source_snapshot.path.parent, f"{stage} 来源快照目录"),
        (config_origin.path.parent, f"{stage} 配置原件目录"),
        (config_snapshot.path.parent, f"{stage} 配置快照目录"),
        (
            environment_identity.python.path.parent,
            f"{stage} 执行 Python 目录",
        ),
        (
            environment_identity.pyvenv_config.path.parent,
            f"{stage} pyvenv 配置目录",
        ),
        (target_root, f"{stage} 目标目录"),
    )
    seen: set[str] = set()
    pinned: list[tuple[_PinnedDirectory, str]] = []
    with ExitStack() as stack:
        for directory, name in directories:
            key = os.path.normcase(str(directory))
            if key in seen:
                continue
            seen.add(key)
            item = stack.enter_context(_pin_directory_chain(
                directory,
                name,
                transaction_state=transaction_state,
            ))
            pinned.append((item, name))
        result = _load_committed_stage_unpinned(
            stage=stage,
            receipt_path=receipt_path,
            report_path=report_path,
            plan_id=plan_id,
            prediction_id=prediction_id,
            source_origin=source_origin,
            source_snapshot=source_snapshot,
            config_origin=config_origin,
            config_snapshot=config_snapshot,
            runner_python=runner_python,
            git_identity=git_identity,
            expectation=expectation,
            environment_identity=environment_identity,
            code_identity=code_identity,
            runtime_identity=runtime_identity,
            execution_identity=execution_identity,
            runtime_tracked=runtime_tracked,
            execution_tracked=execution_tracked,
            code_root=code_root,
            runtime=runtime,
            execution=execution,
            dry_ledger_path=dry_ledger_path,
            verify_origin_current=verify_origin_current,
        )
        for item, name in pinned:
            _revalidate_directory_chain(item.path, item.identity, name)
        if result is not None:
            transaction_state.committed = True
        return result


def _commit_stage(
    *,
    stage: str,
    receipt_path: Path,
    report_path: Path,
    plan_id: str,
    prediction_id: str,
    source_origin: FileIdentity,
    source_snapshot: FileIdentity,
    target: FileIdentity,
    config_origin: FileIdentity,
    config_snapshot: FileIdentity,
    runner_python: FileIdentity,
    git_identity: FileIdentity,
    expectation: TargetExpectation,
    environment_identity: EnvironmentIdentity,
    code_identity: ExecutionIdentity,
    runtime_identity: ExecutionIdentity,
    execution_identity: ExecutionIdentity,
    runtime_tracked: Sequence[FileIdentity],
    execution_tracked: Sequence[FileIdentity],
    code_root: Path,
    runtime: Path,
    execution: Path,
    dry_ledger_path: Path | None,
    returncode: int,
    transaction_state: StageLockState,
) -> StageResult:
    if returncode != 0:
        raise ValueError(f"{stage} 非零返回码不能提交成功回执")
    if stage == DRY_RUN_MODE and dry_ledger_path is None:
        raise ValueError("dry-run 缺少意图账路径合同")
    _stable_identity(source_snapshot.path, "提交前来源快照", source_snapshot.sha256)
    _stable_identity(
        config_origin.path, "提交前执行配置原件", config_origin.sha256,
    )
    _stable_identity(
        config_snapshot.path, "提交前执行配置快照",
        config_snapshot.sha256,
    )
    _revalidate_identity(runner_python, "提交前 runner Python")
    _revalidate_identity(git_identity, "提交前 Git 执行文件")
    _execution_environment_identity(execution, environment_identity)
    _repository_identity(
        code_root, "runner 代码仓", code_identity,
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    _repository_identity(
        runtime, "冻结运行仓", runtime_identity,
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    _execution_identity(execution, execution_identity)
    target = _stable_identity(target.path, "提交前执行目标", target.sha256)
    _validate_target_bytes(
        _stable_file_bytes(target.path, "提交前执行目标"), expectation,
    )
    report_body = _stable_file_bytes(report_path, f"{stage} report")
    report = FileIdentity(
        _absolute_canonical_path(report_path, f"{stage} report"),
        _sha256_bytes(report_body),
    )
    payload = (
        _validate_report_bytes(
            report_body,
            target,
            expectation,
            dry_ledger_path
            if dry_ledger_path is not None
            else execution / SHADOW_ROOT / "intent_ledger.jsonl",
        )
        if stage == DRY_RUN_MODE
        else _validate_paper_report_bytes(
            report_body, target, expectation, execution,
        )
    )
    receipt_body = (json.dumps(
        _receipt_payload(
            stage, plan_id, prediction_id, source_origin, source_snapshot,
            target, report, config_origin, config_snapshot,
            runner_python, git_identity, environment_identity,
            code_identity, runtime_identity,
            execution_identity,
        ),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ) + "\n").encode("utf-8")
    _atomic_publish_new(
        receipt_path, receipt_body, allow_existing_identical=False,
    )
    # 回执持久后不能安全重跑
    transaction_state.committed = True
    committed = _load_committed_stage(
        stage=stage,
        receipt_path=receipt_path,
        report_path=report_path,
        plan_id=plan_id,
        prediction_id=prediction_id,
        source_origin=source_origin,
        source_snapshot=source_snapshot,
        config_origin=config_origin,
        config_snapshot=config_snapshot,
        runner_python=runner_python,
        git_identity=git_identity,
        expectation=expectation,
        environment_identity=environment_identity,
        code_identity=code_identity,
        runtime_identity=runtime_identity,
        execution_identity=execution_identity,
        runtime_tracked=runtime_tracked,
        execution_tracked=execution_tracked,
        code_root=code_root,
        runtime=runtime,
        execution=execution,
        dry_ledger_path=dry_ledger_path,
        verify_origin_current=False,
        transaction_state=transaction_state,
    )
    if committed is None:
        raise ValueError(f"{stage} 成功回执提交后不可见")
    return StageResult(
        committed.target, committed.report, committed.receipt,
        committed.report_payload, False, returncode,
    )


def _paper_summary(result: StageResult) -> dict[str, object]:
    """提取 paper 报告终态、模型成交与证据身份。"""
    report = result.report_payload
    endpoints = _object(report.get("endpoints"), "paper report endpoints")
    intent = report.get("intent")
    return {
        "status": "reused" if result.reused else "completed",
        "target_path": str(result.target.path),
        "target_sha256": result.target.sha256,
        "report_path": str(result.report.path),
        "report_sha256": result.report.sha256,
        "receipt_path": str(result.receipt.path),
        "receipt_sha256": result.receipt.sha256,
        "returncode": result.returncode,
        "outcome": report.get("status"),
        "intent_state": (
            None if intent is None else _object(intent, "paper intent").get("state")
        ),
        "position_after": report.get("position_after"),
        "fill": report.get("fill"),
        "cost": report.get("cost"),
        "read_touched": endpoints.get("read_touched"),
        "write_touched": endpoints.get("write_touched"),
    }


def _run_report_process(
    command: Sequence[str],
    *,
    report_option: str,
    report_path: Path,
    cwd: Path,
    pinned_directories: Sequence[tuple[Path, str]],
    guarded_files: Sequence[FileIdentity],
    repository_checks: Sequence[
        tuple[Path, str, ExecutionIdentity, bool]
    ],
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """让执行器写唯一临时报告，再以原子新建封存到规范路径。"""
    if _entry_exists(report_path):
        raise ValueError(f"执行前报告已存在: {report_path}")
    stage_path = _managed_file_path(
        report_path.parent,
        f".{report_path.name}.{os.getpid()}.{uuid.uuid4().hex}.stage",
        "执行报告临时文件",
    )
    parts = list(command)
    try:
        option_index = parts.index(report_option)
        parts[option_index + 1] = str(stage_path)
    except (ValueError, IndexError) as exc:
        raise ValueError(f"执行命令缺少 {report_option}") from exc
    report_durable = False
    try:
        result = _run_pinned(
            parts,
            cwd=cwd,
            directories=pinned_directories,
            guarded_files=guarded_files,
            repository_checks=repository_checks,
            env=env,
        )
        if _entry_exists(stage_path):
            body = _stable_file_bytes(stage_path, "执行器阶段报告")
        else:
            body = (json.dumps({
                "runner_quarantine": "executor_did_not_write_report",
                "returncode": result.returncode,
            }, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        _atomic_publish_new(report_path, body, allow_existing_identical=False)
        report_durable = True
        return result
    finally:
        body_failed = sys.exc_info()[0] is not None
        try:
            with _pin_directory_chain(report_path.parent, "执行报告父目录"):
                stage_path.unlink(missing_ok=True)
        except OSError:
            if not report_durable and not body_failed:
                raise


def run_paper_step(
    execution: Path,
    runtime: Path,
    code_root: Path,
    environment_identity: EnvironmentIdentity,
    prediction_snapshot: FileIdentity,
    source_origin: FileIdentity,
    prediction_id: str,
    plan_id: str,
    config_origin: FileIdentity,
    config_snapshot: FileIdentity,
    runner_python: FileIdentity,
    git_identity: FileIdentity,
    expectation: TargetExpectation,
    code_identity: ExecutionIdentity,
    runtime_identity: ExecutionIdentity,
    execution_identity: ExecutionIdentity,
    runtime_tracked: Sequence[FileIdentity],
    execution_tracked: Sequence[FileIdentity],
) -> dict[str, object]:
    """运行或严格复用 paper 阶段；任何失败均不外抛。"""
    raise ValueError("paper 执行已禁用：PAPER_FILLED 成本 provenance 尚未绑定")
    plan_id = _frozen_id(plan_id, _FROZEN_PLAN_ID, "plan_id")
    prediction_id = _frozen_id(
        prediction_id, _FROZEN_PREDICTION_ID, "prediction_id",
    )
    paper: dict[str, object] = {"status": "failed"}
    paper_root = _managed_directory(execution, PAPER_ROOT, "paper 受管根")
    report_root = _managed_directory(
        execution, f"{PAPER_ROOT}/reports", "paper 报告目录",
    )
    receipt_root = _managed_directory(
        execution, f"{PAPER_ROOT}/receipts", "paper 回执目录",
    )
    paper_data_root = _managed_directory(
        execution, PAPER_LEDGER_ROOT, "paper 数据根",
    )
    paper_ledger_root = _managed_directory(
        execution,
        f"{PAPER_LEDGER_ROOT}/execution/paper",
        "paper 台账目录",
    )
    lock_root = _managed_directory(
        execution, f"{SHADOW_ROOT}/locks", "shadow 阶段锁目录",
    )
    report_path = _managed_file_path(
        report_root, f"{prediction_id}.json", "paper 报告",
    )
    receipt_path = _managed_file_path(
        receipt_root, f"{prediction_id}.json", "paper 回执",
    )
    lock_path = _managed_file_path(
        lock_root, f"paper-{prediction_id}.lock", "paper 阶段锁",
    )
    paper["report_path"] = str(report_path)
    paper["receipt_path"] = str(receipt_path)
    try:
        lock_state = StageLockState()
        with _exclusive_stage_lock(lock_path, "paper 阶段锁", lock_state):
            committed = _load_committed_stage(
                stage=PAPER_MODE,
                receipt_path=receipt_path,
                report_path=report_path,
                plan_id=plan_id,
                prediction_id=prediction_id,
                source_origin=source_origin,
                source_snapshot=prediction_snapshot,
                config_origin=config_origin,
                config_snapshot=config_snapshot,
                runner_python=runner_python,
                git_identity=git_identity,
                expectation=expectation,
                environment_identity=environment_identity,
                code_identity=code_identity,
                runtime_identity=runtime_identity,
                execution_identity=execution_identity,
                runtime_tracked=runtime_tracked,
                execution_tracked=execution_tracked,
                code_root=code_root,
                runtime=runtime,
                execution=execution,
                dry_ledger_path=None,
                verify_origin_current=True,
                transaction_state=lock_state,
            )
            if committed is not None:
                lock_state.committed = True
                return _paper_summary(committed)
            target = _adapt_target(
                execution,
                environment_identity,
                prediction_snapshot,
                config_snapshot,
                expectation,
                execution_identity,
                execution_tracked,
                market_id=expectation.market_id,
                symbol=expectation.symbol,
                mode=PAPER_MODE,
                budget_jpy=None,
            )
            paper["target_path"] = str(target.path)
            paper["target_sha256"] = target.sha256
            for filename in (
                "intent_ledger.jsonl",
                "positions.jsonl",
                "difference_ledger.jsonl",
                "prediction_claims.jsonl",
                "taker_fee_cache.json",
            ):
                _validate_optional_single_file(
                    _managed_file_path(
                        paper_ledger_root, filename, f"paper {filename}",
                    ),
                    f"paper {filename}",
                )
            _execution_environment_identity(execution, environment_identity)
            _execution_identity(execution, execution_identity)
            result = _run_report_process(
                _isolated_python_command(
                    environment_identity,
                    execution / "src",
                    execution / "scripts/run_paper_executor.py",
                    (
                    "--target", str(target.path),
                    "--source-prediction", str(prediction_snapshot.path),
                    "--source-prediction-sha256", prediction_snapshot.sha256,
                    "--config", str(config_snapshot.path),
                    "--env-file", str(environment_identity.empty_child_env.path),
                    "--ledger-root", str(paper_data_root),
                    "--report", str(report_path),
                    ),
                ),
                report_option="--report",
                report_path=report_path,
                cwd=execution,
                pinned_directories=(
                    (report_root, "paper 报告目录"),
                    (paper_root, "paper 受管根"),
                    (paper_ledger_root, "paper 台账目录"),
                    (target.path.parent, "paper 目标目录"),
                    (prediction_snapshot.path.parent, "paper 来源目录"),
                    (config_snapshot.path.parent, "paper 配置快照目录"),
                    (
                        environment_identity.python.path.parent,
                        "paper 执行 Python 目录",
                    ),
                    (
                        environment_identity.pyvenv_config.path.parent,
                        "paper pyvenv 配置目录",
                    ),
                ),
                guarded_files=(
                    prediction_snapshot,
                    target,
                    config_snapshot,
                    *_environment_guarded_files(environment_identity),
                    *execution_tracked,
                ),
                repository_checks=((
                    execution, "执行仓", execution_identity, False,
                ),),
                # paper 成本来源未绑定，路径不可达
                env=_business_child_environment(PAPER_MODE),
            )
            paper["returncode"] = result.returncode
            _stable_identity(
                prediction_snapshot.path,
                "paper 执行后来源快照",
                prediction_snapshot.sha256,
            )
            _stable_identity(target.path, "paper 执行后目标", target.sha256)
            _stable_identity(
                config_origin.path,
                "paper 执行后配置原件",
                config_origin.sha256,
            )
            _stable_identity(
                config_snapshot.path,
                "paper 执行后配置快照",
                config_snapshot.sha256,
            )
            _execution_environment_identity(execution, environment_identity)
            _execution_identity(execution, execution_identity)
            for filename in (
                "intent_ledger.jsonl",
                "positions.jsonl",
                "difference_ledger.jsonl",
                "prediction_claims.jsonl",
                "taker_fee_cache.json",
            ):
                _validate_optional_single_file(
                    _managed_file_path(
                        paper_ledger_root, filename, f"paper {filename}",
                    ),
                    f"paper {filename}",
                )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout).strip()
                raise RuntimeError(f"paper 执行失败({result.returncode}): {detail}")
            report_body = _stable_file_bytes(report_path, "paper report")
            _validate_paper_report_bytes(
                report_body, target, expectation, execution,
            )
            committed = _commit_stage(
                stage=PAPER_MODE,
                receipt_path=receipt_path,
                report_path=report_path,
                plan_id=plan_id,
                prediction_id=prediction_id,
                source_origin=source_origin,
                source_snapshot=prediction_snapshot,
                target=target,
                config_origin=config_origin,
                config_snapshot=config_snapshot,
                runner_python=runner_python,
                git_identity=git_identity,
                expectation=expectation,
                environment_identity=environment_identity,
                code_identity=code_identity,
                runtime_identity=runtime_identity,
                execution_identity=execution_identity,
                runtime_tracked=runtime_tracked,
                execution_tracked=execution_tracked,
                code_root=code_root,
                runtime=runtime,
                execution=execution,
                dry_ledger_path=None,
                returncode=result.returncode,
                transaction_state=lock_state,
            )
            lock_state.committed = True
            return _paper_summary(committed)
    except Exception as exc:
        paper["status"] = "failed"
        paper["error"] = f"{type(exc).__name__}: {exc}"
        return paper


def _runner_code_root() -> Path:
    return _absolute_canonical_path(
        Path(__file__).resolve().parents[1], "runner 代码根",
    )


def _current_python_executable() -> Path:
    return Path(sys.executable)


def run_shadow(
    repository: Path,
    runtime_root: Path,
    execution_repository: Path,
    plan_id: str,
    market_id: str,
    *,
    code_root: Path,
    expected_code_head: str,
    python_executable: Path,
    expected_python_sha256: str,
    git_executable: Path,
    expected_git_sha256: str,
    expected_execution_environment_tree_sha256: str,
    symbol: str = "BTC",
    budget_jpy: str = "500",
    max_prediction_age_minutes: int = DEFAULT_MAX_PREDICTION_AGE_MINUTES,
    paper_enabled: bool = False,
) -> dict[str, object]:
    """在任何 Git 探测前固定绝对 Git 执行文件。"""
    if paper_enabled:
        raise ValueError("paper 执行已禁用：PAPER_FILLED 成本 provenance 尚未绑定")
    _preflight_receipt_schema(execution_repository)
    plan_id = _frozen_id(plan_id, _FROZEN_PLAN_ID, "plan_id")
    git_identity = _stable_identity(
        git_executable,
        "Git 执行文件",
        _digest(expected_git_sha256, "expected_git_sha256"),
        allow_hardlinks=True,
    )
    with (
        _use_git_executable(git_identity),
        _guard_file_identities((git_identity,), "Git 全程执行身份"),
    ):
        return _run_shadow_with_git(
            repository,
            runtime_root,
            execution_repository,
            plan_id,
            market_id,
            code_root=code_root,
            expected_code_head=expected_code_head,
            python_executable=python_executable,
            expected_python_sha256=expected_python_sha256,
            expected_execution_environment_tree_sha256=(
                expected_execution_environment_tree_sha256
            ),
            symbol=symbol,
            budget_jpy=budget_jpy,
            max_prediction_age_minutes=max_prediction_age_minutes,
            paper_enabled=paper_enabled,
        )


def _run_shadow_with_git(
    repository: Path,
    runtime_root: Path,
    execution_repository: Path,
    plan_id: str,
    market_id: str,
    *,
    code_root: Path,
    expected_code_head: str,
    python_executable: Path,
    expected_python_sha256: str,
    expected_execution_environment_tree_sha256: str,
    symbol: str = "BTC",
    budget_jpy: str = "500",
    max_prediction_age_minutes: int = DEFAULT_MAX_PREDICTION_AGE_MINUTES,
    paper_enabled: bool = False,
) -> dict[str, object]:
    """先固定 detached runner 代码树，再进入业务串联。"""
    if paper_enabled:
        raise ValueError("paper 执行已禁用：PAPER_FILLED 成本 provenance 尚未绑定")
    plan_id = _frozen_id(plan_id, _FROZEN_PLAN_ID, "plan_id")
    git_identity = _PINNED_GIT.get()
    if git_identity is None:
        raise RuntimeError("Git 执行文件尚未绑定")
    declared_code_root = _absolute_canonical_path(code_root, "runner 代码根")
    actual_code_root = _runner_code_root()
    if not _same_path(declared_code_root, actual_code_root):
        raise ValueError("runner 代码根与已加载脚本不符")
    expected_head = _text(expected_code_head, "expected_code_head")
    if re.fullmatch(r"[0-9a-f]{40,64}", expected_head) is None:
        raise ValueError("expected_code_head 非法")
    code_identity = _repository_identity(
        declared_code_root,
        "runner 代码仓",
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    if code_identity.head_commit != expected_head:
        raise ValueError("runner 代码仓 HEAD 与预期不符")
    actual_python = _absolute_canonical_path(
        _current_python_executable(), "runner Python",
    )
    declared_python = _absolute_canonical_path(
        python_executable, "runner Python",
    )
    if not _same_path(actual_python, declared_python):
        raise ValueError("runner Python 与当前解释器不符")
    runner_python = _stable_identity(
        actual_python,
        "runner Python",
        _digest(expected_python_sha256, "expected_python_sha256"),
        allow_hardlinks=True,
    )
    source_root = _absolute_canonical_path(repository, "live 数据根")
    runtime = _execution_root(runtime_root)
    execution = _execution_root(execution_repository)
    if _same_path(source_root, declared_code_root):
        raise ValueError("runner CodeRoot 不得与 live DataRoot 相同")
    runtime_identity = _repository_identity(
        runtime,
        "冻结运行仓",
        require_detached=True,
        reject_untracked_scopes=("src", "scripts"),
    )
    execution_identity = _execution_identity(execution)
    code_tracked = _git_tracked_identities(
        declared_code_root, "runner 代码仓",
    )
    runtime_tracked = _git_tracked_identities(runtime, "冻结运行仓")
    execution_tracked = _git_tracked_identities(execution, "执行仓")
    with _guard_file_identities(
        (*code_tracked, runner_python), "runner 全程代码",
    ):
        result = _run_shadow_guarded(
            source_root,
            runtime,
            execution,
            plan_id,
            market_id,
            code_root=declared_code_root,
            code_identity=code_identity,
            code_tracked=code_tracked,
            runtime_identity=runtime_identity,
            execution_identity=execution_identity,
            runtime_tracked=runtime_tracked,
            execution_tracked=execution_tracked,
            runner_python=runner_python,
            git_identity=git_identity,
            expected_execution_environment_tree_sha256=(
                expected_execution_environment_tree_sha256
            ),
            symbol=symbol,
            budget_jpy=budget_jpy,
            max_prediction_age_minutes=max_prediction_age_minutes,
            paper_enabled=paper_enabled,
        )
        _repository_identity(
            declared_code_root,
            "runner 代码仓",
            code_identity,
            require_detached=True,
            reject_untracked_scopes=("src", "scripts"),
        )
        _revalidate_identity(runner_python, "runner Python")
        return result


def _run_shadow_guarded(
    source_root: Path,
    runtime: Path,
    execution: Path,
    plan_id: str,
    market_id: str,
    *,
    code_root: Path,
    code_identity: ExecutionIdentity,
    code_tracked: Sequence[FileIdentity],
    runtime_identity: ExecutionIdentity,
    execution_identity: ExecutionIdentity,
    runtime_tracked: Sequence[FileIdentity],
    execution_tracked: Sequence[FileIdentity],
    runner_python: FileIdentity,
    git_identity: FileIdentity,
    expected_execution_environment_tree_sha256: str,
    symbol: str,
    budget_jpy: str,
    max_prediction_age_minutes: int,
    paper_enabled: bool,
) -> dict[str, object]:
    """串联快照、冻结预测、内容寻址执行目标与两阶段成功回执。"""
    if paper_enabled:
        raise ValueError("paper 执行已禁用：PAPER_FILLED 成本 provenance 尚未绑定")
    started = datetime.now(UTC)
    # 路径构造前先校验标识
    plan_id = _frozen_id(plan_id, _FROZEN_PLAN_ID, "plan_id")
    shadow_root = _managed_directory(execution, SHADOW_ROOT, "shadow 受管根")
    report_root = _managed_directory(
        execution, f"{SHADOW_ROOT}/reports", "shadow 报告目录",
    )
    receipt_root = _managed_directory(
        execution, f"{SHADOW_ROOT}/receipts", "shadow 回执目录",
    )
    lock_root = _managed_directory(
        execution, f"{SHADOW_ROOT}/locks", "shadow 阶段锁目录",
    )
    task_log = _managed_file_path(shadow_root, "task.jsonl", "shadow 任务日志")
    try:
        _repository_identity(
            code_root,
            "runner 代码仓",
            code_identity,
            require_detached=True,
            reject_untracked_scopes=("src", "scripts"),
        )
        _repository_identity(
            runtime,
            "冻结运行仓",
            runtime_identity,
            require_detached=True,
            reject_untracked_scopes=("src", "scripts"),
        )
        _execution_identity(execution, execution_identity)
        config_path = _absolute_canonical_path(
            execution / PAPER_CONFIG, "paper 执行配置",
        )
        config_origin, config_snapshot, config_payload = _capture_config(
            execution, config_path,
        )
        config_contract = _paper_config_contract(
            config_payload, market_id=market_id, symbol=symbol,
        )
        environment_identity = _execution_environment_identity(execution)
        expected_environment_tree = _digest(
            expected_execution_environment_tree_sha256,
            "expected_execution_environment_tree_sha256",
        )
        if environment_identity.tree_sha256 != expected_environment_tree:
            raise ValueError("执行 venv 清单与注册值不符")
        environment_identity = _attach_import_closures(
            execution,
            environment_identity,
            code_root=code_root,
            code_tracked=code_tracked,
            runtime=runtime,
            runtime_tracked=runtime_tracked,
            execution_tracked=execution_tracked,
        )
        refresh = _run_refresh_process(
            source_root,
            runtime,
            code_root,
            environment_identity,
            code_identity,
            code_tracked,
            market_id,
        )
        _repository_identity(
            runtime,
            "冻结运行仓",
            runtime_identity,
            require_detached=True,
            reject_untracked_scopes=("src", "scripts"),
        )
        prediction = _json_stdout(_run_pinned(
            _isolated_python_command(
                environment_identity,
                runtime / "src",
                runtime / "scripts/manage_frozen_forward.py",
                (
                "--root", str(runtime), "predict", plan_id,
                "--registry", str(runtime / "data/research/governance.sqlite3"),
                ),
            ),
            cwd=runtime,
            directories=(
                (runtime, "冻结运行根"),
                (
                    environment_identity.python.path.parent,
                    "predictor 执行 Python 目录",
                ),
                (
                    environment_identity.pycache_sentinel.path.parent,
                    "predictor Python 缓存目录",
                ),
            ),
            guarded_files=(
                *runtime_tracked,
                *_environment_guarded_files(environment_identity),
            ),
            repository_checks=((
                runtime, "冻结运行仓", runtime_identity, True,
            ),),
            env=_business_child_environment(),
        ), "frozen prediction")
        prediction_id = _frozen_id(
            prediction.get("prediction_id"),
            _FROZEN_PREDICTION_ID,
            "prediction_id",
        )
        capture_lock = _managed_file_path(
            lock_root,
            f"capture-{prediction_id}.lock",
            "冻结来源捕获锁",
        )
        capture_state = StageLockState()
        with _exclusive_stage_lock(
            capture_lock, "冻结来源捕获锁", capture_state,
        ):
            source_origin, source_snapshot, official = _capture_prediction(
                execution, runtime, prediction, plan_id,
            )
            capture_state.committed = True
        decision_time = _timestamp(official.get("decision_time"), "decision_time")
        age = datetime.now(UTC) - decision_time
        if age < timedelta(0) or age > timedelta(minutes=max_prediction_age_minutes):
            raise ValueError(f"冻结预测过期: {age.total_seconds():.1f}s")
        dry_expectation = _target_expectation(
            official,
            source_snapshot,
            config_contract,
            plan_id=plan_id,
            market_id=market_id,
            symbol=symbol,
            mode=DRY_RUN_MODE,
            budget_jpy=budget_jpy,
        )
        paper_expectation = _target_expectation(
            official,
            source_snapshot,
            config_contract,
            plan_id=plan_id,
            market_id=market_id,
            symbol=symbol,
            mode=PAPER_MODE,
            budget_jpy=None,
        )

        report_path = _managed_file_path(
            report_root, f"{prediction_id}.json", "shadow 报告",
        )
        receipt_path = _managed_file_path(
            receipt_root, f"{prediction_id}.json", "shadow 回执",
        )
        ledger_path = _managed_file_path(
            shadow_root, "intent_ledger.jsonl", "shadow 意图账",
        )
        lock_path = _managed_file_path(
            lock_root, f"dry-run-{prediction_id}.lock", "dry-run 阶段锁",
        )
        lock_state = StageLockState()
        with _exclusive_stage_lock(lock_path, "dry-run 阶段锁", lock_state):
            stage = _load_committed_stage(
                stage=DRY_RUN_MODE,
                receipt_path=receipt_path,
                report_path=report_path,
                plan_id=plan_id,
                prediction_id=prediction_id,
                source_origin=source_origin,
                source_snapshot=source_snapshot,
                config_origin=config_origin,
                config_snapshot=config_snapshot,
                runner_python=runner_python,
                git_identity=git_identity,
                expectation=dry_expectation,
                environment_identity=environment_identity,
                code_identity=code_identity,
                runtime_identity=runtime_identity,
                execution_identity=execution_identity,
                runtime_tracked=runtime_tracked,
                execution_tracked=execution_tracked,
                code_root=code_root,
                runtime=runtime,
                execution=execution,
                dry_ledger_path=ledger_path,
                verify_origin_current=True,
                transaction_state=lock_state,
            )
            if stage is None:
                target = _adapt_target(
                    execution,
                    environment_identity,
                    source_snapshot,
                    config_snapshot,
                    dry_expectation,
                    execution_identity,
                    execution_tracked,
                    market_id=market_id,
                    symbol=symbol,
                    mode=DRY_RUN_MODE,
                    budget_jpy=budget_jpy,
                )
                # shadow 子进程固定模拟模式
                dry_run_env = _business_child_environment(DRY_RUN_MODE)
                _validate_optional_single_file(ledger_path, "shadow 意图账")
                _execution_environment_identity(
                    execution, environment_identity,
                )
                _execution_identity(execution, execution_identity)
                result = _run_report_process(
                    _isolated_python_command(
                        environment_identity,
                        execution / "src",
                        execution / "scripts/run_dry_run_executor.py",
                        (
                        "--target", str(target.path), "--symbol", symbol,
                        "--source-prediction", str(source_snapshot.path),
                        "--source-prediction-sha256", source_snapshot.sha256,
                        "--budget-jpy", budget_jpy, "--ledger", str(ledger_path),
                        "--env-file", str(
                            environment_identity.empty_child_env.path
                        ),
                        "--dry-run-report", str(report_path),
                        ),
                    ),
                    report_option="--dry-run-report",
                    report_path=report_path,
                    cwd=execution,
                    pinned_directories=(
                        (report_root, "shadow 报告目录"),
                        (shadow_root, "shadow 受管根"),
                        (target.path.parent, "dry-run 目标目录"),
                        (source_snapshot.path.parent, "dry-run 来源目录"),
                        (
                            config_snapshot.path.parent,
                            "dry-run 配置快照目录",
                        ),
                        (
                            environment_identity.python.path.parent,
                            "dry-run 执行 Python 目录",
                        ),
                        (
                            environment_identity.pyvenv_config.path.parent,
                            "dry-run pyvenv 配置目录",
                        ),
                    ),
                    guarded_files=(
                        source_snapshot,
                        target,
                        config_snapshot,
                        *_environment_guarded_files(environment_identity),
                        *execution_tracked,
                    ),
                    repository_checks=((
                        execution, "执行仓", execution_identity, False,
                    ),),
                    env=dry_run_env,
                )
                _stable_identity(
                    source_snapshot.path,
                    "dry-run 执行后来源快照",
                    source_snapshot.sha256,
                )
                _stable_identity(target.path, "dry-run 执行后目标", target.sha256)
                _stable_identity(
                    config_origin.path,
                    "dry-run 执行后配置原件",
                    config_origin.sha256,
                )
                _stable_identity(
                    config_snapshot.path,
                    "dry-run 执行后配置快照",
                    config_snapshot.sha256,
                )
                _execution_environment_identity(execution, environment_identity)
                _execution_identity(execution, execution_identity)
                _validate_optional_single_file(ledger_path, "shadow 意图账")
                if result.returncode != 0:
                    detail = (result.stderr or result.stdout).strip()
                    raise RuntimeError(f"dry-run 失败({result.returncode}): {detail}")
                stage = _commit_stage(
                    stage=DRY_RUN_MODE,
                    receipt_path=receipt_path,
                    report_path=report_path,
                    plan_id=plan_id,
                    prediction_id=prediction_id,
                    source_origin=source_origin,
                    source_snapshot=source_snapshot,
                    target=target,
                    config_origin=config_origin,
                    config_snapshot=config_snapshot,
                    runner_python=runner_python,
                    git_identity=git_identity,
                    expectation=dry_expectation,
                    environment_identity=environment_identity,
                    code_identity=code_identity,
                    runtime_identity=runtime_identity,
                    execution_identity=execution_identity,
                    runtime_tracked=runtime_tracked,
                    execution_tracked=execution_tracked,
                    code_root=code_root,
                    runtime=runtime,
                    execution=execution,
                    dry_ledger_path=ledger_path,
                    returncode=result.returncode,
                    transaction_state=lock_state,
                )
            lock_state.committed = True
        if paper_enabled:
            paper = run_paper_step(
                execution, runtime, code_root, environment_identity,
                source_snapshot, source_origin, prediction_id, plan_id,
                config_origin, config_snapshot, runner_python,
                git_identity,
                paper_expectation,
                code_identity, runtime_identity, execution_identity,
                runtime_tracked, execution_tracked,
            )
        else:
            paper = {"status": "skipped", "reason": "--no-paper"}
        report = stage.report_payload
        paper_failed = paper.get("status") == "failed"
        summary: dict[str, object] = {
            "status": (
                "failed"
                if paper_failed
                else ("reused" if stage.reused else "completed")
            ),
            "started_at": started.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "plan_id": plan_id,
            "prediction_id": prediction_id,
            "source_prediction_sha256": source_snapshot.sha256,
            "source_prediction_origin_path": str(source_origin.path),
            "source_prediction_origin_sha256": source_origin.sha256,
            "source_prediction_snapshot_path": str(source_snapshot.path),
            "source_prediction_snapshot_sha256": source_snapshot.sha256,
            "decision_time": decision_time.isoformat(),
            "prediction_age_seconds": round(age.total_seconds(), 3),
            "aggregate_target": official.get("aggregate_target"),
            "target_path": str(stage.target.path),
            "target_sha256": stage.target.sha256,
            "report_path": str(stage.report.path),
            "report_sha256": stage.report.sha256,
            "receipt_path": str(stage.receipt.path),
            "receipt_sha256": stage.receipt.sha256,
            "intent_state": (
                None if report.get("intent") is None
                else _object(report["intent"], "intent").get("state")
            ),
            "write_touched": _object(
                report["endpoints"], "endpoints",
            ).get("write_touched"),
            "paper": paper,
            "execution_environment_attestation": (
                environment_identity.attestation
            ),
            "execution_environment_guard_strength": (
                environment_identity.guard_strength
            ),
            "code_repository_head": code_identity.head_commit,
            "runtime_repository_head": runtime_identity.head_commit,
            "execution_repository_head": execution_identity.head_commit,
            "refresh": refresh,
        }
        _repository_identity(
            code_root,
            "runner 代码仓",
            code_identity,
            require_detached=True,
            reject_untracked_scopes=("src", "scripts"),
        )
        _repository_identity(
            runtime,
            "冻结运行仓",
            runtime_identity,
            require_detached=True,
            reject_untracked_scopes=("src", "scripts"),
        )
        _execution_identity(execution, execution_identity)
        _execution_environment_identity(execution, environment_identity)
        summary["task_log_status"] = "written"
        try:
            _append_record(task_log, summary)
        except BaseException as log_exc:
            # 日志失败不诱导重跑
            summary["task_log_status"] = "failed"
            summary["task_log_error"] = (
                f"{type(log_exc).__name__}: {log_exc}"
            )
        _repository_identity(
            runtime,
            "冻结运行仓",
            runtime_identity,
            require_detached=True,
            reject_untracked_scopes=("src", "scripts"),
        )
        _execution_identity(execution, execution_identity)
        _execution_environment_identity(execution, environment_identity)
        return summary
    except BaseException as exc:
        try:
            _append_record(task_log, {
                "status": "failed", "started_at": started.isoformat(),
                "completed_at": datetime.now(UTC).isoformat(),
                "plan_id": plan_id,
                "error": f"{type(exc).__name__}: {exc}",
            })
        except BaseException:
            pass
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="冻结目标每小时 shadow 串联")
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--execution-repository", type=Path, required=True)
    parser.add_argument("--code-root", type=Path, required=True)
    parser.add_argument("--expected-code-head", required=True)
    parser.add_argument("--python-executable", type=Path, required=True)
    parser.add_argument("--expected-python-sha256", required=True)
    parser.add_argument("--git-executable", type=Path, required=True)
    parser.add_argument("--expected-git-sha256", required=True)
    parser.add_argument(
        "--expected-execution-environment-tree-sha256", required=True,
    )
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--market-id", default="mkt__gmo__btc__r0")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--budget-jpy", default="500")
    parser.add_argument(
        "--max-prediction-age-minutes", type=int,
        default=DEFAULT_MAX_PREDICTION_AGE_MINUTES,
        help="进入执行适配前允许的最大预测年龄；缺省 45 分钟，预留过期缓冲",
    )
    parser.add_argument(
        "--no-paper", action="store_true", help="跳过 paper 执行步骤",
    )
    args = parser.parse_args(argv)
    if not bool(args.no_paper):
        parser.error(
            "paper fill 成本 provenance 尚未绑定；必须显式传入 --no-paper",
        )
    summary = run_shadow(
        args.repository, args.runtime_root, args.execution_repository,
        str(args.plan_id), str(args.market_id),
        code_root=args.code_root,
        expected_code_head=str(args.expected_code_head),
        python_executable=args.python_executable,
        expected_python_sha256=str(args.expected_python_sha256),
        git_executable=args.git_executable,
        expected_git_sha256=str(args.expected_git_sha256),
        expected_execution_environment_tree_sha256=str(
            args.expected_execution_environment_tree_sha256,
        ),
        symbol=str(args.symbol),
        budget_jpy=str(args.budget_jpy),
        max_prediction_age_minutes=int(args.max_prediction_age_minutes),
        paper_enabled=False,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    if summary.get("status") == "failed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
