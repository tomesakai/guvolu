"""Research-only 经济研究代理与时点正确的宏观语境制品。

本模块只管理观测、语境和搜索提案。它不联网、不读取密钥、不修改
策略配置或候选注册表，也没有任何执行或 TRADE 权限。
"""
from __future__ import annotations

import ctypes
import importlib
import json
import math
import os
import re
import secrets
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Protocol, cast

from guvolu.research import clock
from guvolu.research.provenance import (
    canonical_json,
    sha256_text,
    stable_identifier,
)

ECONOMIC_OBSERVATION_SCHEMA_VERSION = 1
ECONOMIC_CONTEXT_SCHEMA_VERSION = 1
ECONOMIC_PROPOSAL_SCHEMA_VERSION = 1
ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION = 2
ECONOMIC_CONTEXT_METHOD_VERSION = "economic-context-v1"
ECONOMIC_PROPOSAL_METHOD_VERSION = "economic-search-plan-proposal-v1"
ECONOMIC_AGENT_METHOD_VERSION = "economic-research-agent-v1-embedded-ledger"

DIMENSIONS = ("growth", "inflation", "rates", "liquidity", "fx", "risk")
_REGIME_LABELS: Mapping[str, tuple[str, str, str]] = {
    "growth": ("strong", "weak", "balanced"),
    "inflation": ("hot", "cool", "balanced"),
    "rates": ("restrictive", "accommodative", "balanced"),
    "liquidity": ("abundant", "scarce", "balanced"),
    "fx": ("supportive", "adverse", "balanced"),
    "risk": ("risk_on", "risk_off", "balanced"),
}
_IDENTIFIER = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ZERO_HASH = "0" * 64
_SOURCE_RECEIPT_KEYS = frozenset({"source_id", "receipt_sha256", "locator"})


@dataclass(frozen=True)
class _DirectoryIdentity:
    """A lexical directory component bound to its resolved path and file ID."""

    lexical: str
    resolved: str
    device: int
    inode: int
    mode: int
    file_attributes: int
    reparse_tag: int


def _path_race_hook(_phase: str, _path: Path) -> None:
    """Internal no-op seam used to exercise check/use races deterministically."""


def _absolute_lexical_path(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{name} 必须使用绝对规范路径")
    normalized_text = os.path.normpath(str(path))
    if os.path.normcase(str(path)) != os.path.normcase(normalized_text):
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
    except OSError as error:
        raise ValueError(f"{name} 不存在或无法读取身份: {lexical}") from error
    attributes = int(getattr(metadata, "st_file_attributes", 0))
    reparse_tag = int(getattr(metadata, "st_reparse_tag", 0))
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or attributes & reparse_flag
        or reparse_tag
    ):
        raise ValueError(f"{name} 不得是 symlink、junction 或其他 reparse point")
    if os.path.normcase(str(lexical)) != os.path.normcase(str(resolved)):
        raise ValueError(f"{name} 不得使用非规范路径或目录别名")
    return _DirectoryIdentity(
        lexical=str(lexical),
        resolved=str(resolved),
        device=int(metadata.st_dev),
        inode=int(metadata.st_ino),
        mode=int(metadata.st_mode),
        file_attributes=attributes,
        reparse_tag=reparse_tag,
    )


def _capture_directory_chain(
    directory: Path,
    name: str,
) -> tuple[_DirectoryIdentity, ...]:
    """Bind every existing lexical component, including Windows reparse metadata."""
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
        raise ValueError(f"{name} 目录身份在持锁操作期间发生变化")


class _FcntlModule(Protocol):
    LOCK_EX: int
    LOCK_UN: int

    def flock(self, fd: int, operation: int) -> None: ...


class _WindowsFileTime(ctypes.Structure):
    _fields_ = (
        ("low", ctypes.c_uint32),
        ("high", ctypes.c_uint32),
    )


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


@dataclass(frozen=True)
class _PinnedDirectory:
    """An opened direct parent plus every no-delete Windows ancestor handle."""

    path: Path
    identity: tuple[_DirectoryIdentity, ...]
    descriptor: int | None
    windows_handles: tuple[int, ...]


@dataclass
class _LedgerTransactionState:
    """Share the durable commit point with every enclosing cleanup layer."""

    committed: bool = False


def _cleanup_transaction_resource(
    action: Callable[[], None],
    *,
    state: _LedgerTransactionState | None,
    body_failed: bool,
    phase: str,
    path: Path,
) -> None:
    """Run cleanup without turning a durable commit into a retry signal."""
    try:
        action()
        _path_race_hook(phase, path)
    except OSError:
        if not body_failed and not (state is not None and state.committed):
            raise


def _windows_kernel32() -> Any:
    loader = cast(Any, getattr(ctypes, "WinDLL"))
    return loader("kernel32", use_last_error=True)


def _windows_open_directory_handle(
    path: Path,
    expected: _DirectoryIdentity,
) -> int:
    """Open a directory without FILE_SHARE_DELETE and bind its file index."""
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
    # 只共享读写，不共享删除。
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
        _windows_discard_handle_after_failure(handle)
        raise OSError(error, f"无法读取目录句柄身份: {path}")
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    directory_flag = int(getattr(stat, "FILE_ATTRIBUTE_DIRECTORY", 0x10))
    if (
        information.file_index != expected.inode
        or not int(information.file_attributes) & directory_flag
        or int(information.file_attributes) & reparse_flag
    ):
        _windows_discard_handle_after_failure(handle)
        raise ValueError(f"目录句柄与已验证路径身份不一致: {path}")
    return handle


def _windows_close_handle(handle: int) -> None:
    kernel = _windows_kernel32()
    close_handle = kernel.CloseHandle
    close_handle.argtypes = (ctypes.c_void_p,)
    close_handle.restype = ctypes.c_int
    if not close_handle(ctypes.c_void_p(handle)):
        error = ctypes.get_last_error()
        raise OSError(error, "Windows 句柄关闭失败")


def _windows_discard_handle_after_failure(handle: int) -> None:
    try:
        _windows_close_handle(handle)
    except OSError:
        pass


@contextmanager
def _pin_directory_chain(
    directory: Path,
    name: str,
    *,
    transaction_state: _LedgerTransactionState | None = None,
) -> Iterator[_PinnedDirectory]:
    """Open and identity-bind a directory chain before any path-based mutation."""
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
        yield _PinnedDirectory(
            lexical,
            identity,
            descriptor,
            tuple(handles),
        )
    finally:
        body_failed = sys.exc_info()[0] is not None
        cleanup_error: OSError | None = None
        if descriptor is not None:
            try:
                _cleanup_transaction_resource(
                    lambda: os.close(descriptor),
                    state=transaction_state,
                    body_failed=body_failed,
                    phase="ledger-parent-close-after-effect",
                    path=lexical,
                )
            except OSError as error:
                cleanup_error = error
        for handle in reversed(handles):
            try:
                _cleanup_transaction_resource(
                    lambda: _windows_close_handle(handle),
                    state=transaction_state,
                    body_failed=body_failed or cleanup_error is not None,
                    phase="ledger-parent-close-after-effect",
                    path=lexical,
                )
            except OSError as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None:
            raise cleanup_error


def _ensure_canonical_directory_tree(
    directory: Path,
    name: str,
) -> Path:
    """Create missing ancestors one at a time without trusting recursive mkdir."""
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


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} 必须为对象")
    return value


def _text(value: object, name: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{name} 必须为非空文本，且长度不超过 {maximum}")
    return value


def _identifier(value: object, name: str) -> str:
    text = _text(value, name, maximum=128)
    if _IDENTIFIER.fullmatch(text) is None:
        raise ValueError(f"{name} 不是规范标识")
    return text


def _sha256(value: object, name: str) -> str:
    text = _text(value, name, maximum=64)
    if _SHA256.fullmatch(text) is None:
        raise ValueError(f"{name} 必须为 64 位小写 SHA-256")
    return text


def _number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} 必须为有限数值")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} 必须为有限数值")
    return result


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _utc_time(value: object, name: str) -> datetime:
    text = _text(value, name, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} 不是合法 ISO-8601 时间") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} 必须携带时区")
    return parsed.astimezone(UTC)


def _time_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("时间必须携带时区")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _load_json(path: Path, name: str) -> Mapping[str, object]:
    try:
        loaded: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{name} 无法读取") from error
    return _object(loaded, name)


def _validate_exact_keys(
    value: Mapping[str, object],
    expected: frozenset[str],
    name: str,
) -> None:
    unexpected = sorted(set(value) - expected)
    missing = sorted(expected - set(value))
    if missing or unexpected:
        raise ValueError(
            f"{name} 字段不合同: missing={missing}, unexpected={unexpected}",
        )


@dataclass(frozen=True)
class EconomicObservation:
    """一条带修订链和三个源时点的经济观测。"""

    observation_id: str
    series_id: str
    value: float
    unit: str
    event_time: datetime
    available_time: datetime
    ingest_time: datetime
    revision_id: str
    supersedes_revision_id: str | None
    source_receipt: tuple[tuple[str, str], ...]

    def payload(self) -> Mapping[str, object]:
        """输出可散列的规范观测。"""
        return {
            "schema_version": ECONOMIC_OBSERVATION_SCHEMA_VERSION,
            "observation_id": self.observation_id,
            "series_id": self.series_id,
            "value": self.value,
            "unit": self.unit,
            "event_time": _time_text(self.event_time),
            "available_time": _time_text(self.available_time),
            "ingest_time": _time_text(self.ingest_time),
            "revision_id": self.revision_id,
            "supersedes_revision_id": self.supersedes_revision_id,
            "source_receipt": dict(self.source_receipt),
        }


@dataclass(frozen=True)
class EconomicObservationSnapshot:
    """绑定观测台账前缀与哈希链头的不可变快照。"""

    observations: tuple[EconomicObservation, ...]
    ledger_sequence: int
    ledger_head_sha256: str


def _source_receipt(value: object) -> tuple[tuple[str, str], ...]:
    receipt = _object(value, "source_receipt")
    unexpected = sorted(set(receipt) - _SOURCE_RECEIPT_KEYS)
    if unexpected:
        raise ValueError(f"source_receipt 含非合同字段: {unexpected}")
    source_id = _identifier(receipt.get("source_id"), "source_receipt.source_id")
    digest = _sha256(
        receipt.get("receipt_sha256"), "source_receipt.receipt_sha256",
    )
    normalized = {"source_id": source_id, "receipt_sha256": digest}
    locator = receipt.get("locator")
    if locator is not None:
        normalized["locator"] = _text(locator, "source_receipt.locator", maximum=1024)
    return tuple(sorted(normalized.items()))


def _observation_identities(
    *,
    series_id: str,
    value: float,
    unit: str,
    event_time: datetime,
    available_time: datetime,
    ingest_time: datetime,
    supersedes_revision_id: str | None,
    source_receipt: tuple[tuple[str, str], ...],
) -> tuple[str, str]:
    revision_body = {
        "series_id": series_id,
        "value": value,
        "unit": unit,
        "event_time": _time_text(event_time),
        "available_time": _time_text(available_time),
        "supersedes_revision_id": supersedes_revision_id,
        "source_receipt": dict(source_receipt),
    }
    revision_id = stable_identifier("economic-revision", revision_body)
    observation_id = stable_identifier("economic-observation", {
        **revision_body,
        "revision_id": revision_id,
        "ingest_time": _time_text(ingest_time),
    })
    return observation_id, revision_id


def parse_economic_observation(value: Mapping[str, object]) -> EconomicObservation:
    """规范化并验证一条观测；缺省标识由内容生成。"""
    schema = value.get("schema_version", ECONOMIC_OBSERVATION_SCHEMA_VERSION)
    if schema != ECONOMIC_OBSERVATION_SCHEMA_VERSION:
        raise ValueError("经济观测 schema_version 不受支持")
    series_id = _identifier(value.get("series_id"), "series_id")
    unit = _identifier(value.get("unit"), "unit")
    number = _number(value.get("value"), "value")
    event = _utc_time(value.get("event_time"), "event_time")
    available = _utc_time(value.get("available_time"), "available_time")
    ingested = _utc_time(value.get("ingest_time"), "ingest_time")
    if event > available:
        raise ValueError("available_time 不得早于 event_time")
    raw_supersedes = value.get("supersedes_revision_id")
    supersedes = (
        None
        if raw_supersedes is None
        else _text(raw_supersedes, "supersedes_revision_id", maximum=96)
    )
    receipt = _source_receipt(value.get("source_receipt"))
    expected_observation, expected_revision = _observation_identities(
        series_id=series_id,
        value=number,
        unit=unit,
        event_time=event,
        available_time=available,
        ingest_time=ingested,
        supersedes_revision_id=supersedes,
        source_receipt=receipt,
    )
    supplied_revision = value.get("revision_id")
    if supplied_revision is not None and supplied_revision != expected_revision:
        raise ValueError("revision_id 与规范内容不一致")
    supplied_observation = value.get("observation_id")
    if supplied_observation is not None and supplied_observation != expected_observation:
        raise ValueError("observation_id 与规范内容不一致")
    return EconomicObservation(
        observation_id=expected_observation,
        series_id=series_id,
        value=number,
        unit=unit,
        event_time=event,
        available_time=available,
        ingest_time=ingested,
        revision_id=expected_revision,
        supersedes_revision_id=supersedes,
        source_receipt=receipt,
    )


def _canonical_ledger_path(
    path: Path,
    name: str,
    *,
    require_file: bool,
) -> Path:
    """拒绝相对、符号链接、``..`` 与硬链接台账别名。"""
    lexical = _absolute_lexical_path(path, name)
    if not require_file:
        _ensure_canonical_directory_tree(lexical.parent, f"{name} 父目录")
    else:
        _capture_directory_chain(lexical.parent, f"{name} 父目录")
    resolved = path.resolve(strict=False)
    if os.path.normcase(str(lexical)) != os.path.normcase(str(resolved)):
        raise ValueError(f"{name} 不得使用非规范路径或符号链接别名")
    if resolved.exists():
        if not resolved.is_file():
            raise ValueError(f"{name} 不是普通文件")
        try:
            link_count = resolved.stat().st_nlink
        except OSError as error:
            raise ValueError(f"{name} 无法读取文件身份") from error
        if link_count != 1:
            raise ValueError(f"{name} 不得使用硬链接台账")
    elif require_file:
        raise ValueError(f"{name} 不存在: {resolved}")
    return resolved


@dataclass
class _PendingLedgerAppend:
    """One inode-bound append that remains reversible until lock exit."""

    descriptor: int
    created: bool
    device: int
    inode: int
    original_body: bytes
    committed_body: bytes
    record_type: str


@dataclass
class _LockedLedger:
    path: Path
    parent: _PinnedDirectory
    pending_append: _PendingLedgerAppend | None = None


def _windows_final_path(handle: int, name: str) -> Path:
    kernel = _windows_kernel32()
    get_final_path = kernel.GetFinalPathNameByHandleW
    get_final_path.argtypes = (
        ctypes.c_void_p,
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
    )
    get_final_path.restype = ctypes.c_uint32
    buffer = ctypes.create_unicode_buffer(32768)
    length = int(
        get_final_path(
            ctypes.c_void_p(handle),
            buffer,
            len(buffer),
            0,
        ),
    )
    if length == 0 or length >= len(buffer):
        error = ctypes.get_last_error()
        raise OSError(error, f"无法读取{name}最终路径")
    value = buffer.value
    if value.startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return Path(value)


def _windows_open_parent_anchor(parent: Path, ledger_name: str) -> int:
    """Create a delete-on-close child that pins the verified parent subtree."""
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
    invalid = ctypes.c_void_p(-1).value
    for _attempt in range(128):
        anchor_path = (
            parent
            / f".{ledger_name}.anchor-{secrets.token_hex(16)}"
        )
        handle_value = create_file(
            str(anchor_path),
            0x80000000 | 0x40000000 | 0x00010000,
            0x1 | 0x2,
            None,
            1,
            0x100 | 0x04000000 | 0x00200000,
            None,
        )
        if handle_value is not None and int(handle_value) != invalid:
            return int(handle_value)
        error = ctypes.get_last_error()
        if error not in {80, 183}:
            raise OSError(error, f"无法建立台账父目录锚点: {parent}")
    raise FileExistsError("无法分配唯一台账父目录锚点")


def _validate_windows_parent_anchor(
    handle: int,
    parent: _PinnedDirectory,
    name: str,
) -> None:
    anchor_parent = _windows_final_path(handle, f"{name}父目录锚点").parent
    expected = os.path.normcase(os.path.abspath(parent.path))
    actual = os.path.normcase(os.path.abspath(anchor_parent))
    if actual != expected:
        raise ValueError(f"{name}父目录锚点越出已验证目录")


@contextmanager
def _windows_named_ledger_mutex(
    path: Path,
    transaction_state: _LedgerTransactionState | None = None,
) -> Iterator[None]:
    """Serialize one canonical ledger without creating a path-based lock file."""
    kernel = _windows_kernel32()
    create_mutex = kernel.CreateMutexW
    create_mutex.argtypes = (
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_wchar_p,
    )
    create_mutex.restype = ctypes.c_void_p
    mutex_name = (
        "Global\\guvolu-economic-ledger-"
        + sha256_text(os.path.normcase(str(path)))
    )
    handle_value = create_mutex(None, 0, mutex_name)
    if handle_value is None:
        error = ctypes.get_last_error()
        raise OSError(error, "无法建立经济台账命名 mutex")
    handle = int(handle_value)
    wait = kernel.WaitForSingleObject
    wait.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    wait.restype = ctypes.c_uint32
    result = int(wait(ctypes.c_void_p(handle), 0xFFFFFFFF))
    if result not in {0, 0x80}:
        error = ctypes.get_last_error()
        _windows_discard_handle_after_failure(handle)
        raise OSError(error, f"经济台账 mutex 等待失败: {result}")
    try:
        yield
    finally:
        body_failed = sys.exc_info()[0] is not None
        release = kernel.ReleaseMutex
        release.argtypes = (ctypes.c_void_p,)
        release.restype = ctypes.c_int

        def release_mutex() -> None:
            if not release(ctypes.c_void_p(handle)):
                error = ctypes.get_last_error()
                raise OSError(error, "经济台账 mutex 释放失败")

        release_error: OSError | None = None
        try:
            _cleanup_transaction_resource(
                release_mutex,
                state=transaction_state,
                body_failed=body_failed,
                phase="ledger-lock-release-after-effect",
                path=path.parent,
            )
        except OSError as error:
            release_error = error
        _cleanup_transaction_resource(
            lambda: _windows_close_handle(handle),
            state=transaction_state,
            body_failed=body_failed or release_error is not None,
            phase="ledger-lock-close-after-effect",
            path=path.parent,
        )
        if release_error is not None:
            raise release_error



def _windows_open_regular_file(path: Path, name: str) -> int:
    """Open one existing regular file without following or sharing deletion."""
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
        0x80000000,
        0x1,
        None,
        3,
        0x80 | 0x00200000,
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle_value is None or int(handle_value) == invalid:
        error = ctypes.get_last_error()
        raise OSError(error, f"无法安全打开{name}: {path}")
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
        _windows_discard_handle_after_failure(handle)
        raise OSError(error, f"无法读取{name}身份: {path}")
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    directory_flag = int(getattr(stat, "FILE_ATTRIBUTE_DIRECTORY", 0x10))
    if (
        int(information.file_attributes) & (reparse_flag | directory_flag)
        or int(information.number_of_links) != 1
    ):
        _windows_discard_handle_after_failure(handle)
        raise ValueError(f"{name}必须是普通单链接文件")
    import msvcrt

    try:
        return msvcrt.open_osfhandle(
            handle,
            os.O_RDONLY | getattr(os, "O_BINARY", 0),
        )
    except BaseException:
        _windows_discard_handle_after_failure(handle)
        raise


def _windows_open_regular_file_for_update(
    path: Path,
    name: str,
    *,
    create: bool,
) -> int:
    """Open or create one inode-bound ledger without sharing writes/deletion."""
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
        0x80000000 | 0x40000000,
        0,
        None,
        1 if create else 3,
        0x80 | 0x00200000,
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle_value is None or int(handle_value) == invalid:
        error = ctypes.get_last_error()
        action = "建立" if create else "打开"
        raise OSError(error, f"无法安全{action}{name}: {path}")
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
        _windows_discard_handle_after_failure(handle)
        raise OSError(error, f"无法读取{name}身份: {path}")
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    directory_flag = int(getattr(stat, "FILE_ATTRIBUTE_DIRECTORY", 0x10))
    if (
        int(information.file_attributes) & (reparse_flag | directory_flag)
        or int(information.number_of_links) != 1
    ):
        _windows_discard_handle_after_failure(handle)
        raise ValueError(f"{name}必须是普通单链接文件")
    import msvcrt

    try:
        return msvcrt.open_osfhandle(
            handle,
            os.O_RDWR | getattr(os, "O_BINARY", 0),
        )
    except BaseException:
        _windows_discard_handle_after_failure(handle)
        raise


@contextmanager
def _exclusive_pinned_lock(
    ledger_path: Path,
    parent: _PinnedDirectory,
    name: str,
    *,
    transaction_state: _LedgerTransactionState | None = None,
    create_lock: bool,
) -> Iterator[None]:
    """Lock a ledger without resolving the lock through a mutable parent."""
    if parent.descriptor is None:
        with _windows_named_ledger_mutex(ledger_path, transaction_state):
            _path_race_hook("ledger-lock-before-open", parent.path)
            _revalidate_directory_chain(
                parent.path,
                parent.identity,
                f"{name} 父目录",
            )
            _path_race_hook("ledger-lock-after-final-check", parent.path)
            anchor = _windows_open_parent_anchor(
                parent.path,
                ledger_path.name,
            )
            try:
                identity_error: BaseException | None = None
                try:
                    _validate_windows_parent_anchor(anchor, parent, name)
                    _revalidate_directory_chain(
                        parent.path,
                        parent.identity,
                        f"{name} 父目录",
                    )
                except BaseException as error:
                    identity_error = error
                _path_race_hook("ledger-lock-after-open", parent.path)
                if identity_error is not None:
                    raise identity_error
                _validate_windows_parent_anchor(anchor, parent, name)
                _revalidate_directory_chain(
                    parent.path,
                    parent.identity,
                    f"{name} 父目录",
                )
                yield
            finally:
                _cleanup_transaction_resource(
                    lambda: _windows_close_handle(anchor),
                    state=transaction_state,
                    body_failed=sys.exc_info()[0] is not None,
                    phase="ledger-anchor-close-after-effect",
                    path=parent.path,
                )
        return

    lock_name = ledger_path.name + ".lock"
    descriptor = -1
    _path_race_hook("ledger-lock-before-open", parent.path)
    _revalidate_directory_chain(
        parent.path,
        parent.identity,
        f"{name} 父目录",
    )
    _path_race_hook("ledger-lock-after-final-check", parent.path)
    try:
        flags = (
            os.O_RDWR | os.O_CREAT
            if create_lock
            else os.O_RDONLY
        )
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(
            lock_name,
            flags,
            0o600,
            dir_fd=parent.descriptor,
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or int(metadata.st_nlink) != 1:
            raise ValueError("台账锁必须是普通单链接文件")
        if metadata.st_size == 0 and create_lock:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        elif metadata.st_size != 1:
            raise ValueError("台账锁文件长度非法")
        os.lseek(descriptor, 0, os.SEEK_SET)
        identity_error = None
        try:
            _revalidate_directory_chain(
                parent.path,
                parent.identity,
                f"{name} 父目录",
            )
        except BaseException as error:
            identity_error = error
        _path_race_hook("ledger-lock-after-open", parent.path)
        if identity_error is not None:
            raise identity_error
        _revalidate_directory_chain(
            parent.path,
            parent.identity,
            f"{name} 父目录",
        )
        fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            yield
        finally:
            _cleanup_transaction_resource(
                lambda: fcntl.flock(descriptor, fcntl.LOCK_UN),
                state=transaction_state,
                body_failed=sys.exc_info()[0] is not None,
                phase="ledger-lock-release-after-effect",
                path=parent.path,
            )
    finally:
        if descriptor >= 0:
            _cleanup_transaction_resource(
                lambda: os.close(descriptor),
                state=transaction_state,
                body_failed=sys.exc_info()[0] is not None,
                phase="ledger-lock-close-after-effect",
                path=parent.path,
            )



@contextmanager
def _exclusive_ledger_lock(
    path: Path,
    name: str,
    *,
    require_file: bool,
    transaction_state: _LedgerTransactionState | None = None,
    precommit_validator: Callable[[], None] | None = None,
) -> Iterator[_LockedLedger]:
    """固定父目录链后持锁，避免锁和数据路径双换位。"""
    lexical = _absolute_lexical_path(path, name)
    if not require_file:
        _ensure_canonical_directory_tree(lexical.parent, f"{name} 父目录")
    canonical = _canonical_ledger_path(path, name, require_file=require_file)
    if transaction_state is None:
        transaction_state = _LedgerTransactionState()
    with _pin_directory_chain(
        canonical.parent,
        f"{name} 父目录",
        transaction_state=transaction_state,
    ) as parent:
        with _exclusive_pinned_lock(
            canonical,
            parent,
            name,
            transaction_state=transaction_state,
            create_lock=not require_file,
        ):
            canonical = _canonical_ledger_path(
                canonical,
                name,
                require_file=require_file,
            )
            locked = _LockedLedger(canonical, parent)
            try:
                yield locked
            except BaseException:
                try:
                    _rollback_pending_append(locked)
                except BaseException as rollback_error:
                    raise OSError(
                        f"{name} 失败后无法恢复原台账 inode",
                    ) from rollback_error
                raise
            else:
                if (
                    transaction_state.committed
                    and locked.pending_append is None
                ):
                    # 包围已提交写事务的只读锁不再反转结果。
                    return
                try:
                    _canonical_ledger_path(canonical, name, require_file=True)
                    _revalidate_directory_chain(
                        canonical.parent,
                        parent.identity,
                        f"{name} 父目录",
                    )
                    had_pending_append = locked.pending_append is not None
                    _finish_pending_append(
                        locked,
                        precommit_validator=precommit_validator,
                    )
                    if had_pending_append:
                        transaction_state.committed = True
                except BaseException:
                    try:
                        _rollback_pending_append(locked)
                    except BaseException as rollback_error:
                        raise OSError(
                            f"{name} 提交检查失败后无法恢复原台账 inode",
                        ) from rollback_error
                    raise


def _pinned_lstat(ledger: _LockedLedger) -> os.stat_result | None:
    try:
        if ledger.parent.descriptor is None:
            return ledger.path.lstat()
        return os.stat(
            ledger.path.name,
            dir_fd=ledger.parent.descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None


def _read_pinned_bytes(ledger: _LockedLedger, name: str) -> bytes:
    metadata = _pinned_lstat(ledger)
    if metadata is None:
        raise FileNotFoundError(ledger.path)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or int(metadata.st_nlink) != 1
    ):
        raise ValueError(f"{name} 必须是普通单链接文件")
    if ledger.parent.descriptor is None:
        descriptor = _windows_open_regular_file(ledger.path, name)
    else:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(
            ledger.path.name,
            flags,
            dir_fd=ledger.parent.descriptor,
        )
    try:
        opened = os.fstat(descriptor)
        if (
            int(opened.st_dev) != int(metadata.st_dev)
            or int(opened.st_ino) != int(metadata.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or int(opened.st_nlink) != 1
        ):
            raise ValueError(f"{name} 文件身份在打开时发生变化")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            return handle.read()
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_chain(
    ledger: _LockedLedger,
    record_type: str,
) -> tuple[Mapping[str, object], ...]:
    path = ledger.path
    try:
        body = _read_pinned_bytes(ledger, f"{record_type} 台账")
        text = body.decode("utf-8")
    except FileNotFoundError:
        return ()
    except (OSError, UnicodeDecodeError) as error:
        raise ValueError(f"{record_type} 台账无法读取") from error
    if body and not body.endswith(b"\n"):
        raise ValueError(f"{record_type} 台账存在未完成行")
    lines = text.splitlines()
    previous = _ZERO_HASH
    rows: list[Mapping[str, object]] = []
    for sequence, line in enumerate(lines, start=1):
        try:
            loaded: object = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"{record_type} 台账第 {sequence} 行非法") from error
        row = _object(loaded, f"{record_type} 台账第 {sequence} 行")
        if canonical_json(row) != line:
            raise ValueError(f"{record_type} 台账第 {sequence} 行不是 canonical JSON")
        if row.get("record_type") != record_type or row.get("sequence") != sequence:
            raise ValueError(f"{record_type} 台账顺序或类型非法")
        if row.get("ledger_canonical_path") != path.as_posix():
            raise ValueError(f"{record_type} 台账登记路径与当前路径不一致")
        if row.get("previous_record_sha256") != previous:
            raise ValueError(f"{record_type} 台账哈希链断裂")
        supplied_hash = _sha256(row.get("record_sha256"), "record_sha256")
        hash_body = dict(row)
        del hash_body["record_sha256"]
        expected_hash = sha256_text(canonical_json(hash_body))
        if supplied_hash != expected_hash:
            raise ValueError(f"{record_type} 台账记录散列不匹配")
        previous = supplied_hash
        rows.append(row)
    return tuple(rows)


def _chain_rows(
    record_type: str,
    ledger_path: Path,
    existing: Sequence[Mapping[str, object]],
    payloads: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    previous = (
        _ZERO_HASH
        if not existing
        else _sha256(existing[-1].get("record_sha256"), "record_sha256")
    )
    rows: list[Mapping[str, object]] = []
    for offset, payload in enumerate(payloads, start=1):
        body: dict[str, object] = {
            "record_type": record_type,
            "sequence": len(existing) + offset,
            "previous_record_sha256": previous,
            **dict(payload),
            "ledger_canonical_path": ledger_path.as_posix(),
        }
        digest = sha256_text(canonical_json(body))
        row = {**body, "record_sha256": digest}
        rows.append(row)
        previous = digest
    return tuple(rows)


def _fsync_directory(directory: Path) -> None:
    """尽力持久化目录项；不支持目录 fsync 的平台安全降级。"""
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        if os.name == "nt":
            return
        raise
    try:
        os.fsync(descriptor)
    except OSError:
        if os.name != "nt":
            raise
    finally:
        os.close(descriptor)


def _create_pinned_temp(
    parent: _PinnedDirectory,
    prefix: str,
    mode: int,
) -> tuple[int, str, Path]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    for _attempt in range(128):
        name = f"{prefix}{secrets.token_hex(16)}"
        path = parent.path / name
        try:
            if parent.descriptor is None:
                descriptor = os.open(path, flags, mode)
            else:
                descriptor = os.open(
                    name,
                    flags,
                    mode,
                    dir_fd=parent.descriptor,
                )
            return descriptor, name, path
        except FileExistsError:
            continue
    raise FileExistsError("无法分配唯一台账临时文件")


def _unlink_pinned(parent: _PinnedDirectory, name: str) -> None:
    if parent.descriptor is None:
        (parent.path / name).unlink()
    else:
        os.unlink(name, dir_fd=parent.descriptor)


def _link_pinned(parent: _PinnedDirectory, source: str, target: str) -> None:
    if parent.descriptor is None:
        os.link(
            parent.path / source,
            parent.path / target,
            follow_symlinks=False,
        )
    else:
        os.link(
            source,
            target,
            src_dir_fd=parent.descriptor,
            dst_dir_fd=parent.descriptor,
            follow_symlinks=False,
        )


def _sync_pinned_directory(parent: _PinnedDirectory) -> None:
    if parent.descriptor is None:
        _fsync_directory(parent.path)
    else:
        os.fsync(parent.descriptor)


def _same_file_identity(
    metadata: os.stat_result,
    device: int,
    inode: int,
) -> bool:
    return (
        int(metadata.st_dev) == device
        and int(metadata.st_ino) == inode
        and stat.S_ISREG(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
    )


def _read_descriptor_bytes(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def _write_descriptor_all(descriptor: int, body: bytes) -> None:
    offset = 0
    while offset < len(body):
        written = os.write(descriptor, body[offset:])
        if written <= 0:
            raise OSError("台账写入未取得进展")
        offset += written


def _open_pinned_ledger_for_update(
    ledger: _LockedLedger,
    name: str,
) -> tuple[int, bool, os.stat_result]:
    """Open the canonical inode once; never write through a replaceable temp."""
    prior = _pinned_lstat(ledger)
    if prior is not None and (
        not stat.S_ISREG(prior.st_mode)
        or stat.S_ISLNK(prior.st_mode)
        or int(prior.st_nlink) != 1
    ):
        raise ValueError(f"{name}必须是普通单链接文件")
    created = prior is None
    descriptor = -1
    opened: os.stat_result | None = None
    try:
        if ledger.parent.descriptor is None:
            descriptor = _windows_open_regular_file_for_update(
                ledger.path,
                name,
                create=created,
            )
        else:
            flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
            if created:
                flags |= os.O_CREAT | os.O_EXCL
            descriptor = os.open(
                ledger.path.name,
                flags,
                0o644,
                dir_fd=ledger.parent.descriptor,
            )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or int(opened.st_nlink) != 1
            or (
                prior is not None
                and not _same_file_identity(
                    opened,
                    int(prior.st_dev),
                    int(prior.st_ino),
                )
            )
        ):
            raise ValueError(f"{name} 文件身份在更新打开时发生变化")
        current = _pinned_lstat(ledger)
        if current is None or not _same_file_identity(
            current,
            int(opened.st_dev),
            int(opened.st_ino),
        ):
            raise ValueError(f"{name} 规范路径未绑定打开的 inode")
        return descriptor, created, opened
    except BaseException:
        truncate_error: BaseException | None = None
        cleanup_error: BaseException | None = None
        if descriptor >= 0:
            try:
                if created:
                    os.ftruncate(descriptor, 0)
                    os.fsync(descriptor)
            except BaseException as error:
                truncate_error = error
            try:
                os.close(descriptor)
            except OSError:
                # close 后效不阻断清理。
                pass
        if created and opened is None:
            cleanup_error = OSError(f"{name} 新台账身份不可得")
        elif created:
            assert opened is not None
            try:
                current = _pinned_lstat(ledger)
                if current is not None:
                    if not _same_file_identity(
                        current,
                        int(opened.st_dev),
                        int(opened.st_ino),
                    ):
                        raise OSError("新台账路径被其他 inode 占用")
                    if int(current.st_nlink) != 1 and truncate_error is not None:
                        raise OSError("新台账外链无法恢复为空") from truncate_error
                    _unlink_pinned(ledger.parent, ledger.path.name)
                _sync_pinned_directory(ledger.parent)
            except BaseException as error:
                cleanup_error = error
        if cleanup_error is not None:
            raise OSError(f"{name} 更新打开失败后无法清理") from cleanup_error
        raise


def _validate_pending_append(
    ledger: _LockedLedger,
    *,
    require_single_link: bool,
) -> _PendingLedgerAppend:
    pending = ledger.pending_append
    if pending is None:
        raise RuntimeError("台账没有待提交追加")
    metadata = os.fstat(pending.descriptor)
    if not _same_file_identity(metadata, pending.device, pending.inode):
        raise ValueError(f"{pending.record_type} 台账 inode 身份发生变化")
    if require_single_link and int(metadata.st_nlink) != 1:
        raise ValueError(f"{pending.record_type} 台账更新期间出现硬链接")
    return pending


def _rollback_pending_append(ledger: _LockedLedger) -> None:
    pending = ledger.pending_append
    if pending is None:
        return
    failure: BaseException | None = None
    remove_created = False
    try:
        _validate_pending_append(ledger, require_single_link=False)
        os.lseek(pending.descriptor, 0, os.SEEK_SET)
        _write_descriptor_all(pending.descriptor, pending.original_body)
        os.ftruncate(pending.descriptor, len(pending.original_body))
        os.fsync(pending.descriptor)
        current = _pinned_lstat(ledger)
        if pending.created:
            if current is not None:
                if not _same_file_identity(current, pending.device, pending.inode):
                    raise OSError("新台账规范路径已被其他 inode 占用")
                remove_created = True
        elif current is None or not _same_file_identity(
            current,
            pending.device,
            pending.inode,
        ):
            raise OSError("既有台账规范路径不再指向原 inode")
    except BaseException as error:
        failure = error
    finally:
        try:
            os.close(pending.descriptor)
        except OSError:
            # 内容已经持久化。
            # close 后效不遮蔽主异常。
            # 仍须继续 unlink 和目录同步。
            pass
        ledger.pending_append = None
    if remove_created:
        try:
            current = _pinned_lstat(ledger)
            if current is None or not _same_file_identity(
                current,
                pending.device,
                pending.inode,
            ):
                raise OSError("新台账关闭后规范路径身份发生变化")
            _unlink_pinned(ledger.parent, ledger.path.name)
        except BaseException as error:
            failure = failure or error
    try:
        _sync_pinned_directory(ledger.parent)
    except BaseException as error:
        failure = failure or error
    if failure is not None:
        raise OSError("inode-bound 台账追加无法回滚") from failure


def _finish_pending_append(
    ledger: _LockedLedger,
    *,
    precommit_validator: Callable[[], None] | None = None,
) -> None:
    pending = ledger.pending_append
    if pending is None:
        return
    _validate_pending_append(ledger, require_single_link=True)
    if _read_descriptor_bytes(pending.descriptor) != pending.committed_body:
        raise ValueError(f"{pending.record_type} 台账提交内容发生变化")
    current = _pinned_lstat(ledger)
    if (
        current is None
        or not _same_file_identity(current, pending.device, pending.inode)
        or int(current.st_nlink) != 1
    ):
        raise ValueError(f"{pending.record_type} 台账规范路径身份发生变化")
    os.fsync(pending.descriptor)
    _sync_pinned_directory(ledger.parent)
    _path_race_hook("ledger-before-commit", ledger.path.parent)
    _revalidate_directory_chain(
        ledger.path.parent,
        ledger.parent.identity,
        f"{pending.record_type} 台账父目录",
    )
    _validate_pending_append(ledger, require_single_link=True)
    current = _pinned_lstat(ledger)
    if (
        current is None
        or not _same_file_identity(current, pending.device, pending.inode)
        or int(current.st_nlink) != 1
    ):
        raise ValueError(f"{pending.record_type} 台账提交前路径身份发生变化")
    if precommit_validator is not None:
        precommit_validator()
    descriptor = pending.descriptor
    ledger.pending_append = None
    try:
        os.close(descriptor)
    except OSError:
        # 已提交。
        # close 状态不明时不得诱导重试。
        pass


def _append_chain_unlocked(
    ledger: _LockedLedger,
    record_type: str,
    existing: Sequence[Mapping[str, object]],
    payloads: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    path = ledger.path
    rows = _chain_rows(record_type, path, existing, payloads)
    if not rows:
        return ()
    if ledger.pending_append is not None:
        raise RuntimeError("同一台账锁内只允许一次待提交追加")
    old_body = "".join(
        canonical_json(row) + "\n" for row in existing
    ).encode("utf-8")
    appended_body = "".join(
        canonical_json(row) + "\n" for row in rows
    ).encode("utf-8")
    new_body = old_body + appended_body
    _path_race_hook("ledger-before-temp", path.parent)
    _revalidate_directory_chain(
        path.parent,
        ledger.parent.identity,
        f"{record_type} 台账父目录",
    )
    descriptor, created, metadata = _open_pinned_ledger_for_update(
        ledger,
        f"{record_type} 台账",
    )
    try:
        current_body = _read_descriptor_bytes(descriptor)
        if current_body != old_body:
            raise ValueError(f"{record_type} 台账在持锁期间发生变化")
        ledger.pending_append = _PendingLedgerAppend(
            descriptor=descriptor,
            created=created,
            device=int(metadata.st_dev),
            inode=int(metadata.st_ino),
            original_body=old_body,
            committed_body=new_body,
            record_type=record_type,
        )
        descriptor = -1
        pending = _validate_pending_append(ledger, require_single_link=True)
        os.lseek(pending.descriptor, len(old_body), os.SEEK_SET)
        _write_descriptor_all(pending.descriptor, appended_body)
        if os.lseek(pending.descriptor, 0, os.SEEK_END) != len(new_body):
            raise OSError(f"{record_type} 台账出现短写或并发改写")
        os.fsync(pending.descriptor)
        _path_race_hook("ledger-after-write", path.parent)
        _validate_pending_append(ledger, require_single_link=True)
        _revalidate_directory_chain(
            path.parent,
            ledger.parent.identity,
            f"{record_type} 台账父目录",
        )
        _path_race_hook("ledger-before-replace", path.parent)
        _revalidate_directory_chain(
            path.parent,
            ledger.parent.identity,
            f"{record_type} 台账父目录",
        )
        _path_race_hook("ledger-after-final-check", path.parent)
        identity_error: BaseException | None = None
        try:
            _revalidate_directory_chain(
                path.parent,
                ledger.parent.identity,
                f"{record_type} 台账父目录",
            )
        except BaseException as error:
            identity_error = error
        _path_race_hook("ledger-after-install", path.parent)
        if identity_error is not None:
            raise identity_error
        _path_race_hook("ledger-after-replace", path.parent)
        _revalidate_directory_chain(
            path.parent,
            ledger.parent.identity,
            f"{record_type} 台账父目录",
        )
        _sync_pinned_directory(ledger.parent)
        _validate_pending_append(ledger, require_single_link=True)
    finally:
        if descriptor >= 0:
            truncate_error: BaseException | None = None
            cleanup_error: BaseException | None = None
            if created:
                try:
                    os.ftruncate(descriptor, 0)
                    os.fsync(descriptor)
                except BaseException as error:
                    truncate_error = error
            try:
                os.close(descriptor)
            except OSError:
                # close 后效错误不中断新文件的删除。
                pass
            if created:
                try:
                    current = _pinned_lstat(ledger)
                    if current is not None:
                        if not _same_file_identity(
                            current,
                            int(metadata.st_dev),
                            int(metadata.st_ino),
                        ):
                            raise OSError("新台账路径被其他 inode 占用")
                        if int(current.st_nlink) != 1 and truncate_error is not None:
                            raise OSError("新台账外链无法恢复为空") from truncate_error
                        _unlink_pinned(ledger.parent, path.name)
                    _sync_pinned_directory(ledger.parent)
                except BaseException as error:
                    cleanup_error = error
            if cleanup_error is not None:
                raise OSError(
                    f"{record_type} 台账待追加建立失败后无法清理",
                ) from cleanup_error
    return rows


def _validate_revision_chain(observations: Sequence[EconomicObservation]) -> None:
    observation_ids: set[str] = set()
    revision_ids: set[str] = set()
    latest: dict[tuple[str, datetime], EconomicObservation] = {}
    for observation in observations:
        if observation.observation_id in observation_ids:
            raise ValueError(f"重复 observation_id: {observation.observation_id}")
        if observation.revision_id in revision_ids:
            raise ValueError(f"重复 revision_id: {observation.revision_id}")
        key = (observation.series_id, observation.event_time)
        previous = latest.get(key)
        if previous is None:
            if observation.supersedes_revision_id is not None:
                raise ValueError("首版观测不得声称 supersedes_revision_id")
        else:
            if observation.supersedes_revision_id != previous.revision_id:
                raise ValueError("修订必须精确指向同期前一 revision_id")
            if observation.available_time <= previous.available_time:
                raise ValueError("修订 available_time 必须严格递增")
        observation_ids.add(observation.observation_id)
        revision_ids.add(observation.revision_id)
        latest[key] = observation


_OBSERVATION_ROW_KEYS = frozenset({
    "record_type", "sequence", "previous_record_sha256", "record_sha256",
    "ledger_canonical_path", "schema_version", "observation_id", "series_id",
    "value", "unit", "event_time", "available_time", "ingest_time",
    "revision_id", "supersedes_revision_id", "source_receipt",
})


def _observation_snapshot(
    rows: Sequence[Mapping[str, object]],
) -> EconomicObservationSnapshot:
    for row in rows:
        _validate_exact_keys(row, _OBSERVATION_ROW_KEYS, "economic observation row")
    observations = tuple(parse_economic_observation(row) for row in rows)
    _validate_revision_chain(observations)
    head = (
        _ZERO_HASH
        if not rows
        else _sha256(rows[-1].get("record_sha256"), "record_sha256")
    )
    return EconomicObservationSnapshot(observations, len(rows), head)


def load_economic_observation_snapshot(path: Path) -> EconomicObservationSnapshot:
    """在共享路径锁内读取并校验完整观测台账。"""
    with _exclusive_ledger_lock(
        path,
        "经济观测台账",
        require_file=True,
    ) as ledger:
        rows = _read_chain(ledger, "economic_observation")
    return _observation_snapshot(rows)


def load_economic_observations(path: Path) -> tuple[EconomicObservation, ...]:
    """读取并全链校验追加式观测台账。"""
    return load_economic_observation_snapshot(path).observations


def append_economic_observations(
    path: Path,
    values: Sequence[Mapping[str, object]],
) -> tuple[EconomicObservation, ...]:
    """原子验证一批观测后追加；任一非法则整批不落盘。"""
    parsed = tuple(parse_economic_observation(value) for value in values)
    if not parsed:
        raise ValueError("观测批次不得为空")
    with _exclusive_ledger_lock(
        path,
        "经济观测台账",
        require_file=False,
    ) as ledger:
        rows = _read_chain(ledger, "economic_observation")
        existing = _observation_snapshot(rows).observations
        _validate_revision_chain((*existing, *parsed))
        _append_chain_unlocked(
            ledger,
            "economic_observation",
            rows,
            tuple(observation.payload() for observation in parsed),
        )
    return parsed


@dataclass(frozen=True)
class EconomicSeriesPolicy:
    """一个经济序列的语义、归一化与新鲜度合同。"""

    series_id: str
    dimension: str
    unit: str
    neutral_value: float
    scale: float
    direction: str
    weight: float
    max_age_seconds: int

    def payload(self) -> Mapping[str, object]:
        return {
            "dimension": self.dimension,
            "unit": self.unit,
            "neutral_value": self.neutral_value,
            "scale": self.scale,
            "direction": self.direction,
            "weight": self.weight,
            "max_age_seconds": self.max_age_seconds,
        }


@dataclass(frozen=True)
class ProposalGatePolicy:
    """搜索提案配额、模板白名单和 holdout 隔离合同。"""

    allowed_templates: tuple[tuple[str, tuple[str, ...]], ...]
    template_parameters: tuple[tuple[str, tuple[str, ...]], ...]
    max_proposals_per_run: int
    max_trial_budget_per_proposal: int
    max_total_trial_budget: int
    max_parameter_count: int
    max_regime_count: int
    max_horizon: int
    holdout_start_time: datetime | None

    def templates_for(self, family: str) -> tuple[str, ...]:
        return dict(self.allowed_templates).get(family, ())

    def parameters_for(self, template: str) -> tuple[str, ...]:
        return dict(self.template_parameters).get(template, ())

    def payload(self) -> Mapping[str, object]:
        return {
            "allowed_templates": {
                family: list(templates) for family, templates in self.allowed_templates
            },
            "template_parameters": {
                template: list(parameters)
                for template, parameters in self.template_parameters
            },
            "max_proposals_per_run": self.max_proposals_per_run,
            "max_trial_budget_per_proposal": self.max_trial_budget_per_proposal,
            "max_total_trial_budget": self.max_total_trial_budget,
            "max_parameter_count": self.max_parameter_count,
            "max_regime_count": self.max_regime_count,
            "max_horizon": self.max_horizon,
            "holdout_start_time": (
                None
                if self.holdout_start_time is None
                else _time_text(self.holdout_start_time)
            ),
        }


@dataclass(frozen=True)
class EconomicAgentPolicy:
    """完整、可内容寻址的经济代理政策。"""

    series: tuple[EconomicSeriesPolicy, ...]
    regime_threshold: float
    proposal_gate: ProposalGatePolicy

    def payload(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
            "series": {
                item.series_id: dict(item.payload())
                for item in sorted(self.series, key=lambda item: item.series_id)
            },
            "regime_threshold": self.regime_threshold,
            "proposal_gate": dict(self.proposal_gate.payload()),
        }

    @property
    def policy_id(self) -> str:
        return stable_identifier("economic-policy", self.payload())


def _identifier_list(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} 必须为非空列表")
    items = tuple(_identifier(item, f"{name}[]") for item in value)
    if len(set(items)) != len(items):
        raise ValueError(f"{name} 不得重复")
    return tuple(sorted(items))


def parse_economic_policy(value: Mapping[str, object]) -> EconomicAgentPolicy:
    """校验并规范化经济语境与提案门禁政策。"""
    _validate_exact_keys(
        value,
        frozenset({"schema_version", "series", "regime_threshold", "proposal_gate"}),
        "economic policy",
    )
    if value.get("schema_version") != 1:
        raise ValueError("economic policy schema_version 不受支持")
    raw_series = _object(value.get("series"), "series")
    series: list[EconomicSeriesPolicy] = []
    for raw_id, raw_spec in sorted(raw_series.items()):
        series_id = _identifier(raw_id, "series_id")
        spec = _object(raw_spec, f"series.{series_id}")
        _validate_exact_keys(
            spec,
            frozenset({
                "dimension", "unit", "neutral_value", "scale", "direction",
                "weight", "max_age_seconds",
            }),
            f"series.{series_id}",
        )
        dimension = _identifier(spec.get("dimension"), "dimension")
        if dimension not in DIMENSIONS:
            raise ValueError(f"不受支持的经济维度: {dimension}")
        direction = _identifier(spec.get("direction"), "direction")
        if direction not in {"higher", "lower"}:
            raise ValueError("direction 必须为 higher 或 lower")
        scale = _number(spec.get("scale"), "scale")
        weight = _number(spec.get("weight"), "weight")
        if scale <= 0.0 or weight <= 0.0:
            raise ValueError("scale 与 weight 必须大于零")
        series.append(EconomicSeriesPolicy(
            series_id=series_id,
            dimension=dimension,
            unit=_identifier(spec.get("unit"), "unit"),
            neutral_value=_number(spec.get("neutral_value"), "neutral_value"),
            scale=scale,
            direction=direction,
            weight=weight,
            max_age_seconds=_positive_integer(
                spec.get("max_age_seconds"), "max_age_seconds",
            ),
        ))
    threshold = _number(value.get("regime_threshold"), "regime_threshold")
    if threshold <= 0.0 or threshold > 3.0:
        raise ValueError("regime_threshold 必须在 (0, 3] 内")
    gate = _object(value.get("proposal_gate"), "proposal_gate")
    _validate_exact_keys(
        gate,
        frozenset({
            "allowed_templates", "template_parameters", "max_proposals_per_run",
            "max_trial_budget_per_proposal", "max_total_trial_budget",
            "max_parameter_count", "max_regime_count", "max_horizon",
            "holdout_start_time",
        }),
        "proposal_gate",
    )
    raw_allowed = _object(gate.get("allowed_templates"), "allowed_templates")
    allowed: list[tuple[str, tuple[str, ...]]] = []
    all_templates: set[str] = set()
    for raw_family, raw_templates in sorted(raw_allowed.items()):
        family = _identifier(raw_family, "family")
        templates = _identifier_list(raw_templates, f"allowed_templates.{family}")
        allowed.append((family, templates))
        all_templates.update(templates)
    raw_parameters = _object(
        gate.get("template_parameters"), "template_parameters",
    )
    parameters: list[tuple[str, tuple[str, ...]]] = []
    for raw_template, raw_names in sorted(raw_parameters.items()):
        template = _identifier(raw_template, "template")
        parameters.append((
            template,
            _identifier_list(raw_names, f"template_parameters.{template}"),
        ))
    if set(raw_parameters) != all_templates:
        raise ValueError("template_parameters 必须精确覆盖所有允许模板")
    raw_holdout = gate.get("holdout_start_time")
    holdout = (
        None
        if raw_holdout is None
        else _utc_time(raw_holdout, "holdout_start_time")
    )
    proposal_gate = ProposalGatePolicy(
        allowed_templates=tuple(allowed),
        template_parameters=tuple(parameters),
        max_proposals_per_run=_positive_integer(
            gate.get("max_proposals_per_run"), "max_proposals_per_run",
        ),
        max_trial_budget_per_proposal=_positive_integer(
            gate.get("max_trial_budget_per_proposal"),
            "max_trial_budget_per_proposal",
        ),
        max_total_trial_budget=_positive_integer(
            gate.get("max_total_trial_budget"), "max_total_trial_budget",
        ),
        max_parameter_count=_positive_integer(
            gate.get("max_parameter_count"), "max_parameter_count",
        ),
        max_regime_count=_positive_integer(
            gate.get("max_regime_count"), "max_regime_count",
        ),
        max_horizon=_positive_integer(gate.get("max_horizon"), "max_horizon"),
        holdout_start_time=holdout,
    )
    if (
        proposal_gate.max_total_trial_budget
        < proposal_gate.max_trial_budget_per_proposal
    ):
        raise ValueError("max_total_trial_budget 不得小于单提案上限")
    return EconomicAgentPolicy(tuple(series), threshold, proposal_gate)


def load_economic_policy(path: Path) -> EconomicAgentPolicy:
    """从 JSON 文件读取经济代理政策。"""
    return parse_economic_policy(_load_json(path, "economic policy"))


def _artifact(kind: str, body: Mapping[str, object]) -> Mapping[str, object]:
    artifact_id = stable_identifier(kind, body)
    return {**dict(body), "artifact_id": artifact_id}


def _verify_artifact(value: Mapping[str, object], kind: str) -> str:
    artifact_id = _text(value.get("artifact_id"), "artifact_id", maximum=96)
    body = dict(value)
    del body["artifact_id"]
    if artifact_id != stable_identifier(kind, body):
        raise ValueError(f"{kind} 制品散列不匹配")
    return artifact_id


def write_content_addressed_artifact(
    output: Path,
    value: Mapping[str, object],
    kind: str,
) -> Path:
    """以制品标识命名并且绝不改写既有不同内容。"""
    if kind in {"economic-search-plan-proposal", "economic-agent-run"}:
        raise ValueError("v1 禁止写入 standalone proposal/run 制品")
    artifact_id = _verify_artifact(value, kind)
    content = (canonical_json(value) + "\n").encode("utf-8")
    parent = _ensure_canonical_directory_tree(output, f"{kind} 制品目录")
    path = parent / f"{artifact_id}.json"
    transaction_state = _LedgerTransactionState()
    with _pin_directory_chain(
        parent,
        f"{kind} 制品目录",
        transaction_state=transaction_state,
    ) as pinned:
        with _exclusive_pinned_lock(
            path,
            pinned,
            f"{kind} 制品",
            transaction_state=transaction_state,
            create_lock=True,
        ):
            artifact = _LockedLedger(path, pinned)
            if _pinned_lstat(artifact) is not None:
                if _read_pinned_bytes(artifact, kind) != content:
                    raise ValueError(f"已有同名制品内容不同: {path}")
                transaction_state.committed = True
                return path
            descriptor, temp_name, _temp = _create_pinned_temp(
                pinned,
                f".{path.name}.artifact-",
                0o644,
            )
            installed = False
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    descriptor = -1
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                _path_race_hook("artifact-before-install", parent)
                _revalidate_directory_chain(
                    parent,
                    pinned.identity,
                    f"{kind} 制品目录",
                )
                _link_pinned(pinned, temp_name, path.name)
                installed = True
                identity_error: BaseException | None = None
                try:
                    _revalidate_directory_chain(
                        parent,
                        pinned.identity,
                        f"{kind} 制品目录",
                    )
                except BaseException as error:
                    identity_error = error
                _path_race_hook("artifact-after-install", parent)
                if identity_error is not None:
                    raise identity_error
                _revalidate_directory_chain(
                    parent,
                    pinned.identity,
                    f"{kind} 制品目录",
                )
                _unlink_pinned(pinned, temp_name)
                _sync_pinned_directory(pinned)
                if _read_pinned_bytes(artifact, kind) != content:
                    raise OSError("已提交制品内容发生变化")
                transaction_state.committed = True
            except BaseException as error:
                if installed:
                    try:
                        if _read_pinned_bytes(artifact, kind) != content:
                            raise OSError("已安装制品内容发生变化")
                        _unlink_pinned(pinned, path.name)
                        _sync_pinned_directory(pinned)
                    except BaseException as rollback_error:
                        raise OSError("制品安装失败后无法回滚") from rollback_error
                raise
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
                try:
                    _unlink_pinned(pinned, temp_name)
                except FileNotFoundError:
                    pass
            return path


def _load_content_addressed_artifact_untyped(
    path: Path,
    kind: str,
) -> Mapping[str, object]:
    """只验证 canonical 内容身份；调用方还必须做合同语义验证。"""
    value = _load_json(path, kind)
    artifact_id = _verify_artifact(value, kind)
    if path.stem != artifact_id:
        raise ValueError(f"{kind} 制品文件名与内容标识不一致")
    if path.read_text(encoding="utf-8") != canonical_json(value) + "\n":
        raise ValueError(f"{kind} 制品不是 canonical JSON")
    return value


def load_content_addressed_artifact(path: Path, kind: str) -> Mapping[str, object]:
    """读取普通内容制品；proposal/run 必须使用带 commitment 的专用入口。"""
    if kind in {"economic-search-plan-proposal", "economic-agent-run"}:
        raise ValueError(f"{kind} 必须使用专用语义与台账 commitment 验证入口")
    return _load_content_addressed_artifact_untyped(path, kind)


def build_economic_context(
    snapshot: EconomicObservationSnapshot,
    decision_time: datetime,
    policy: EconomicAgentPolicy,
) -> Mapping[str, object]:
    """按第四时点 decision_time 回放可知修订并生成确定性语境。"""
    decision = _utc_time(_time_text(decision_time), "decision_time")
    observations = snapshot.observations
    _validate_revision_chain(observations)
    # D-04 防未来。
    # 只按可用时点判定。
    # ingest_time 只记录落盘。
    # 它可以晚于 decision_time。
    # 不得据此抹去公开事实。
    eligible = tuple(sorted(
        (
            observation
            for observation in observations
            if observation.available_time <= decision
        ),
        key=lambda item: (
            item.series_id,
            item.event_time,
            item.available_time,
            item.ingest_time,
            item.revision_id,
        ),
    ))
    latest_revision: dict[tuple[str, datetime], EconomicObservation] = {}
    for observation in eligible:
        latest_revision[(observation.series_id, observation.event_time)] = observation
    latest_series: dict[str, EconomicObservation] = {}
    for observation in latest_revision.values():
        current = latest_series.get(observation.series_id)
        if current is None or (
            observation.event_time,
            observation.available_time,
            observation.ingest_time,
        ) > (current.event_time, current.available_time, current.ingest_time):
            latest_series[observation.series_id] = observation

    evidence: dict[str, Mapping[str, object]] = {}
    dimensions: dict[str, object] = {}
    missing_series: list[str] = []
    stale_series: list[str] = []
    for dimension in DIMENSIONS:
        configured = tuple(
            item for item in policy.series if item.dimension == dimension
        )
        series_states: list[Mapping[str, object]] = []
        score_parts: list[tuple[float, float]] = []
        for spec in configured:
            selected = latest_series.get(spec.series_id)
            if selected is None:
                missing_series.append(spec.series_id)
                series_states.append({
                    "series_id": spec.series_id,
                    "status": "missing",
                    "observation_id": None,
                    "revision_id": None,
                    "age_seconds": None,
                    "normalized_score": None,
                })
                continue
            if selected.unit != spec.unit:
                raise ValueError(
                    f"序列 {spec.series_id} unit={selected.unit} 与政策不一致",
                )
            age = (decision - selected.available_time).total_seconds()
            normalized = (selected.value - spec.neutral_value) / spec.scale
            if spec.direction == "lower":
                normalized = -normalized
            normalized = round(max(-3.0, min(3.0, normalized)), 12)
            stale = age > spec.max_age_seconds
            status = "stale" if stale else "fresh"
            if stale:
                stale_series.append(spec.series_id)
            else:
                score_parts.append((normalized, spec.weight))
                evidence[selected.observation_id] = selected.payload()
            series_states.append({
                "series_id": spec.series_id,
                "status": status,
                "observation_id": selected.observation_id,
                "revision_id": selected.revision_id,
                "event_time": _time_text(selected.event_time),
                "available_time": _time_text(selected.available_time),
                "ingest_time": _time_text(selected.ingest_time),
                "value": selected.value,
                "unit": selected.unit,
                "age_seconds": round(age, 6),
                "normalized_score": normalized,
            })
        fresh_count = len(score_parts)
        if not configured or not series_states or all(
            state["status"] == "missing" for state in series_states
        ):
            data_status = "missing"
        elif fresh_count == 0:
            data_status = "stale"
        elif fresh_count < len(configured):
            data_status = "partial"
        else:
            data_status = "fresh"
        score = (
            None
            if not score_parts
            else round(
                sum(item * weight for item, weight in score_parts)
                / sum(weight for _item, weight in score_parts),
                12,
            )
        )
        positive, negative, neutral = _REGIME_LABELS[dimension]
        regime = (
            "unknown"
            if score is None
            else positive
            if score >= policy.regime_threshold
            else negative
            if score <= -policy.regime_threshold
            else neutral
        )
        dimensions[dimension] = {
            "data_status": data_status,
            "score": score,
            "regime": regime,
            "configured_series_count": len(configured),
            "fresh_series_count": fresh_count,
            "series": series_states,
        }
    eligible_payloads = [observation.payload() for observation in eligible]
    body: Mapping[str, object] = {
        "schema_version": ECONOMIC_CONTEXT_SCHEMA_VERSION,
        "artifact_type": "economic_context",
        "method_version": ECONOMIC_CONTEXT_METHOD_VERSION,
        "decision_time": _time_text(decision),
        "policy_id": policy.policy_id,
        "policy": policy.payload(),
        "observation_ledger": {
            "sequence": snapshot.ledger_sequence,
            "head_sha256": snapshot.ledger_head_sha256,
        },
        "as_of_input_sha256": sha256_text(canonical_json(eligible_payloads)),
        "eligible_observation_ids": [
            observation.observation_id for observation in eligible
        ],
        "selected_observation_ids": sorted(evidence),
        "selected_revision_ids": sorted(
            str(item["revision_id"]) for item in evidence.values()
        ),
        "evidence": [evidence[item] for item in sorted(evidence)],
        "dimensions": dimensions,
        "quality": {
            "pit": True,
            "pit_basis": "available_time_lte_decision_time",
            "four_clocks_separated": True,
            "missing_series": sorted(missing_series),
            "stale_series": sorted(stale_series),
            "missing_dimensions": [
                dimension for dimension in DIMENSIONS
                if _object(dimensions[dimension], dimension)["data_status"]
                == "missing"
            ],
            "stale_dimensions": [
                dimension for dimension in DIMENSIONS
                if _object(dimensions[dimension], dimension)["data_status"]
                == "stale"
            ],
        },
        "authority": {
            "research_only": True,
            "network": False,
            "secrets": False,
            "trade": False,
            "execution": False,
            "config_mutation": False,
            "registry_mutation": False,
            "promotion": False,
            "holdout_governance_bound": False,
        },
    }
    return _artifact("economic-context", body)


def _verify_economic_context_rows(
    context: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
    policy: EconomicAgentPolicy,
    *,
    reject_eligible_tail: bool,
) -> str:
    """从已锁定台账行重建 context，并可要求 as-of 输入完整。"""
    context_id = _verify_artifact(context, "economic-context")
    ledger_identity = _object(
        context.get("observation_ledger"), "context.observation_ledger",
    )
    raw_sequence = ledger_identity.get("sequence")
    if (
        isinstance(raw_sequence, bool)
        or not isinstance(raw_sequence, int)
        or raw_sequence < 0
    ):
        raise ValueError("context.observation_ledger.sequence 非法")
    expected_head = _sha256(
        ledger_identity.get("head_sha256"),
        "context.observation_ledger.head_sha256",
    )
    if len(rows) < raw_sequence:
        raise ValueError("context 绑定的观测台账前缀已缺失")
    prefix = rows[:raw_sequence]
    snapshot = _observation_snapshot(prefix)
    if snapshot.ledger_head_sha256 != expected_head:
        raise ValueError("context 绑定的观测台账链头不匹配")
    decision = _utc_time(context.get("decision_time"), "context.decision_time")
    expected = build_economic_context(snapshot, decision, policy)
    if canonical_json(expected) != canonical_json(context):
        raise ValueError("economic-context 不能由已绑定观测台账重建")
    if reject_eligible_tail:
        full_snapshot = _observation_snapshot(rows)
        eligible_tail = tuple(
            observation.observation_id
            for observation in full_snapshot.observations[raw_sequence:]
            if observation.available_time <= decision
        )
        if eligible_tail:
            raise ValueError(
                "economic-context 前缀之后存在 decision_time 已可知的观测: "
                f"{list(eligible_tail)}",
            )
    return context_id


def verify_economic_context(
    context: Mapping[str, object],
    observation_ledger_path: Path,
    policy: EconomicAgentPolicy,
) -> str:
    """由台账已绑定前缀重建历史 context；不把它声明为当前完整输入。"""
    with _exclusive_ledger_lock(
        observation_ledger_path,
        "经济观测台账",
        require_file=True,
    ) as ledger:
        rows = _read_chain(ledger, "economic_observation")
        return _verify_economic_context_rows(
            context,
            rows,
            policy,
            reject_eligible_tail=False,
        )


def _normalize_bounds(value: object, name: str) -> Mapping[str, object]:
    bounds = _object(value, name)
    unexpected = sorted(set(bounds) - {"minimum", "maximum", "step"})
    if unexpected or "minimum" not in bounds or "maximum" not in bounds:
        raise ValueError(f"{name} 范围字段非法")
    minimum = _number(bounds.get("minimum"), f"{name}.minimum")
    maximum = _number(bounds.get("maximum"), f"{name}.maximum")
    if minimum > maximum:
        raise ValueError(f"{name}.minimum 不得大于 maximum")
    result: dict[str, object] = {"minimum": minimum, "maximum": maximum}
    if "step" in bounds:
        step = _number(bounds.get("step"), f"{name}.step")
        if step <= 0.0:
            raise ValueError(f"{name}.step 必须大于零")
        result["step"] = step
    return result


def _normalize_proposal(
    value: Mapping[str, object],
    policy: ProposalGatePolicy,
) -> Mapping[str, object]:
    required = frozenset({
        "hypothesis", "evidence_ids", "family", "template", "parameter_bounds",
        "regimes", "horizon", "falsification", "trial_budget",
    })
    unexpected = sorted(set(value) - (required | {"proposal_id"}))
    missing = sorted(required - set(value))
    if unexpected or missing:
        raise ValueError(
            f"ResearchProposal 字段不合同: missing={missing}, "
            f"unexpected={unexpected}",
        )
    family = _identifier(value.get("family"), "family")
    template = _identifier(value.get("template"), "template")
    if template not in policy.templates_for(family):
        raise ValueError("提案 family/template 不在白名单")
    raw_bounds = _object(value.get("parameter_bounds"), "parameter_bounds")
    if not raw_bounds or len(raw_bounds) > policy.max_parameter_count:
        raise ValueError("parameter_bounds 为空或超出参数配额")
    allowed_parameters = set(policy.parameters_for(template))
    if not set(raw_bounds).issubset(allowed_parameters):
        raise ValueError("parameter_bounds 含模板未登记参数")
    parameter_bounds = {
        _identifier(name, "parameter name"): _normalize_bounds(
            bounds, f"parameter_bounds.{name}",
        )
        for name, bounds in sorted(raw_bounds.items())
    }
    evidence_ids = _identifier_list(value.get("evidence_ids"), "evidence_ids")
    regimes = _identifier_list(value.get("regimes"), "regimes")
    if len(regimes) > policy.max_regime_count:
        raise ValueError("regimes 超出配额")
    for regime in regimes:
        parts = regime.split(":", maxsplit=1)
        if len(parts) != 2 or parts[0] not in DIMENSIONS:
            raise ValueError(f"非法 regime: {regime}")
    horizon = _object(value.get("horizon"), "horizon")
    _validate_exact_keys(
        horizon, frozenset({"unit", "minimum", "maximum"}), "horizon",
    )
    unit = _identifier(horizon.get("unit"), "horizon.unit")
    if unit not in {"bars", "hours", "days"}:
        raise ValueError("horizon.unit 只能为 bars/hours/days")
    horizon_min = _positive_integer(horizon.get("minimum"), "horizon.minimum")
    horizon_max = _positive_integer(horizon.get("maximum"), "horizon.maximum")
    if horizon_min > horizon_max or horizon_max > policy.max_horizon:
        raise ValueError("horizon 顺序非法或超出上限")
    trial_budget = _positive_integer(value.get("trial_budget"), "trial_budget")
    if trial_budget > policy.max_trial_budget_per_proposal:
        raise ValueError("trial_budget 超出单提案上限")
    body: Mapping[str, object] = {
        "schema_version": ECONOMIC_PROPOSAL_SCHEMA_VERSION,
        "hypothesis": _text(value.get("hypothesis"), "hypothesis"),
        "evidence_ids": list(evidence_ids),
        "family": family,
        "template": template,
        "parameter_bounds": parameter_bounds,
        "regimes": list(regimes),
        "horizon": {
            "unit": unit,
            "minimum": horizon_min,
            "maximum": horizon_max,
        },
        "falsification": _text(value.get("falsification"), "falsification"),
        "trial_budget": trial_budget,
    }
    proposal_id = stable_identifier("research-proposal", body)
    supplied = value.get("proposal_id")
    if supplied is not None and supplied != proposal_id:
        raise ValueError("proposal_id 与规范提案不一致")
    return {**dict(body), "proposal_id": proposal_id}


def _normalize_persisted_proposal(
    value: Mapping[str, object],
    policy: ProposalGatePolicy,
) -> Mapping[str, object]:
    """重读已规范提案，同时严格校验持久化 schema。"""
    required = frozenset({
        "schema_version", "hypothesis", "evidence_ids", "family", "template",
        "parameter_bounds", "regimes", "horizon", "falsification",
        "trial_budget", "proposal_id",
    })
    _validate_exact_keys(value, required, "persisted ResearchProposal")
    if value.get("schema_version") != ECONOMIC_PROPOSAL_SCHEMA_VERSION:
        raise ValueError("persisted ResearchProposal schema_version 非法")
    source = dict(value)
    del source["schema_version"]
    normalized = _normalize_proposal(source, policy)
    if canonical_json(normalized) != canonical_json(value):
        raise ValueError("persisted ResearchProposal 不是规范提案")
    return normalized


def _inference_identity(value: Mapping[str, object] | None) -> Mapping[str, object]:
    if value is not None:
        raise ValueError(
            "v1 不接受无法从本地封存制品重建的外部 inference_identity",
        )
    empty = sha256_text("")
    return {
        "provider": "none",
        "model_id": "deterministic-rules-v1",
        "model_parameters_sha256": sha256_text(canonical_json({})),
        "prompt_template_id": "none",
        "prompt_sha256": empty,
        "model_input_sha256": empty,
        "model_output_sha256": empty,
    }


@dataclass(frozen=True)
class EconomicAgentRunResult:
    """一次提案门禁的内嵌台账回执。"""

    run_id: str
    ledger_path: Path
    proposal_paths: tuple[Path, ...]
    accepted_proposal_ids: tuple[str, ...]
    rejected_count: int


def _proposal_context_reasons(
    proposal: Mapping[str, object],
    context: Mapping[str, object],
    evidence: Mapping[str, Mapping[str, object]],
    gate: ProposalGatePolicy,
    evaluated_at: datetime,
) -> list[str]:
    reasons: list[str] = []
    evidence_ids = proposal.get("evidence_ids")
    assert isinstance(evidence_ids, list)
    unknown_evidence = sorted(set(evidence_ids) - set(evidence))
    if unknown_evidence:
        reasons.append("evidence_not_fresh_or_not_in_context")
    dimensions = _object(context.get("dimensions"), "context.dimensions")
    regimes = proposal.get("regimes")
    assert isinstance(regimes, list)
    for raw_regime in regimes:
        dimension, expected = str(raw_regime).split(":", maxsplit=1)
        state = _object(dimensions.get(dimension), f"dimensions.{dimension}")
        if state.get("regime") != expected or state.get("data_status") not in {
            "fresh", "partial",
        }:
            reasons.append("regime_not_supported_by_current_context")
            break
    boundary = gate.holdout_start_time
    context_time = _utc_time(context.get("decision_time"), "context.decision_time")
    if evaluated_at < context_time:
        reasons.append("evaluation_precedes_context")
    # v1 治理尚未绑定。
    # market/vintage 未绑定。
    # 所有提案失败关闭。
    reasons.append("holdout_governance_unbound")
    if boundary is not None:
        if context_time >= boundary or evaluated_at >= boundary:
            reasons.append("holdout_boundary_reached")
        for evidence_id in evidence_ids:
            item = evidence.get(str(evidence_id))
            if item is not None and _utc_time(
                item.get("available_time"), "evidence.available_time",
            ) >= boundary:
                reasons.append("holdout_evidence_leakage")
                break
    return sorted(set(reasons))


def _attempt_index(value: Mapping[str, object]) -> int:
    raw = value.get("input_index")
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError("提案尝试缺少合法 input_index")
    return raw


def _proposal_artifact(
    context_id: str,
    policy: EconomicAgentPolicy,
    proposal: Mapping[str, object],
) -> Mapping[str, object]:
    """由规范提案重建固定为 research-only 的 proposal 制品。"""
    body: Mapping[str, object] = {
        "schema_version": ECONOMIC_PROPOSAL_SCHEMA_VERSION,
        "artifact_type": "economic_search_plan_proposal",
        "method_version": ECONOMIC_PROPOSAL_METHOD_VERSION,
        "source_context_artifact_id": context_id,
        "policy_id": policy.policy_id,
        "proposal": proposal,
        "search_plan_interface": {
            "contract": "proposal_only",
            "consumer": "SearchPlan",
            "family": proposal["family"],
            "template": proposal["template"],
            "parameter_bounds": proposal["parameter_bounds"],
            "trial_budget": proposal["trial_budget"],
            "requires_explicit_review": True,
            "requires_candidate_registration": True,
            "holdout_governance_bound": False,
            "may_write_config": False,
            "may_write_registry": False,
            "may_promote": False,
        },
        "authority": {
            "research_only": True,
            "network": False,
            "secrets": False,
            "trade": False,
            "execution": False,
            "config_mutation": False,
            "registry_mutation": False,
            "promotion": False,
            "holdout_governance_bound": False,
        },
    }
    return _artifact("economic-search-plan-proposal", body)


@dataclass(frozen=True)
class _ProposalEvaluation:
    attempts: tuple[Mapping[str, object], ...]
    accepted_artifacts: tuple[Mapping[str, object], ...]
    accepted_proposal_ids: tuple[str, ...]
    total_trial_budget: int


def _context_evidence(
    context: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    evidence_rows = context.get("evidence")
    if not isinstance(evidence_rows, list):
        raise ValueError("经济语境缺少 evidence")
    evidence: dict[str, Mapping[str, object]] = {}
    for raw in evidence_rows:
        item = _object(raw, "context.evidence[]")
        observation = parse_economic_observation(item)
        evidence[observation.observation_id] = observation.payload()
    return evidence


def _normalize_proposal_batch(
    proposals: Sequence[Mapping[str, object]],
    gate: ProposalGatePolicy,
) -> tuple[tuple[int, Mapping[str, object]], ...]:
    """在取锁或生成任何运行文件前完整验证提案合同。"""
    if len(proposals) > gate.max_proposals_per_run:
        raise ValueError(
            "ResearchProposal 批次数量超出 max_proposals_per_run；"
            "本批次不生成回执",
        )
    normalized: list[tuple[int, Mapping[str, object]]] = []
    for index, proposal in enumerate(proposals):
        try:
            normalized.append((index, _normalize_proposal(proposal, gate)))
        except ValueError as error:
            raise ValueError(
                f"ResearchProposal[{index}] 合同非法；本批次不生成回执",
            ) from error
    return tuple(normalized)


def _evaluate_proposals(
    normalized: Sequence[tuple[int, Mapping[str, object]]],
    *,
    context: Mapping[str, object],
    policy: EconomicAgentPolicy,
    evaluated_at: datetime,
) -> _ProposalEvaluation:
    """按确定性顺序门禁提案；v1 因治理未绑定而不会接受。"""
    evidence = _context_evidence(context)
    attempts: list[Mapping[str, object]] = []
    for index, proposal in sorted(
        normalized,
        key=lambda item: (str(item[1]["proposal_id"]), item[0]),
    ):
        proposal_id = str(proposal["proposal_id"])
        reasons = _proposal_context_reasons(
            proposal,
            context,
            evidence,
            policy.proposal_gate,
            evaluated_at,
        )
        reasons.append("holdout_governance_unbound")
        reasons = sorted(set(reasons))
        attempts.append({
            "input_index": index,
            "input_sha256": sha256_text(canonical_json(proposal)),
            "proposal_id": proposal_id,
            "proposal": proposal,
            "status": "rejected",
            "reasons": reasons,
        })
    attempts.sort(key=_attempt_index)
    return _ProposalEvaluation(
        tuple(attempts),
        (),
        (),
        0,
    )


def _run_authority() -> Mapping[str, object]:
    return {
        "research_only": True,
        "network": False,
        "secrets": False,
        "trade": False,
        "execution": False,
        "config_mutation": False,
        "registry_mutation": False,
        "promotion": False,
        "holdout_governance_bound": False,
    }


def _run_receipt(
    *,
    context: Mapping[str, object],
    context_id: str,
    policy: EconomicAgentPolicy,
    evaluated_at: datetime,
    inference: Mapping[str, object],
    evaluation: _ProposalEvaluation,
    verified_observation_ledger: Mapping[str, object],
) -> Mapping[str, object]:
    attempts = list(evaluation.attempts)
    input_hashes = [str(item["input_sha256"]) for item in attempts]
    input_identity = {
        "context_artifact_id": context_id,
        "policy_id": policy.policy_id,
        "proposal_input_sha256s": input_hashes,
        "proposal_batch_sha256": sha256_text(canonical_json(input_hashes)),
        "proposal_count": len(attempts),
        "observation_ledger": context["observation_ledger"],
        "verified_observation_ledger": verified_observation_ledger,
        "holdout_governance": {
            "bound": False,
            "binding": None,
            "reason": "holdout_governance_unbound",
        },
    }
    output_identity = {
        "accepted_artifact_ids": sorted(
            str(item["artifact_id"])
            for item in evaluation.accepted_artifacts
        ),
        "accepted_proposal_ids": list(evaluation.accepted_proposal_ids),
        "attempts_sha256": sha256_text(canonical_json(attempts)),
        "total_trial_budget": evaluation.total_trial_budget,
    }
    body: Mapping[str, object] = {
        "schema_version": ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION,
        "artifact_type": "economic_agent_run_receipt",
        "method_version": ECONOMIC_AGENT_METHOD_VERSION,
        "evaluated_at": _time_text(evaluated_at),
        "model_identity": inference,
        "prompt_identity": {
            "prompt_template_id": inference["prompt_template_id"],
            "prompt_sha256": inference["prompt_sha256"],
        },
        "input_identity": input_identity,
        "output_identity": output_identity,
        "attempts": attempts,
        "context": context,
        "policy": policy.payload(),
        "authority": _run_authority(),
    }
    return _artifact("economic-agent-run", body)


def _ledger_payload(
    receipt: Mapping[str, object],
    artifacts: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    if artifacts:
        raise ValueError("v1 不允许外部 accepted proposal 制品")
    run_id = str(receipt["artifact_id"])
    return {
        "schema_version": ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION,
        "method_version": ECONOMIC_AGENT_METHOD_VERSION,
        "run_id": run_id,
        "evaluated_at": receipt["evaluated_at"],
        "receipt_artifact_id": run_id,
        "receipt_sha256": sha256_text(canonical_json(receipt) + "\n"),
        "receipt_storage": "embedded_in_ledger",
        "receipt": receipt,
        "artifact_commitments": [],
        "research_only": True,
    }


def _canonical_artifact_root(path: Path, *, require_dir: bool) -> Path:
    if not path.is_absolute():
        raise ValueError("经济代理输出根必须使用绝对规范路径")
    normalized_text = os.path.normpath(str(path))
    if os.path.normcase(str(path)) != os.path.normcase(normalized_text):
        raise ValueError("经济代理输出根不得使用含 . 或 .. 的路径别名")
    lexical = Path(os.path.abspath(path))
    resolved = path.resolve(strict=False)
    if os.path.normcase(str(lexical)) != os.path.normcase(str(resolved)):
        raise ValueError("经济代理输出根不得使用路径别名")
    if resolved.exists() and not resolved.is_dir():
        raise ValueError("经济代理输出根不是目录")
    if require_dir and not resolved.is_dir():
        raise ValueError(f"经济代理输出根不存在: {resolved}")
    return resolved




















def _verify_proposal_artifact_value(
    value: Mapping[str, object],
    *,
    context: Mapping[str, object],
    policy: EconomicAgentPolicy,
) -> str:
    _validate_exact_keys(
        value,
        frozenset({
            "schema_version", "artifact_type", "method_version",
            "source_context_artifact_id", "policy_id", "proposal",
            "search_plan_interface", "authority", "artifact_id",
        }),
        "economic proposal artifact",
    )
    context_id = _verify_artifact(context, "economic-context")
    normalized = _normalize_persisted_proposal(
        _object(value.get("proposal"), "proposal"),
        policy.proposal_gate,
    )
    expected = _proposal_artifact(context_id, policy, normalized)
    if canonical_json(expected) != canonical_json(value):
        raise ValueError("economic proposal artifact 不能由 context/policy/proposal 重建")
    return str(expected["artifact_id"])


def _verify_attempts(
    value: object,
    *,
    context: Mapping[str, object],
    policy: EconomicAgentPolicy,
    evaluated_at: datetime,
) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("economic agent receipt attempts 必须为非空列表")
    evidence = _context_evidence(context)
    attempts = tuple(_object(item, "attempt") for item in value)
    if [_attempt_index(item) for item in attempts] != list(range(len(attempts))):
        raise ValueError("economic agent receipt attempts input_index 不连续")
    for attempt in attempts:
        proposal_value = attempt.get("proposal")
        if proposal_value is None:
            raise ValueError("v1 运行台账不得封存 contract-invalid 原始输入")
        _validate_exact_keys(
            attempt,
            frozenset({
                "input_index", "input_sha256", "proposal_id", "proposal", "status", "reasons",
            }),
            "normalized proposal attempt",
        )
        proposal = _normalize_persisted_proposal(
            _object(proposal_value, "attempt.proposal"),
            policy.proposal_gate,
        )
        if attempt.get("proposal_id") != proposal["proposal_id"]:
            raise ValueError("attempt proposal_id 与规范提案不一致")
        if attempt.get("input_sha256") != sha256_text(canonical_json(proposal)):
            raise ValueError("attempt input_sha256 与规范提案不一致")
        expected_reasons = _proposal_context_reasons(
            proposal,
            context,
            evidence,
            policy.proposal_gate,
            evaluated_at,
        )
        if attempt.get("status") != "rejected" or attempt.get("reasons") != expected_reasons:
            raise ValueError("v1 规范提案必须按治理未绑定原因失败关闭")
    return attempts


def _verify_run_receipt_value(
    value: Mapping[str, object],
    *,
    observation_rows: Sequence[Mapping[str, object]],
    expected_policy: EconomicAgentPolicy,
) -> tuple[str, EconomicAgentPolicy, Mapping[str, object]]:
    _validate_exact_keys(
        value,
        frozenset({
            "schema_version", "artifact_type", "method_version", "evaluated_at",
            "model_identity", "prompt_identity", "input_identity", "output_identity",
            "attempts", "context", "policy", "authority", "artifact_id",
        }),
        "economic agent run receipt",
    )
    if (
        value.get("schema_version") != ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION
        or value.get("artifact_type") != "economic_agent_run_receipt"
        or value.get("method_version") != ECONOMIC_AGENT_METHOD_VERSION
    ):
        raise ValueError("economic agent run receipt 版本或类型非法")
    evaluated = _utc_time(value.get("evaluated_at"), "receipt.evaluated_at")
    inference = _inference_identity(None)
    if canonical_json(value.get("model_identity")) != canonical_json(inference):
        raise ValueError("v1 run receipt 仅允许本地 deterministic inference identity")
    expected_prompt = {
        "prompt_template_id": inference["prompt_template_id"],
        "prompt_sha256": inference["prompt_sha256"],
    }
    if value.get("prompt_identity") != expected_prompt:
        raise ValueError("run receipt prompt_identity 不能由 model_identity 重建")
    policy_payload = _object(value.get("policy"), "receipt.policy")
    policy = parse_economic_policy(policy_payload)
    if canonical_json(policy.payload()) != canonical_json(policy_payload):
        raise ValueError("run receipt policy 不是规范政策")
    if policy.policy_id != expected_policy.policy_id:
        raise ValueError("run receipt policy 未绑定当前受信政策")
    input_identity = _object(value.get("input_identity"), "receipt.input_identity")
    verified_identity = _object(
        input_identity.get("verified_observation_ledger"),
        "receipt.input_identity.verified_observation_ledger",
    )
    verified_sequence = verified_identity.get("sequence")
    if (
        isinstance(verified_sequence, bool)
        or not isinstance(verified_sequence, int)
        or verified_sequence < 0
        or verified_sequence > len(observation_rows)
    ):
        raise ValueError("run receipt 绑定的观测验证前缀非法")
    verified_rows = observation_rows[:verified_sequence]
    verified_snapshot = _observation_snapshot(verified_rows)
    verified_head = _sha256(
        verified_identity.get("head_sha256"),
        "verified_observation_ledger.head_sha256",
    )
    if verified_snapshot.ledger_head_sha256 != verified_head:
        raise ValueError("run receipt 绑定的观测验证链头不匹配")
    normalized_verified_identity = {
        "sequence": verified_snapshot.ledger_sequence,
        "head_sha256": verified_snapshot.ledger_head_sha256,
    }
    context = _object(value.get("context"), "receipt.context")
    context_id = _verify_economic_context_rows(
        context,
        verified_rows,
        policy,
        reject_eligible_tail=True,
    )
    attempts = _verify_attempts(
        value.get("attempts"),
        context=context,
        policy=policy,
        evaluated_at=evaluated,
    )
    evaluation = _ProposalEvaluation(attempts, (), (), 0)
    expected = _run_receipt(
        context=context,
        context_id=context_id,
        policy=policy,
        evaluated_at=evaluated,
        inference=inference,
        evaluation=evaluation,
        verified_observation_ledger=normalized_verified_identity,
    )
    if canonical_json(expected) != canonical_json(value):
        raise ValueError("economic agent run receipt 不能由输入与失败关闭结果重建")
    if value.get("authority") != _run_authority():
        raise ValueError("economic agent run receipt authority 非法")
    return str(expected["artifact_id"]), policy, context


_LEDGER_ROW_KEYS = frozenset({
    "record_type", "sequence", "previous_record_sha256", "record_sha256",
    "ledger_canonical_path",
    "schema_version", "method_version", "run_id", "evaluated_at",
    "receipt_artifact_id", "receipt_sha256", "receipt_storage", "receipt",
    "artifact_commitments", "research_only",
})


def _verify_agent_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    observation_rows: Sequence[Mapping[str, object]],
    expected_policy: EconomicAgentPolicy,
) -> None:
    seen_runs: set[str] = set()
    for row in rows:
        _validate_exact_keys(row, _LEDGER_ROW_KEYS, "economic agent ledger row")
        if (
            row.get("record_type") != "economic_agent_run"
            or row.get("schema_version") != ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION
            or row.get("method_version") != ECONOMIC_AGENT_METHOD_VERSION
            or row.get("research_only") is not True
        ):
            raise ValueError("economic agent ledger row 合同非法")
        receipt = _object(row.get("receipt"), "ledger.receipt")
        run_id, policy, context = _verify_run_receipt_value(
            receipt,
            observation_rows=observation_rows,
            expected_policy=expected_policy,
        )
        if run_id in seen_runs:
            raise ValueError("economic agent ledger 重复 run_id")
        seen_runs.add(run_id)
        expected_payload = _ledger_payload(receipt, ())
        payload = {
            key: item for key, item in row.items()
            if key not in {
                "record_type", "sequence", "previous_record_sha256", "record_sha256",
                "ledger_canonical_path",
            }
        }
        if canonical_json(payload) != canonical_json(expected_payload):
            raise ValueError("economic agent ledger row 不能由 run receipt 重建")
        if row.get("receipt_storage") != "embedded_in_ledger":
            raise ValueError("run receipt 必须内嵌于已验证运行台账")
        if row.get("receipt_sha256") != sha256_text(canonical_json(receipt) + "\n"):
            raise ValueError("运行台账 receipt_sha256 不一致")
        if policy.policy_id != _object(receipt["input_identity"], "input_identity").get(
            "policy_id",
        ):
            raise ValueError("run receipt policy_id 不一致")
        if _verify_artifact(context, "economic-context") != _object(
            receipt["input_identity"], "input_identity",
        ).get("context_artifact_id"):
            raise ValueError("run receipt context_artifact_id 不一致")


def run_economic_research_agent(
    *,
    context: Mapping[str, object],
    proposals: Sequence[Mapping[str, object]],
    policy: EconomicAgentPolicy,
    observation_ledger_path: Path,
    output: Path,
    ledger_path: Path,
    inference_identity: Mapping[str, object] | None = None,
) -> EconomicAgentRunResult:
    """审计全部提案；v1 在治理绑定完成前只写全拒绝回执。"""
    if not proposals:
        raise ValueError("ResearchProposal 批次不得为空")
    normalized_proposals = _normalize_proposal_batch(
        proposals,
        policy.proposal_gate,
    )
    evaluated = _utc_time(_time_text(clock.utc_now()), "evaluated_at")
    inference = _inference_identity(inference_identity)
    _canonical_artifact_root(output, require_dir=False)
    operation_state = _LedgerTransactionState()
    with _exclusive_ledger_lock(
        observation_ledger_path,
        "经济观测台账",
        require_file=True,
        transaction_state=operation_state,
    ) as observation_ledger:
        observation_rows = _read_chain(observation_ledger, "economic_observation")
        context_id = _verify_economic_context_rows(
            context,
            observation_rows,
            policy,
            reject_eligible_tail=True,
        )
        canonical_agent = _canonical_ledger_path(
            ledger_path,
            "经济代理运行台账",
            require_file=False,
        )
        if canonical_agent == observation_ledger.path:
            raise ValueError("经济观测台账与代理运行台账不得共用路径")

        def validate_observation_dependency() -> None:
            """在运行台账提交点重验观测前缀。"""
            current_rows = _read_chain(
                observation_ledger,
                "economic_observation",
            )
            if current_rows != observation_rows:
                raise ValueError("经济观测台账在运行提交前变化")

        with _exclusive_ledger_lock(
            canonical_agent,
            "经济代理运行台账",
            require_file=False,
            transaction_state=operation_state,
            precommit_validator=validate_observation_dependency,
        ) as agent_ledger:
            # 保持观测锁。
            # 直至台账 fsync。
            _canonical_ledger_path(
                observation_ledger.path,
                "经济观测台账",
                require_file=True,
            )
            ledger_rows = _read_chain(agent_ledger, "economic_agent_run")
            _verify_agent_rows(
                ledger_rows,
                observation_rows=observation_rows,
                expected_policy=policy,
            )
            evaluation = _evaluate_proposals(
                normalized_proposals,
                context=context,
                policy=policy,
                evaluated_at=evaluated,
            )
            receipt = _run_receipt(
                context=context,
                context_id=context_id,
                policy=policy,
                evaluated_at=evaluated,
                inference=inference,
                evaluation=evaluation,
                verified_observation_ledger={
                    "sequence": len(observation_rows),
                    "head_sha256": _observation_snapshot(
                        observation_rows,
                    ).ledger_head_sha256,
                },
            )
            run_id = str(receipt["artifact_id"])
            verified_run_id, _verified_policy, _verified_context = (
                _verify_run_receipt_value(
                    receipt,
                    observation_rows=observation_rows,
                    expected_policy=policy,
                )
            )
            if verified_run_id != run_id:
                raise ValueError("经济代理新 run receipt 预提交语义校验失败")
            if any(row.get("run_id") == run_id for row in ledger_rows):
                raise ValueError("相同 economic agent run 已入账")
            _append_chain_unlocked(
                agent_ledger,
                "economic_agent_run",
                ledger_rows,
                (_ledger_payload(receipt, evaluation.accepted_artifacts),),
            )
    return EconomicAgentRunResult(
        run_id=run_id,
        ledger_path=agent_ledger.path,
        proposal_paths=(),
        accepted_proposal_ids=evaluation.accepted_proposal_ids,
        rejected_count=sum(
            item["status"] == "rejected" for item in evaluation.attempts
        ),
    )


def verify_economic_agent_ledger(
    path: Path,
    *,
    observation_ledger_path: Path,
    output: Path,
    policy: EconomicAgentPolicy,
) -> tuple[Mapping[str, object], ...]:
    """锁定两本台账，逐行重建内嵌 receipt 的完整语义。"""
    _canonical_artifact_root(output, require_dir=False)
    with _exclusive_ledger_lock(
        observation_ledger_path,
        "经济观测台账",
        require_file=True,
    ) as observation_ledger:
        observation_rows = _read_chain(observation_ledger, "economic_observation")
        canonical_agent = _canonical_ledger_path(
            path,
            "经济代理运行台账",
            require_file=True,
        )
        if canonical_agent == observation_ledger.path:
            raise ValueError("经济观测台账与代理运行台账不得共用路径")
        with _exclusive_ledger_lock(
            canonical_agent,
            "经济代理运行台账",
            require_file=True,
        ) as agent_ledger:
            rows = _read_chain(agent_ledger, "economic_agent_run")
            _verify_agent_rows(
                rows,
                observation_rows=observation_rows,
                expected_policy=policy,
            )
            return rows


def load_economic_run_receipt(
    run_id: str,
    *,
    observation_ledger_path: Path,
    agent_ledger_path: Path,
    output: Path,
    policy: EconomicAgentPolicy,
) -> Mapping[str, object]:
    """按 run_id 返回已由完整语义验证的台账内嵌 receipt。"""
    requested_run_id = _identifier(run_id, "run_id")
    rows = verify_economic_agent_ledger(
        agent_ledger_path,
        observation_ledger_path=observation_ledger_path,
        output=output,
        policy=policy,
    )
    for row in rows:
        if row.get("run_id") == requested_run_id:
            return _object(row.get("receipt"), "ledger.receipt")
    raise ValueError("run_id 没有有效运行台账内嵌 receipt")


def load_economic_proposal_artifact(
    path: Path,
    *,
    context: Mapping[str, object],
    policy: EconomicAgentPolicy,
    observation_ledger_path: Path,
    agent_ledger_path: Path,
    output: Path,
) -> Mapping[str, object]:
    """v1 无 accepted proposal；仍先验证台账后失败关闭。"""
    verify_economic_agent_ledger(
        agent_ledger_path,
        observation_ledger_path=observation_ledger_path,
        output=output,
        policy=policy,
    )
    raise ValueError("v1 没有 accepted proposal 台账 commitment")
