"""内容寻址搜索束：身份、评估身份、序列化与加载。

搜索束只接受 CPU 导出的 f32 数组与掩码，不解析 raw JSON（禁区第 1 条）。
"""
from __future__ import annotations

import json
from array import array
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from guvolu.data.durable_io import atomic_write_bytes, atomic_write_text
from guvolu.search.identity import canonical_json, sha256_bytes, sha256_text
from guvolu.search.tensorize import (
    MASK_SEMANTICS,
    PANEL_DTYPE,
    PanelTensor,
    array_bytes,
    array_from_bytes,
    panel_sha256,
)
from guvolu.strategy.generation import SEARCH_PLAN_METHOD_VERSION

BUNDLE_SCHEMA_VERSION = 1
BUNDLE_METHOD_VERSION = "searchfast-bundle-v1"
KERNEL_METHOD_VERSION = "searchfast-torch-dag-scan-v1"
IDENTITY_FIELDS = (
    "panel_sha256",
    "feature_method_version",
    "columns",
    "dtype",
    "mask_semantics",
    "search_plan_id",
    "cost_model_hash",
    "fold_spec",
    "bootstrap",
    "kernel_method_version",
    "code_tree_digest",
)
_BOOTSTRAP_FIELDS = ("seed", "block", "paths")


@dataclass(frozen=True)
class SearchBundleIdentity:
    """搜索束身份的十一个字段。"""

    panel_sha256: str
    feature_method_version: str
    columns: tuple[str, ...]
    dtype: str
    mask_semantics: str
    search_plan_id: str
    cost_model_hash: str
    fold_spec: Mapping[str, object]
    bootstrap: Mapping[str, object]
    kernel_method_version: str
    code_tree_digest: str

    def payload(self) -> Mapping[str, object]:
        """生成规范身份载荷。"""
        return {
            "panel_sha256": self.panel_sha256,
            "feature_method_version": self.feature_method_version,
            "columns": list(self.columns),
            "dtype": self.dtype,
            "mask_semantics": self.mask_semantics,
            "search_plan_id": self.search_plan_id,
            "cost_model_hash": self.cost_model_hash,
            "fold_spec": dict(self.fold_spec),
            "bootstrap": dict(self.bootstrap),
            "kernel_method_version": self.kernel_method_version,
            "code_tree_digest": self.code_tree_digest,
        }


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"搜索束身份字段缺失或为空: {name}")
    return value


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证对象字段。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"搜索束身份字段必须为对象: {name}")
    return {str(key): item for key, item in value.items()}


def validate_identity(identity: SearchBundleIdentity) -> None:
    """身份字段缺一即拒绝。"""
    payload = identity.payload()
    for name in IDENTITY_FIELDS:
        if name not in payload:
            raise ValueError(f"搜索束身份字段缺失: {name}")
    for name in (
        "panel_sha256",
        "feature_method_version",
        "dtype",
        "mask_semantics",
        "search_plan_id",
        "cost_model_hash",
        "kernel_method_version",
        "code_tree_digest",
    ):
        _text(payload[name], name)
    if identity.dtype != PANEL_DTYPE:
        raise ValueError(f"搜索束 dtype 必须为 {PANEL_DTYPE}")
    if identity.mask_semantics != MASK_SEMANTICS:
        raise ValueError("搜索束掩码语义不受支持")
    if not identity.columns:
        raise ValueError("搜索束身份字段缺失或为空: columns")
    fold_spec = _object(identity.fold_spec, "fold_spec")
    if not fold_spec:
        raise ValueError("搜索束身份字段缺失或为空: fold_spec")
    bootstrap = _object(identity.bootstrap, "bootstrap")
    for name in _BOOTSTRAP_FIELDS:
        if name not in bootstrap:
            raise ValueError(f"搜索束 bootstrap 缺少字段: {name}")


def bundle_identifier(identity: SearchBundleIdentity) -> str:
    """由规范身份 JSON 生成搜索束标识。"""
    validate_identity(identity)
    return "search-bundle-" + sha256_text(canonical_json(identity.payload()))


def evaluation_identifier(
    candidate_id: str,
    identity: SearchBundleIdentity,
) -> str:
    """evaluation_id 绑定候选身份与搜索束身份。"""
    if not candidate_id:
        raise ValueError("candidate_id 不得为空")
    body = canonical_json({
        "candidate_id": candidate_id,
        "search_bundle_identity": identity.payload(),
    })
    return "evaluation-" + sha256_text(body)


def cost_model_hash(cost_model: Mapping[str, object]) -> str:
    """散列成本模型配置。"""
    return sha256_text(canonical_json(dict(cost_model)))


@dataclass(frozen=True)
class SearchBundle:
    """只读搜索束：身份、面板、计划与评估配置。"""

    bundle_id: str
    identity: SearchBundleIdentity
    panel: PanelTensor
    search_plan: Mapping[str, object]
    cost_model: Mapping[str, object]


def _plan_identifier(plan: Mapping[str, object]) -> str:
    """读取并验证 SearchPlan 身份。"""
    if plan.get("search_plan_method_version") != SEARCH_PLAN_METHOD_VERSION:
        raise ValueError("SearchPlan 方法版本不受支持")
    return _text(plan.get("search_plan_id"), "search_plan_id")


def build_search_bundle(
    panel: PanelTensor,
    search_plan: Mapping[str, object],
    cost_model: Mapping[str, object],
    fold_spec: Mapping[str, object],
    bootstrap: Mapping[str, object],
    feature_method_version: str,
    code_tree_digest: str,
    kernel_method_version: str = KERNEL_METHOD_VERSION,
) -> SearchBundle:
    """由 CPU 导出的面板与计划构造搜索束。"""
    identity = SearchBundleIdentity(
        panel_sha256=panel_sha256(panel),
        feature_method_version=feature_method_version,
        columns=tuple(panel.columns),
        dtype=PANEL_DTYPE,
        mask_semantics=MASK_SEMANTICS,
        search_plan_id=_plan_identifier(search_plan),
        cost_model_hash=cost_model_hash(cost_model),
        fold_spec=dict(fold_spec),
        bootstrap=dict(bootstrap),
        kernel_method_version=kernel_method_version,
        code_tree_digest=code_tree_digest,
    )
    return SearchBundle(
        bundle_id=bundle_identifier(identity),
        identity=identity,
        panel=panel,
        search_plan=search_plan,
        cost_model=dict(cost_model),
    )


def _write_array(
    directory: Path,
    values: array[float] | array[int],
) -> Mapping[str, object]:
    """以内容散列命名写入数组文件。"""
    body = array_bytes(values)
    digest = sha256_bytes(body)
    path = directory / f"{digest}.bin"
    if not path.exists():
        atomic_write_bytes(path, body)
    return {
        "file": f"arrays/{digest}.bin",
        "sha256": digest,
        "typecode": values.typecode,
        "count": len(values),
    }


def write_search_bundle(bundle: SearchBundle, root: Path) -> Path:
    """把搜索束写为 manifest 与内容寻址数组文件。"""
    directory = root / bundle.bundle_id
    arrays_directory = directory / "arrays"
    arrays_directory.mkdir(parents=True, exist_ok=True)
    array_records: dict[str, object] = {}
    for name in bundle.panel.columns:
        values, masks = bundle.panel.column(name)
        array_records[name] = {
            "values": _write_array(arrays_directory, values),
            "mask": _write_array(arrays_directory, masks),
        }
    manifest: dict[str, object] = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "bundle_method_version": BUNDLE_METHOD_VERSION,
        "bundle_id": bundle.bundle_id,
        "identity": bundle.identity.payload(),
        "cost_model": dict(bundle.cost_model),
        "panel": {
            "bar_count": bundle.panel.bar_count,
            "lookbacks": list(bundle.panel.lookbacks),
            "columns": list(bundle.panel.columns),
            "decision_times": list(bundle.panel.decision_times),
            "byte_order": "little",
            "arrays": array_records,
        },
        "search_plan": dict(bundle.search_plan),
    }
    atomic_write_text(
        directory / "manifest.json",
        canonical_json(manifest) + "\n",
    )
    return directory


def _read_array(
    directory: Path,
    record: Mapping[str, object],
    typecode: str,
) -> array[float] | array[int]:
    """读取并校验一个数组文件。"""
    relative = _text(record.get("file"), "array.file")
    expected = _text(record.get("sha256"), "array.sha256")
    if record.get("typecode") != typecode:
        raise ValueError("搜索束数组类型码不一致")
    body = (directory / relative).read_bytes()
    if sha256_bytes(body) != expected:
        raise ValueError(f"搜索束数组散列不匹配: {relative}")
    values = array_from_bytes(typecode, body)
    if len(values) != record.get("count"):
        raise ValueError(f"搜索束数组长度不一致: {relative}")
    return values


def _identity_from_payload(payload: Mapping[str, object]) -> SearchBundleIdentity:
    """由 manifest 载荷重建身份。"""
    columns = payload.get("columns")
    if not isinstance(columns, list):
        raise ValueError("搜索束身份字段必须为数组: columns")
    return SearchBundleIdentity(
        panel_sha256=_text(payload.get("panel_sha256"), "panel_sha256"),
        feature_method_version=_text(
            payload.get("feature_method_version"), "feature_method_version",
        ),
        columns=tuple(str(item) for item in columns),
        dtype=_text(payload.get("dtype"), "dtype"),
        mask_semantics=_text(payload.get("mask_semantics"), "mask_semantics"),
        search_plan_id=_text(payload.get("search_plan_id"), "search_plan_id"),
        cost_model_hash=_text(payload.get("cost_model_hash"), "cost_model_hash"),
        fold_spec=_object(payload.get("fold_spec"), "fold_spec"),
        bootstrap=_object(payload.get("bootstrap"), "bootstrap"),
        kernel_method_version=_text(
            payload.get("kernel_method_version"), "kernel_method_version",
        ),
        code_tree_digest=_text(payload.get("code_tree_digest"), "code_tree_digest"),
    )


def load_search_bundle(directory: Path) -> SearchBundle:
    """加载并逐项校验搜索束。"""
    manifest = _object(
        json.loads((directory / "manifest.json").read_text(encoding="utf-8")),
        "manifest",
    )
    if manifest.get("bundle_method_version") != BUNDLE_METHOD_VERSION:
        raise ValueError("搜索束方法版本不受支持")
    identity = _identity_from_payload(_object(manifest.get("identity"), "identity"))
    panel_record = _object(manifest.get("panel"), "panel")
    if panel_record.get("byte_order") != "little":
        raise ValueError("搜索束字节序不受支持")
    raw_columns = panel_record.get("columns")
    raw_lookbacks = panel_record.get("lookbacks")
    raw_times = panel_record.get("decision_times")
    if (
        not isinstance(raw_columns, list)
        or not isinstance(raw_lookbacks, list)
        or not isinstance(raw_times, list)
    ):
        raise ValueError("搜索束面板元数据非法")
    columns = tuple(str(item) for item in raw_columns)
    arrays = _object(panel_record.get("arrays"), "panel.arrays")
    values: dict[str, array[float]] = {}
    masks: dict[str, array[int]] = {}
    for name in columns:
        record = _object(arrays.get(name), f"panel.arrays.{name}")
        value_array = _read_array(
            directory, _object(record.get("values"), "values"), "f",
        )
        mask_array = _read_array(
            directory, _object(record.get("mask"), "mask"), "b",
        )
        values[name] = array("f", value_array.tolist())
        masks[name] = array("b", [int(item) for item in mask_array])
    bar_count = panel_record.get("bar_count")
    if not isinstance(bar_count, int) or isinstance(bar_count, bool):
        raise ValueError("搜索束 bar_count 非法")
    panel = PanelTensor(
        columns=columns,
        bar_count=bar_count,
        lookbacks=tuple(int(item) for item in raw_lookbacks),
        values=values,
        masks=masks,
        decision_times=tuple(str(item) for item in raw_times),
    )
    for name in columns:
        if len(values[name]) != bar_count or len(masks[name]) != bar_count:
            raise ValueError(f"搜索束列长度与柱数不一致: {name}")
    if panel_sha256(panel) != identity.panel_sha256:
        raise ValueError("搜索束面板散列与身份不一致")
    if tuple(identity.columns) != columns:
        raise ValueError("搜索束列清单与身份不一致")
    search_plan = _object(manifest.get("search_plan"), "search_plan")
    if _plan_identifier(search_plan) != identity.search_plan_id:
        raise ValueError("搜索束 SearchPlan 身份不一致")
    cost_model = _object(manifest.get("cost_model"), "cost_model")
    if cost_model_hash(cost_model) != identity.cost_model_hash:
        raise ValueError("搜索束成本模型散列不一致")
    bundle_id = bundle_identifier(identity)
    if manifest.get("bundle_id") != bundle_id or directory.name != bundle_id:
        raise ValueError("搜索束标识与身份不一致")
    return SearchBundle(
        bundle_id=bundle_id,
        identity=identity,
        panel=panel,
        search_plan=search_plan,
        cost_model=cost_model,
    )
