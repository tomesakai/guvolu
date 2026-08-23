"""搜索束张量化、内容寻址身份与序列化测试（纯 CPU，无需 Torch）。"""
from __future__ import annotations

import math
from dataclasses import replace
from pathlib import Path

import pytest

from guvolu.search.bundle import (
    KERNEL_METHOD_VERSION,
    SearchBundleIdentity,
    build_search_bundle,
    bundle_identifier,
    evaluation_identifier,
    load_search_bundle,
    validate_identity,
    write_search_bundle,
)
from guvolu.search.synthetic import synthetic_panel, synthetic_strategy_config
from guvolu.search.tensorize import (
    MASK_SEMANTICS,
    panel_columns,
    panel_sha256,
    round_to_f32,
    tensorize_panel,
    window_column,
)
from guvolu.strategy.generation import (
    build_family_batches,
    candidate_search_plan_payload,
)

LOOKBACKS = (4, 8)
COST_MODEL = {
    "one_way_cost_rate": 0.001,
    "maximum_gap_seconds": None,
    "periods_per_year": 8760.0,
}
FOLD_SPEC = {"method": "full_sample"}
BOOTSTRAP = {"seed": 7, "block": 24, "paths": 0}


def _bundle(bars: int = 64, seed: int = 1):
    """构造覆盖六流派的合成搜索束。"""
    bar_rows, feature_rows = synthetic_panel(bars, LOOKBACKS, seed)
    panel = tensorize_panel(bar_rows, feature_rows, LOOKBACKS)
    plan = candidate_search_plan_payload(
        build_family_batches(synthetic_strategy_config(LOOKBACKS)),
    )
    return build_search_bundle(
        panel, plan, COST_MODEL, FOLD_SPEC, BOOTSTRAP,
        feature_method_version="research-features-v2",
        code_tree_digest="0" * 64,
    )


def test_tensorize_exports_fixed_columns_and_masks_without_filling() -> None:
    """缺失只转 NaN 加零掩码，不得插值或前向填充。"""
    bar_rows, feature_rows = synthetic_panel(96, LOOKBACKS, 3)
    panel = tensorize_panel(bar_rows, feature_rows, LOOKBACKS)
    assert panel.columns == panel_columns(LOOKBACKS)
    assert panel.bar_count == 96
    for index, feature in enumerate(feature_rows):
        for lookback in LOOKBACKS:
            values, masks = panel.column(window_column("trend_score", lookback))
            expected = feature.trend_scores[lookback]
            if expected is None:
                assert masks[index] == 0 and math.isnan(values[index])
            else:
                assert masks[index] == 1
                assert values[index] == round_to_f32(expected)
        gate_values, _gate_masks = panel.column("gate_open")
        expected_gate = feature.as_of <= feature.decision_time and feature.contiguous
        assert gate_values[index] == (1.0 if expected_gate else 0.0)
    log_values, _log_masks = panel.column("log_return")
    assert log_values[0] == 0.0
    assert log_values[5] == round_to_f32(
        math.log(bar_rows[5].close / bar_rows[4].close),
    )


def test_panel_sha256_changes_with_any_cell() -> None:
    """任一数值或掩码变化都必须改变面板散列。"""
    bar_rows, feature_rows = synthetic_panel(32, LOOKBACKS, 5)
    panel = tensorize_panel(bar_rows, feature_rows, LOOKBACKS)
    digest = panel_sha256(panel)
    values, _masks = panel.column("close")
    values[3] = values[3] + 1.0
    assert panel_sha256(panel) != digest


def test_bundle_identity_changes_with_every_field(tmp_path: Path) -> None:
    """十一字段任一变化即为新的搜索束，evaluation_id 随之变化。"""
    bundle = _bundle()
    identity = bundle.identity
    validate_identity(identity)
    base = bundle_identifier(identity)
    variants = {
        "panel_sha256": replace(identity, panel_sha256="f" * 64),
        "feature_method_version": replace(identity, feature_method_version="x-v9"),
        "columns": replace(identity, columns=identity.columns[:-1]),
        "mask_semantics": replace(identity, mask_semantics=MASK_SEMANTICS + "x"),
        "search_plan_id": replace(identity, search_plan_id="search-plan-" + "1" * 64),
        "cost_model_hash": replace(identity, cost_model_hash="2" * 64),
        "fold_spec": replace(identity, fold_spec={"method": "walk_forward"}),
        "bootstrap": replace(identity, bootstrap={"seed": 8, "block": 24, "paths": 0}),
        "kernel_method_version": replace(identity, kernel_method_version="k-v2"),
        "code_tree_digest": replace(identity, code_tree_digest="3" * 64),
    }
    seen = {base}
    for name, variant in variants.items():
        if name == "mask_semantics":
            with pytest.raises(ValueError, match="掩码语义"):
                bundle_identifier(variant)
            continue
        identifier = bundle_identifier(variant)
        assert identifier not in seen, name
        seen.add(identifier)
        assert evaluation_identifier("candidate-a", variant) != (
            evaluation_identifier("candidate-a", identity)
        )
    with pytest.raises(ValueError, match="dtype"):
        bundle_identifier(replace(identity, dtype="f64"))
    assert evaluation_identifier("candidate-a", identity) != (
        evaluation_identifier("candidate-b", identity)
    )


def test_bundle_identity_rejects_missing_fields() -> None:
    """身份字段缺一即拒绝。"""
    bundle = _bundle()
    identity = bundle.identity
    with pytest.raises(ValueError, match="code_tree_digest"):
        validate_identity(replace(identity, code_tree_digest=""))
    with pytest.raises(ValueError, match="bootstrap 缺少字段"):
        validate_identity(replace(identity, bootstrap={"seed": 1}))
    with pytest.raises(ValueError, match="fold_spec"):
        validate_identity(replace(identity, fold_spec={}))
    assert identity.kernel_method_version == KERNEL_METHOD_VERSION


def test_bundle_round_trip_is_content_addressed(tmp_path: Path) -> None:
    """写出后重新加载须得到同一身份，篡改数组文件必须被拒绝。"""
    bundle = _bundle()
    directory = write_search_bundle(bundle, tmp_path)
    assert directory.name == bundle.bundle_id
    loaded = load_search_bundle(directory)
    assert loaded.bundle_id == bundle.bundle_id
    assert loaded.identity == bundle.identity
    assert loaded.panel.columns == bundle.panel.columns
    assert loaded.panel.decision_times == bundle.panel.decision_times
    for name in bundle.panel.columns:
        values, masks = bundle.panel.column(name)
        loaded_values, loaded_masks = loaded.panel.column(name)
        assert loaded_masks.tolist() == masks.tolist()
        for left, right in zip(values, loaded_values, strict=True):
            assert (math.isnan(left) and math.isnan(right)) or left == right
    assert loaded.search_plan["search_plan_id"] == bundle.search_plan["search_plan_id"]
    array_files = sorted((directory / "arrays").glob("*.bin"))
    assert array_files
    target = array_files[0]
    body = bytearray(target.read_bytes())
    body[0] ^= 0xFF
    target.write_bytes(bytes(body))
    with pytest.raises(ValueError, match="散列不匹配"):
        load_search_bundle(directory)


def test_bundle_identity_type_is_frozen_dataclass() -> None:
    """身份对象不可变，保证散列稳定。"""
    bundle = _bundle()
    assert isinstance(bundle.identity, SearchBundleIdentity)
    with pytest.raises(AttributeError):
        bundle.identity.dtype = "f64"  # type: ignore[misc]
