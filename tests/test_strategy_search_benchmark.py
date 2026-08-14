"""CPU ValidationExact 与可选 CUDA SearchFast 的硬门禁测试。"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts import benchmark_strategy_search as benchmark


def test_cpu_benchmark_is_deterministic_without_cuda(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """无 CUDA 时仍须发布确定性 CPU 证据，并按 require_gpu 拒绝。"""
    monkeypatch.setattr(benchmark, "_torch_module", lambda: None)
    first = benchmark.run_benchmark(32, 8, 4, 2, False)
    second = benchmark.run_benchmark(32, 8, 4, 2, False)
    assert first["benchmark_method_version"] == (
        benchmark.BENCHMARK_METHOD_VERSION
    )
    assert first["scope"] == (
        "候选×时点的信号判断与收益归约；不含数据读取、特征构造、"
        "walk-forward、成本、统计门禁或制品写入"
    )
    assert first["cpu"]["result_sha256"] == second["cpu"]["result_sha256"]
    assert first["cuda"] == {
        "status": "unavailable",
        "reason": "torch_cuda_runtime_unavailable",
    }
    with pytest.raises(RuntimeError, match="CUDA 运行时不可用"):
        benchmark.run_benchmark(32, 8, 4, 1, True)


def test_cuda_parity_is_a_hard_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SearchFast 超出 CPU reference 容差时不得正常发布 benchmark。"""
    fake_torch = SimpleNamespace(
        __version__="test",
        version=SimpleNamespace(cuda="test"),
        cuda=SimpleNamespace(
            is_available=lambda: True,
            get_device_capability=lambda _index: (9, 0),
            get_device_name=lambda _index: "fake-gpu",
        ),
    )
    monkeypatch.setattr(benchmark, "_torch_module", lambda: fake_torch)
    monkeypatch.setattr(
        benchmark,
        "_cuda_search",
        lambda _torch, _scores, _returns, thresholds, _chunk: tuple(
            1.0 for _threshold in thresholds
        ),
    )
    with pytest.raises(RuntimeError, match="数值差异超出容差"):
        benchmark.run_benchmark(32, 8, 4, 1, True)


def test_cuda_parity_pass_publishes_device_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """数值通过后才发布 GPU 设备、耗时和 parity 证据。"""
    fake_torch = SimpleNamespace(
        __version__="test",
        version=SimpleNamespace(cuda="test"),
        cuda=SimpleNamespace(
            is_available=lambda: True,
            get_device_capability=lambda _index: (9, 0),
            get_device_name=lambda _index: "fake-gpu",
        ),
    )
    monkeypatch.setattr(benchmark, "_torch_module", lambda: fake_torch)
    monkeypatch.setattr(
        benchmark,
        "_cuda_search",
        lambda _torch, scores, returns, thresholds, _chunk: (
            benchmark._cpu_search(scores, returns, thresholds)
        ),
    )
    result = benchmark.run_benchmark(32, 8, 4, 1, True)
    assert result["cuda"]["status"] == "available"
    assert result["cuda"]["device"] == "fake-gpu"
    assert result["cuda"]["parity"]["passed"] is True
    assert result["cuda"]["parity"]["maximum_absolute_difference"] == 0.0
