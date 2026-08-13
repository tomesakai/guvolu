"""比较类型化阈值候选在 CPU reference 与 CUDA SearchFast 上的结果。"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import math
import statistics
import struct
import time
from array import array
from pathlib import Path
from types import ModuleType
from typing import Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.strategy.expression import expression_id, strategy_expression

BENCHMARK_METHOD_VERSION = "typed-searchfast-threshold-grid-v1"


def _inputs(
    bars: int,
    candidates: int,
) -> tuple[array[float], array[float], array[float]]:
    """构造确定性 f32 特征、下一期收益和参数轴。"""
    if bars < 2 or candidates < 2:
        raise ValueError("bars 与 candidates 均须至少为二")
    scores = array("f", (
        math.sin(index * 0.013) + 0.25 * math.cos(index * 0.003)
        for index in range(bars)
    ))
    returns = array("f", (
        0.0005 * math.sin((index + 1) * 0.021)
        + 0.0002 * math.cos((index + 1) * 0.005)
        for index in range(bars)
    ))
    thresholds = array("f", (
        -1.25 + 2.5 * index / (candidates - 1)
        for index in range(candidates)
    ))
    return scores, returns, thresholds


def _cpu_search(
    scores: Sequence[float],
    returns: Sequence[float],
    thresholds: Sequence[float],
) -> tuple[float, ...]:
    """按固定顺序执行 CPU ValidationExact 参考归约。"""
    return tuple(
        math.fsum(
            value_return
            for value_score, value_return in zip(scores, returns, strict=True)
            if value_score >= threshold
        )
        for threshold in thresholds
    )


def _torch_module() -> ModuleType | None:
    """按需加载可选 Torch，主研究环境无需安装。"""
    try:
        return importlib.import_module("torch")
    except ModuleNotFoundError:
        return None


def _cuda_search(
    torch: ModuleType,
    scores: Sequence[float],
    returns: Sequence[float],
    thresholds: Sequence[float],
    candidate_chunk: int,
) -> tuple[float, ...]:
    """以分块 CUDA f32 执行 SearchFast，避免长核与显存尖峰。"""
    if candidate_chunk <= 0:
        raise ValueError("candidate_chunk 必须为正")
    score_tensor = torch.tensor(scores, dtype=torch.float32, device="cuda")
    return_tensor = torch.tensor(returns, dtype=torch.float32, device="cuda")
    threshold_tensor = torch.tensor(
        thresholds,
        dtype=torch.float32,
        device="cuda",
    )
    result: list[float] = []
    for start in range(0, len(thresholds), candidate_chunk):
        chunk = threshold_tensor[start:start + candidate_chunk]
        active = score_tensor.unsqueeze(0) >= chunk.unsqueeze(1)
        values = (active * return_tensor.unsqueeze(0)).sum(dim=1)
        result.extend(float(value) for value in values.cpu().tolist())
    torch.cuda.synchronize()
    return tuple(result)


def _timed(callable_: object, repeats: int) -> tuple[object, tuple[float, ...]]:
    """返回最后结果及多次墙钟耗时。"""
    if repeats <= 0 or not callable(callable_):
        raise ValueError("repeats 必须为正且 callable_ 必须可调用")
    timings: list[float] = []
    result: object = None
    for _repeat in range(repeats):
        started = time.perf_counter()
        result = callable_()
        timings.append(time.perf_counter() - started)
    return result, tuple(timings)


def _result_sha256(values: Sequence[float]) -> str:
    """散列 f64 参考结果。"""
    digest = hashlib.sha256()
    for value in values:
        digest.update(struct.pack("<d", float(value)))
    return digest.hexdigest()


def run_benchmark(
    bars: int,
    candidates: int,
    candidate_chunk: int,
    repeats: int,
    require_gpu: bool,
) -> dict[str, object]:
    """执行 CPU 基准，并在可用时执行 CUDA 一致性与性能基准。"""
    scores, returns, thresholds = _inputs(bars, candidates)
    cpu_result_raw, cpu_timings = _timed(
        lambda: _cpu_search(scores, returns, thresholds),
        repeats,
    )
    if not isinstance(cpu_result_raw, tuple):
        raise TypeError("CPU 基准返回类型错误")
    cpu_result = tuple(float(value) for value in cpu_result_raw)
    payload: dict[str, object] = {
        "schema_version": 1,
        "benchmark_method_version": BENCHMARK_METHOD_VERSION,
        "expression_id": expression_id(strategy_expression("trend")),
        "formula": "sum(next_return where trend_score >= entry_threshold)",
        "inputs": {
            "bars": bars,
            "candidates": candidates,
            "candidate_chunk": candidate_chunk,
            "operations": bars * candidates,
            "input_precision": "f32",
        },
        "cpu": {
            "accumulation": "ordered_fsum_f64",
            "timings_seconds": list(cpu_timings),
            "median_seconds": statistics.median(cpu_timings),
            "result_sha256": _result_sha256(cpu_result),
        },
        "scope": (
            "候选×时点的信号判断与收益归约；不含数据读取、特征构造、"
            "walk-forward、成本、统计门禁或制品写入"
        ),
    }
    torch = _torch_module()
    cuda_available = bool(
        torch is not None
        and bool(torch.cuda.is_available())
    )
    if not cuda_available:
        payload["cuda"] = {
            "status": "unavailable",
            "reason": "torch_cuda_runtime_unavailable",
        }
        if require_gpu:
            raise RuntimeError("请求 GPU 基准，但 Torch CUDA 运行时不可用")
        return payload
    assert torch is not None
    _cuda_search(
        torch,
        scores,
        returns,
        thresholds,
        candidate_chunk,
    )
    gpu_result_raw, gpu_timings = _timed(
        lambda: _cuda_search(
            torch,
            scores,
            returns,
            thresholds,
            candidate_chunk,
        ),
        repeats,
    )
    if not isinstance(gpu_result_raw, tuple):
        raise TypeError("GPU 基准返回类型错误")
    gpu_result = tuple(float(value) for value in gpu_result_raw)
    differences = [
        abs(left - right)
        for left, right in zip(cpu_result, gpu_result, strict=True)
    ]
    maximum_absolute_difference = max(differences, default=0.0)
    maximum_reference = max((abs(value) for value in cpu_result), default=0.0)
    tolerance = max(1e-6, maximum_reference * 2e-5)
    gpu_median = statistics.median(gpu_timings)
    capability = torch.cuda.get_device_capability(0)
    payload["cuda"] = {
        "status": "available",
        "device": torch.cuda.get_device_name(0),
        "compute_capability": f"{capability[0]}.{capability[1]}",
        "torch_version": str(torch.__version__),
        "torch_cuda_version": str(torch.version.cuda),
        "accumulation": "cuda_f32_reduction",
        "timings_seconds": list(gpu_timings),
        "median_seconds": gpu_median,
        "speedup_over_cpu_median": statistics.median(cpu_timings) / gpu_median,
        "result_sha256": _result_sha256(gpu_result),
        "parity": {
            "maximum_absolute_difference": maximum_absolute_difference,
            "tolerance": tolerance,
            "passed": maximum_absolute_difference <= tolerance,
        },
    }
    return payload


def main() -> None:
    """解析参数并打印、可选持久化基准 JSON。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bars", type=int, default=8192)
    parser.add_argument("--candidates", type=int, default=4096)
    parser.add_argument("--candidate-chunk", type=int, default=512)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--require-gpu", action="store_true")
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    payload = run_benchmark(
        arguments.bars,
        arguments.candidates,
        arguments.candidate_chunk,
        arguments.repeats,
        arguments.require_gpu,
    )
    text = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    ) + "\n"
    if arguments.output is not None:
        atomic_write_text(arguments.output.resolve(), text)
    print(text, end="")


if __name__ == "__main__":
    main()
