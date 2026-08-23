"""可选 Torch 运行时：按需加载，主研究环境无需安装。"""
from __future__ import annotations

import importlib
import subprocess
from collections.abc import Mapping
from types import ModuleType
from typing import Any, TypeAlias

Tensor: TypeAlias = Any


def torch_module_or_none() -> ModuleType | None:
    """按需加载 Torch，缺失时返回空。"""
    try:
        return importlib.import_module("torch")
    except ModuleNotFoundError:
        return None


def torch_module() -> ModuleType:
    """加载 Torch，缺失时明确报错。"""
    module = torch_module_or_none()
    if module is None:
        raise RuntimeError("SearchFast 需要 torch，当前环境未安装")
    return module


def cuda_available() -> bool:
    """判断 Torch CUDA 运行时是否可用。"""
    module = torch_module_or_none()
    return bool(module is not None and bool(module.cuda.is_available()))


def resolve_device(requested: str | None) -> str:
    """解析设备名；auto 优先 CUDA。"""
    if requested in (None, "auto"):
        return "cuda" if cuda_available() else "cpu"
    if requested == "cuda" and not cuda_available():
        raise RuntimeError("请求 CUDA 设备，但 Torch CUDA 运行时不可用")
    return str(requested)


def _driver_version() -> str | None:
    """读取显卡驱动版本，读取失败返回空。"""
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=driver_version",
                "--format=csv,noheader",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    text = completed.stdout.strip().splitlines()
    return text[0].strip() if text else None


def runtime_identity(device: str) -> Mapping[str, object]:
    """记录 Torch、CUDA、驱动与设备信息（G-03）。"""
    module = torch_module()
    payload: dict[str, object] = {
        "device": device,
        "torch_version": str(module.__version__),
        "torch_cuda_version": (
            None if module.version.cuda is None else str(module.version.cuda)
        ),
        "cuda_available": bool(module.cuda.is_available()),
    }
    if device.startswith("cuda") and bool(module.cuda.is_available()):
        index = module.cuda.current_device()
        properties = module.cuda.get_device_properties(index)
        payload.update({
            "device_name": str(properties.name),
            "compute_capability": f"{properties.major}.{properties.minor}",
            "total_memory_bytes": int(properties.total_memory),
            "driver_version": _driver_version(),
            "cudnn_version": (
                None if module.backends.cudnn.version() is None
                else int(module.backends.cudnn.version())
            ),
        })
    return payload
