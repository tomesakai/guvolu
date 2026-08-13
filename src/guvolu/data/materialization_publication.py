"""物化终态发布失败后的只增收束。"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from guvolu.data.durable_io import atomic_write_text
from guvolu.data.materialize import (
    _register_content_artifact,
    _relative_storage_path,
    artifact_id,
    register_materialization_manifest,
    sha256_file,
    utc_now,
)


@dataclass(frozen=True)
class UnpromotedOutput:
    """已封存但未晋升为物化输出的制品。"""

    dataset: str
    path: Path
    sha256: str
    row_count: int
    schema_version: int


def settle_failed_publication(
    root: Path,
    conn: sqlite3.Connection,
    attempt_id: str,
    manifest_path: Path,
    manifest_body: Mapping[str, object],
    outputs: tuple[UnpromotedOutput, ...],
    failure: Exception,
    manifest_schema_version: int,
) -> bool:
    """登记失败清单与未晋升制品。

    返回假表示原成功事务已提交，调用方应返回成功结果。
    """
    conn.rollback()
    status_row = conn.execute(
        "SELECT status FROM partition_attempt WHERE attempt_id=?",
        (attempt_id,),
    ).fetchone()
    if status_row is None:
        raise ValueError(f"物化尝试不存在: {attempt_id}")
    status = str(status_row[0])
    if status in {"complete", "complete_with_rejections"}:
        return False
    if status != "running":
        raise ValueError(f"物化尝试不能收束为失败: {attempt_id}/{status}")
    if not outputs:
        raise ValueError("失败发布没有可登记输出")

    failed_at = utc_now()
    output_rows: list[dict[str, object]] = []
    for output in outputs:
        if not output.path.is_file():
            raise FileNotFoundError(output.path)
        actual_sha = sha256_file(output.path)
        if actual_sha != output.sha256:
            raise ValueError(f"失败输出散列不符: {output.path}")
        output_rows.append({
            "artifact_id": artifact_id(actual_sha),
            "dataset": output.dataset,
            "output": _relative_storage_path(root, output.path),
            "row_count": output.row_count,
            "schema_version": output.schema_version,
            "sha256": actual_sha,
        })

    failed_body = dict(manifest_body)
    failed_body.update({
        "status": "failed",
        "failed_at": failed_at,
        "failure_detail": str(failure)[:2000],
        "non_promoted_outputs": output_rows,
    })
    atomic_write_text(
        manifest_path,
        json.dumps(failed_body, ensure_ascii=False, indent=2) + "\n",
    )

    conn.execute("BEGIN IMMEDIATE")
    try:
        for output, row in zip(outputs, output_rows, strict=True):
            _register_content_artifact(
                conn,
                str(row["artifact_id"]),
                "materialized_parquet",
                str(row["output"]),
                output.sha256,
                output.path.stat().st_size,
                failed_at,
                output.schema_version,
            )
        changed = conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (failed_at, str(failure)[:2000], attempt_id),
        ).rowcount
        if changed != 1:
            raise ValueError(f"物化失败状态未收束: {attempt_id}")
        register_materialization_manifest(
            root,
            conn,
            manifest_path,
            manifest_schema_version,
            failed_at,
            artifact_kind="failed_materialization_manifest",
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return True
