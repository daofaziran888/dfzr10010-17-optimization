"""小文件合并结果数据集与收益复核清单的示例数据。

只依赖 ``compact_plan`` 的纯函数，保证分片统计（P50/P95）与分组数
（orders 240 分片 → 58 组）在页面、结果数据集、统计报告里完全一致。
"""
from __future__ import annotations

from typing import Any

from app.compact_plan import (
    DATASET_DONE,
    DATASET_PARTIAL,
    DATASET_PENDING,
    build_shards,
    plan_groups,
    shard_stats,
)
from app.compact_state import compact_registry


TASK_SPECS: list[dict[str, Any]] = [
    {
        "task_id": 1,
        "任务编号": "COMP-0001",
        "目标表名": "iceberg.ods.orders",
        "范围版本": "v1",
        "分区字段": "dt",
        "partitions": [f"dt={index:02d}" for index in range(29)],
        "目标大小": "64MB",
        "target_mb": 64.0,
        "并行度": 8,
        "分片数量": 240,
        "dataset_status": DATASET_DONE,
        "预执行": "全部",
        "fail_first": [],
    },
    {
        "task_id": 2,
        "任务编号": "COMP-0002",
        "目标表名": "iceberg.dwd.dwd_events",
        "范围版本": "v1",
        "分区字段": "dt",
        # dt=11 在文件范围里但当前没有任何分片，示例里用来解释空组。
        "partitions": [f"dt={index:02d}" for index in range(12)],
        "shard_partitions": [f"dt={index:02d}" for index in range(11)],
        "目标大小": "64MB",
        "target_mb": 64.0,
        "并行度": 6,
        "分片数量": 96,
        "dataset_status": DATASET_PARTIAL,
        "预执行": "前 12 组",
        "fail_first": [3, 7],
    },
    {
        "task_id": 3,
        "任务编号": "COMP-0003",
        "目标表名": "iceberg.dim.dim_user",
        "范围版本": "v1",
        "分区字段": "region",
        "partitions": [f"region={index}" for index in range(6)],
        "目标大小": "64MB",
        "target_mb": 64.0,
        "并行度": 4,
        "分片数量": 48,
        "dataset_status": DATASET_PENDING,
        "预执行": "不执行",
        "fail_first": [],
    },
]


def _build_dataset(spec: dict[str, Any], shards: list[dict[str, Any]]) -> dict[str, Any]:
    groups = plan_groups(
        shards,
        list(spec["partitions"]),
        target_mb=float(spec["target_mb"]),
        task_id=int(spec["task_id"]),
        batch=1,
    )
    fail_first = set(int(group_no) for group_no in spec.get("fail_first", []))
    for group_no in fail_first:
        index = group_no - 1
        if 0 <= index < len(groups):
            groups[index]["fail_first"] = True
            # 首跑失败、重跑成功：重试必须从原分片边界继续。
            groups[index]["max_attempts"] = 2
    return {
        "id": compact_registry.next_no("dataset"),
        "dataset_id": f"CDS-{int(spec['task_id']):04d}-v1",
        "task_id": int(spec["task_id"]),
        "任务编号": spec["任务编号"],
        "目标表名": spec["目标表名"],
        "范围版本": spec["范围版本"],
        "分区字段": spec["分区字段"],
        "partitions": list(spec["partitions"]),
        "target_mb": float(spec["target_mb"]),
        "目标大小": spec["目标大小"],
        "并行度": spec["并行度"],
        "状态": spec["dataset_status"],
        "分片统计": shard_stats(shards),
        "groups": groups,
        "batch": 1,
        "范围扩展": [],
        "过期完成项": [],
        "数据集版本": "compact_dataset/v1",
    }


def seed_compact_registry() -> None:
    """幂等初始化：服务第一次被访问时调用，避免 import 期循环依赖。"""
    if compact_registry.seeded:
        return
    compact_registry.seeded = True
    for spec in TASK_SPECS:
        task_id = int(spec["task_id"])
        shards = build_shards(
            task_id,
            count=int(spec["分片数量"]),
            partitions=list(spec.get("shard_partitions") or spec["partitions"]),
            batch=1,
        )
        compact_registry.specs[task_id] = {
            "task_id": task_id,
            "partitions": list(spec["partitions"]),
            "target_mb": float(spec["target_mb"]),
            "分片数量": int(spec["分片数量"]),
        }
        compact_registry.shards[task_id] = shards
        compact_registry.datasets[task_id] = _build_dataset(spec, shards)

    from app.services.compact_dataset import CompactDatasetService

    service = CompactDatasetService()
    for spec in TASK_SPECS:
        task_id = int(spec["task_id"])
        stage = spec["预执行"]
        if stage == "全部":
            service.run_groups(task_id)
        elif stage == "前 12 组":
            # 跑到一半主动停下，模拟“中断后可从剩余分片续跑”的现场。
            service.run_groups(task_id, limit=12)
            service.interrupt(task_id, reason="示例：执行到第 12 组后中断，可从剩余分片继续")
        service.sync_review(task_id, reason="初始化示例数据")


__all__ = ["TASK_SPECS", "seed_compact_registry"]
