"""小文件合并的分片生成、分组计划与收益汇总。

这里只放纯函数：给定分片清单和目标分组大小，算出稳定的分组边界；
给定分组执行结果，算出文件数量下降与容量下降。运行期状态由
``app.compact_state`` 保存，示例数据由 ``app.compact_seed`` 组装。

边界口径：
- 每个分片有全局递增的 ``序号``；
- 分组记录 ``起始分片``/``结束分片``（shard_id）与 ``起始序号``/``结束序号``；
- 这两个边界在计划生成后不再改变，失败重试与中断续跑都从原边界开始。
"""
from __future__ import annotations

from typing import Any, Iterable

GROUP_PENDING = "待执行"
GROUP_RUNNING = "执行中"
GROUP_SUCCESS = "成功"
GROUP_FAILED = "失败"
GROUP_EMPTY = "空组"
GROUP_SKIPPED = "无需合并"

DATASET_PENDING = "待执行"
DATASET_RUNNING = "执行中"
DATASET_INTERRUPTED = "已中断"
DATASET_PARTIAL = "部分失败"
DATASET_DONE = "已完成"
DATASET_CONFLICT = "范围冲突"
DATASET_STALE = "已过期"

TERMINAL_GROUP_STATUSES = (GROUP_SUCCESS, GROUP_SKIPPED, GROUP_EMPTY, GROUP_FAILED)
ACCOUNTED_GROUP_STATUSES = (GROUP_SUCCESS, GROUP_SKIPPED)

# 文件大小分位锚点（百分位, MB），用来生成可复现的分片统计。
# 240 个分片下 P50=9MB、P95=21MB，与 orders 的示例口径一致。
SIZE_ANCHORS_DEFAULT = [
    (0, 8), (10, 8), (25, 9), (50, 9), (75, 13),
    (90, 18), (95, 21), (99, 24), (100, 25),
]

# 每 MB 分片整理后可回收的小文件元数据/校验开销系数。
RECLAIM_RATIO = 0.08


def quantile_size(percent: float, anchors: Iterable[tuple[float, float]]) -> float:
    """按分位锚点做线性插值，返回一个确定性的分片大小。"""
    points = list(anchors)
    for (p0, v0), (p1, v1) in zip(points, points[1:]):
        if p0 <= percent <= p1:
            if p1 == p0:
                return float(v1)
            return v0 + (v1 - v0) * (percent - p0) / (p1 - p0)
    return float(points[-1][1])


def build_shard_sizes(count: int, anchors: Iterable[tuple[float, float]]) -> list[float]:
    """生成 count 个分片大小（升序、0.5MB 对齐），便于得到稳定分位。"""
    sizes = [
        round(quantile_size((index + 0.5) / count * 100, anchors) * 2) / 2
        for index in range(count)
    ]
    return sorted(sizes)


def build_shards(
    task_id: int,
    *,
    count: int,
    partitions: list[str],
    anchors: Iterable[tuple[float, float]] = SIZE_ANCHORS_DEFAULT,
    id_start: int = 1,
    shard_no_start: int = 0,
    batch: int = 1,
    partition_offsets: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """把 count 个分片轮转摊到各分区，分片编号、大小与全局序号保持可复现。

    轮转分配让每个分区都覆盖小、中、大分片，避免某个分区全是超大分片
    导致分组数抖动；分组边界由 ``next_fit`` 在分区内按分片序号计算。
    ``partition_offsets`` 用于范围扩展：迟到文件接着该分区已有的序号往下排，
    不会插到已有分片前面打乱原边界。
    """
    if not partitions:
        partitions = ["default"]
    sizes = build_shard_sizes(count, anchors)
    rows: list[dict[str, Any]] = []
    per_partition: dict[str, int] = dict(partition_offsets or {})
    for offset in range(count):
        size = sizes[offset]
        global_no = shard_no_start + offset
        partition = partitions[global_no % len(partitions)]
        local_no = per_partition.get(partition, 0)
        per_partition[partition] = local_no + 1
        rows.append(
            {
                "id": id_start + offset,
                "task_id": int(task_id),
                # 分片编号全局唯一，方便在日志里直接定位。
                "shard_id": f"part-{global_no:05d}",
                # 分区内序号决定分组顺序与“原分片边界”落在哪里。
                "序号": local_no,
                "全局序号": global_no,
                "分区": partition,
                "大小MB": size,
                # 合并时可回收的重复校验数据/小文件元数据开销，按分片容量比例估算。
                "可回收容量MB": round(size * RECLAIM_RATIO, 1) if size > 0 else 0.0,
                "文件数": 1,
                "状态": "待合并",
                "批次": batch,
            }
        )
    return rows


def next_fit(sizes: list[float], target: float) -> list[tuple[int, int]]:
    """按顺序做 next-fit 切分，返回每组在 sizes 里的下标区间（左闭右闭）。"""
    groups: list[tuple[int, int]] = []
    start = 0
    total = 0.0
    for index, size in enumerate(sizes):
        if index > start and total + size > target + 1e-9:
            groups.append((start, index - 1))
            start = index
            total = 0.0
        total += size
    if sizes:
        groups.append((start, len(sizes) - 1))
    return groups


def _make_group(
    *,
    task_id: int,
    partition: str,
    shards: list[dict[str, Any]],
    target_mb: float,
    batch: int,
) -> dict[str, Any]:
    """构造一个分组的固定边界与收益口径；空组显式给出原因。"""
    shard_count = len(shards)
    input_mb = round(sum(float(shard.get("大小MB") or 0) for shard in shards), 1)
    reclaimable_mb = round(sum(float(shard.get("可回收容量MB") or 0) for shard in shards), 1)
    status = GROUP_PENDING
    reason = ""
    output_files = 1
    if shard_count == 0:
        status = GROUP_EMPTY
        reason = f"分区 {partition} 当前没有分片，空组不参与收益累计"
        output_files = 0
    elif input_mb <= 0:
        status = GROUP_EMPTY
        reason = "分片容量均为 0，空组不参与收益累计"
        output_files = 0
    elif shard_count == 1 and input_mb <= target_mb:
        status = GROUP_SKIPPED
        reason = "仅 1 个分片且未超过目标大小，无需合并"
        output_files = 1
    accounted = False
    # 空组与无需合并的组不产生收益；其余组先按分片边界算出理论收益，
    # 真正执行成功并记账后才会被汇总统计计入。
    if status in (GROUP_SKIPPED, GROUP_EMPTY):
        file_reduction = 0
        capacity_reduction = 0.0
    else:
        file_reduction = max(shard_count - output_files, 0)
        capacity_reduction = reclaimable_mb
    first_no = min((int(shard.get("序号") or 0) for shard in shards), default=-1)
    last_no = max((int(shard.get("序号") or 0) for shard in shards), default=-1)
    return {
        "group_id": "",
        "task_id": int(task_id),
        "分区": partition,
        "起始分片": shards[0]["shard_id"] if shards else "",
        "结束分片": shards[-1]["shard_id"] if shards else "",
        "起始序号": first_no,
        "结束序号": last_no,
        "shard_ids": [shard["shard_id"] for shard in shards],
        "shard_count": shard_count,
        "input_mb": input_mb,
        "output_files": output_files,
        "file_reduction": file_reduction,
        "capacity_reduction": capacity_reduction,
        "status": status,
        "attempts": 0,
        "reason": reason,
        "fail_first": False,
        "max_attempts": 1,
        "last_error": "",
        "accounted": accounted,
        "batch": batch,
    }


def plan_groups(
    shards: list[dict[str, Any]],
    partitions: list[str],
    *,
    target_mb: float,
    task_id: int = 0,
    batch: int = 1,
    include_missing_partitions: bool = True,
) -> list[dict[str, Any]]:
    """按分区顺序、分片序号顺序生成分组；边界一经生成就固定下来。

    ``include_missing_partitions`` 为真时，文件范围里没有分片的分区也会生成
    一个空组，明确写出原因，方便页面解释“为什么没有收益”。
    """
    by_partition: dict[str, list[dict[str, Any]]] = {}
    for shard in shards:
        by_partition.setdefault(str(shard.get("分区") or ""), []).append(shard)
    ordered_partitions = sorted(set(partitions) | set(by_partition))

    groups: list[dict[str, Any]] = []
    for partition in ordered_partitions:
        items = sorted(by_partition.get(partition, []), key=lambda row: int(row.get("序号") or 0))
        if not items:
            if include_missing_partitions:
                groups.append(
                    _make_group(
                        task_id=task_id,
                        partition=partition,
                        shards=[],
                        target_mb=target_mb,
                        batch=batch,
                    )
                )
            continue
        sizes = [float(item.get("大小MB") or 0) for item in items]
        for start, end in next_fit(sizes, target_mb):
            groups.append(
                _make_group(
                    task_id=task_id,
                    partition=partition,
                    shards=items[start:end + 1],
                    target_mb=target_mb,
                    batch=batch,
                )
            )
    for index, group in enumerate(groups, start=1):
        group["group_id"] = f"g{index:03d}"
    return groups


def shard_stats(shards: list[dict[str, Any]]) -> dict[str, Any]:
    """分片统计：总数、空分片、容量与 P50/P95，统计报告与页面共用这一口径。"""
    sizes = sorted(float(shard.get("大小MB") or 0) for shard in shards)
    total = len(sizes)
    if not sizes:
        return {
            "分片总数": 0,
            "空分片数": 0,
            "总容量MB": 0.0,
            "P50MB": 0.0,
            "P95MB": 0.0,
            "最小MB": 0.0,
            "最大MB": 0.0,
        }

    def percentile(ratio: float) -> float:
        index = min(max(int(round(ratio * (total - 1))), 0), total - 1)
        return round(sizes[index], 1)

    return {
        "分片总数": total,
        "空分片数": sum(1 for size in sizes if size <= 0),
        "总容量MB": round(sum(sizes), 1),
        "P50MB": percentile(0.50),
        "P95MB": percentile(0.95),
        "最小MB": round(sizes[0], 1),
        "最大MB": round(sizes[-1], 1),
    }


def group_key(group: dict[str, Any]) -> str:
    """分组身份：分区 + 原分片边界。重排、重试后只要边界不变，收益就不重复记。"""
    return "|".join(
        [
            str(group.get("分区") or ""),
            str(group.get("起始分片") or ""),
            str(group.get("结束分片") or ""),
            str(group.get("shard_count") or 0),
        ]
    )


def aggregate_groups(groups: list[dict[str, Any]]) -> dict[str, Any]:
    """按成功分组汇总收益；每个分组只贡献一次，空组与未完成组不计收益。

    ``成功分组`` 只统计真正执行成功的组；``无需合并``（单分片未超目标大小）
    与 ``空组`` 单独计数，它们对文件数量/容量下降没有贡献。
    """
    success = [group for group in groups if group.get("status") == GROUP_SUCCESS]
    skipped = [group for group in groups if group.get("status") == GROUP_SKIPPED]
    failed = [group for group in groups if group.get("status") == GROUP_FAILED]
    pending = [
        group for group in groups
        if group.get("status") in (GROUP_PENDING, GROUP_RUNNING)
    ]
    empty = [group for group in groups if group.get("status") == GROUP_EMPTY]
    # accounted 表示这一组的收益已经记过账；同一组即使重试也只会出现一次。
    accounted = sorted(
        [group for group in success if group.get("accounted")],
        key=lambda group: str(group.get("group_id")),
    )
    settled = [group for group in groups if group.get("status") in TERMINAL_GROUP_STATUSES]
    return {
        "总分组": len(groups),
        "成功分组": len(success),
        "无需合并分组": len(skipped),
        "失败分组": len(failed),
        "未完成分组": len(pending),
        "空分组": len(empty),
        "文件数量下降": sum(int(group.get("file_reduction") or 0) for group in accounted),
        "容量下降MB": round(
            sum(float(group.get("capacity_reduction") or 0) for group in accounted), 1
        ),
        "已处理分片": sum(int(group.get("shard_count") or 0) for group in settled),
        "剩余分片": sum(
            int(group.get("shard_count") or 0)
            for group in groups
            if group.get("status") not in TERMINAL_GROUP_STATUSES
        ),
        "输出文件数": sum(int(group.get("output_files") or 0) for group in success),
        "成功组列表": [group.get("group_id") for group in accounted],
    }
