"""收益复核模块：读取合并结果数据集，把核对结果同步到收益复核清单。

它只做两件事：
1. 校验结果数据集自身是否自洽（分组边界不重叠、台账不重复、收益能对上）；
2. 把核对结论写回复核清单，供合并列表与统计报告两个入口读取同一份批次。
"""
from __future__ import annotations

from typing import Any

from app.compact_plan import (
    GROUP_EMPTY,
    GROUP_SKIPPED,
    GROUP_SUCCESS,
    group_key,
)
from app.compact_state import compact_registry
from app.services.compact_dataset import CompactDatasetError, CompactDatasetService


class CompactReconcileService:
    def __init__(self) -> None:
        self.dataset_service = CompactDatasetService()

    def reconcile(self, task_id: int, *, reason: str = "统计模块核对") -> dict[str, Any]:
        """核对单个任务的合并结果数据集，并把结论写入收益复核清单。"""
        try:
            dataset = self.dataset_service.get_dataset(task_id)
        except CompactDatasetError as error:
            return {
                "task_id": int(task_id),
                "核对结果": "失败",
                "问题": [str(error)],
                "复核清单": None,
            }
        problems = self._check_dataset(dataset)
        review = self.dataset_service.sync_review(int(task_id), reason=reason)
        review["核对结果"] = "一致" if not problems else "不一致"
        review["核对问题"] = problems
        review["核对分组数"] = len(dataset["groups"])
        review["核对账本条目"] = len(compact_registry.ledger_rows(int(task_id)))
        review["核对批次"] = dataset.get("batch", 1)
        review["dataset_id"] = dataset["dataset_id"]
        review["结果数据集版本"] = dataset.get("数据集版本", "compact_dataset/v1")
        return {
            "task_id": int(task_id),
            "dataset_id": dataset["dataset_id"],
            "核对结果": review["核对结果"],
            "问题": problems,
            "复核清单": review,
        }

    def reconcile_all(self, *, reason: str = "统计模块核对") -> list[dict[str, Any]]:
        self.dataset_service.ensure_ready()
        return [
            self.reconcile(task_id, reason=reason)
            for task_id in sorted(compact_registry.datasets)
        ]

    def _check_dataset(self, dataset: dict[str, Any]) -> list[str]:
        problems: list[str] = []
        groups = dataset["groups"]

        # 1. 分组边界必须完整落在分片清单上，且同一分片不能出现在两个组里。
        shard_map = {
            shard["shard_id"]: shard
            for shard in self.dataset_service.get_shards(int(dataset["task_id"]))
        }
        seen: dict[str, str] = {}
        for group in groups:
            for shard_id in group.get("shard_ids") or []:
                if shard_id not in shard_map:
                    problems.append(
                        f"分组 {group['group_id']} 引用了不存在的分片 {shard_id}"
                    )
                if shard_id in seen:
                    problems.append(
                        f"分片 {shard_id} 同时出现在分组 {seen[shard_id]} 与 {group['group_id']}"
                    )
                seen[shard_id] = group["group_id"]

        # 2. 台账里同一个分组键只能出现一次。
        ledger = compact_registry.ledger_rows(int(dataset["task_id"]))
        keys = [row["分组键"] for row in ledger]
        duplicates = {key for key in keys if keys.count(key) > 1}
        for key in sorted(duplicates):
            problems.append(f"收益台账出现重复分组键：{key}")

        # 3. 台账收益必须与复核清单汇总一致。
        review = compact_registry.review(int(dataset["task_id"]))
        if review is not None:
            ledger_files = sum(int(row["file_reduction"]) for row in ledger)
            ledger_capacity = round(
                sum(float(row["capacity_reduction"]) for row in ledger), 1
            )
            if ledger_files != int(review.get("文件数量下降") or 0):
                problems.append(
                    f"复核清单文件数量下降 {review.get('文件数量下降')} 与台账 {ledger_files} 不一致"
                )
            if ledger_capacity != round(float(review.get("容量下降MB") or 0), 1):
                problems.append(
                    f"复核清单容量下降 {review.get('容量下降MB')} 与台账 {ledger_capacity} 不一致"
                )

        # 4. 成功分组必须都有记账标记，空组/无需合并不能有收益。
        accounted_keys = {row["分组键"] for row in ledger}
        for group in groups:
            if group["status"] == GROUP_SUCCESS and group_key(group) not in accounted_keys:
                problems.append(f"分组 {group['group_id']} 成功但未写入收益台账")
            if group["status"] in (GROUP_EMPTY, GROUP_SKIPPED) and (
                int(group.get("file_reduction") or 0)
                or float(group.get("capacity_reduction") or 0)
            ):
                problems.append(f"分组 {group['group_id']} 为 {group['status']} 却带着收益")

        # 5. 成功分组数必须与台账条目数一致（无需合并的组收益为 0，不记账）。
        success_count = sum(1 for group in groups if group["status"] == GROUP_SUCCESS)
        if success_count != len(ledger):
            problems.append(f"成功分组 {success_count} 与台账条目 {len(ledger)} 不一致")
        return problems
