"""小文件合并结果数据集的运行期仓库。

结果数据集、分片、收益台账与收益复核清单单独存放，不混进业务模块表：
运营概览仍旧只统计 16 个业务模块，不会凭空多出一张“结果数据集”业务表。
"""
from __future__ import annotations

from typing import Any


class CompactRegistry:
    """进程内仓库；重启后由 ``compact_seed`` 重新装配示例数据。"""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        # task_id -> 分片清单
        self.shards: dict[int, list[dict[str, Any]]] = {}
        # task_id -> 任务的分片规格（分区、目标大小、目标分片数）
        self.specs: dict[int, dict[str, Any]] = {}
        # task_id -> 当前版本的结果数据集
        self.datasets: dict[int, dict[str, Any]] = {}
        # task_id -> 历史版本结果数据集（范围重排时归档，便于追溯过期完成项）
        self.history: dict[int, list[dict[str, Any]]] = {}
        # task_id -> 收益复核清单，一个任务只有一条记录
        self.reviews: dict[int, dict[str, Any]] = {}
        # task_id -> 收益台账，一个成功分组一条记录
        self.ledger: dict[int, list[dict[str, Any]]] = {}
        # 统计模块生成的报告批次，最后一条是“上一稳定口径”
        self.reports: list[dict[str, Any]] = []
        self.seq: dict[str, int] = {
            "ledger": 0,
            "review": 0,
            "report": 0,
            "dataset": 0,
        }
        self.seeded = False

    def next_no(self, kind: str) -> int:
        self.seq[kind] = int(self.seq.get(kind, 0)) + 1
        return self.seq[kind]

    def review(self, task_id: int) -> dict[str, Any] | None:
        return self.reviews.get(int(task_id))

    def ledger_rows(self, task_id: int) -> list[dict[str, Any]]:
        return self.ledger.setdefault(int(task_id), [])


compact_registry = CompactRegistry()
