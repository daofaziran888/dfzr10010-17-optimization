"""小文件合并业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "compact"
REQUIRED_FIELDS = ["任务编号", "目标表名", "文件范围"]
STATUS_ORDER = ["待执行", "执行中", "已完成", "已回滚"]
ACTION_RULES = {"执行合并": "待执行", "回滚合并": "执行中", "归档任务": "已完成"}
NEGATIVE_ACTIONS = ["回滚合并"]


class CompactService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def create_entry_with_dataset(
        self, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, list[str], dict[str, Any] | None]:
        """登记任务后立刻生成结果数据集；生成失败只回传说明，不影响任务登记。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            return None, missing, None
        from app.services.compact_dataset import CompactDatasetError, CompactDatasetService

        try:
            dataset = CompactDatasetService().create_plan(
                int(entry["id"]),
                task_no=str(entry["任务编号"]),
                table_name=str(entry["目标表名"]),
                shard_count=max(8, int(values.get("分片数量") or 64)),
                target_mb=float(str(values.get("目标大小") or "64").rstrip("MBmb") or 64),
            )
        except (CompactDatasetError, ValueError):
            dataset = None
        return entry, [], dataset

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"合并任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于小文件合并可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if action == "执行合并":
            message = self._run_dataset(entry)
            if message:
                return None, message
        return entry, f"合并任务已{action}"

    def _run_dataset(self, entry: dict[str, Any]) -> str | None:
        """执行合并时顺带跑结果数据集里的未完成分组，并把任务状态同步回来。"""
        from app.services.compact_dataset import CompactDatasetError, CompactDatasetService

        task_id = int(entry.get("id") or 0)
        try:
            service = CompactDatasetService()
            dataset = service.get_dataset(task_id)
            service.run_groups(task_id, only_unfinished=True)
            dataset = service.get_dataset(task_id)
        except CompactDatasetError:
            # 没有结果数据集的任务照样可以走原来的状态流转。
            return None
        from app.compact_plan import DATASET_DONE

        entry["status"] = "已完成" if dataset["状态"] == DATASET_DONE else "执行中"
        entry["pending"] = entry["status"] != "已完成"
        return None
