"""表统计信息业务规则：状态流转、合并收益报告与筛选口径都收在这里。

合并收益报告固定接 ``compact_dataset/v1`` 版本的合并结果数据集：
先声明兼容目标（版本号与字段清单），再读取现有数据；读取失败时只重试
未完成的合并任务数据集，其余任务继续使用上一稳定口径。
"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "stats"
REQUIRED_FIELDS = ["报告编号", "表名称", "行总数"]
STATUS_ORDER = ["草稿", "已采集", "已过期", "已清理"]
ACTION_RULES = {"采集统计": "草稿", "标记过期": "已采集", "清理报告": "已过期"}
NEGATIVE_ACTIONS = []

# 统计报告接入的合并结果数据集版本与必需字段：先声明兼容目标，再引用数据。
COMPACT_DATASET_VERSION = "compact_dataset/v1"
COMPACT_DATASET_REQUIRED_FIELDS = [
    "dataset_id",
    "task_id",
    "任务编号",
    "目标表名",
    "范围版本",
    "状态",
    "分片统计",
    "groups",
]
COMPACT_REPORT_FIELDS = [
    "任务编号",
    "目标表名",
    "dataset_id",
    "范围版本",
    "状态",
    "分组总数",
    "成功分组",
    "失败分组",
    "空分组",
    "文件数量下降",
    "容量下降MB",
]


class StatsService:
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
            rows = [row for row in rows if keyword in str(row.get("报告编号", ""))]
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

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"统计报告 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于表统计信息可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"统计报告已{action}"

    # ------------------------------------------------------------ 合并收益报告
    def compact_report(self) -> dict[str, Any]:
        """读合并结果数据集并按成功分组汇总；读取失败时退回上一稳定口径。

        正常路径只读不写：统计报告不会替用户执行合并，避免报告刷新改变任务现场。
        某个任务数据集读不出来（缺字段、结构异常等）时，只重试该任务里
        还没完成的分组，其它任务与历史口径保持不变。
        """
        from app.compact_state import compact_registry
        from app.services.compact_dataset import CompactDatasetError, CompactDatasetService

        service = CompactDatasetService()
        service.ensure_ready()
        # 先声明兼容目标，再引用数据。
        compatibility = self.compat_contract()
        errors: list[str] = []
        retried: list[int] = []
        tasks: list[dict[str, Any]] = []
        totals = {"文件数量下降": 0, "容量下降MB": 0.0, "成功分组": 0, "失败分组": 0, "空分组": 0}

        for task_id in sorted(compact_registry.datasets):
            summary: dict[str, Any] | None = None
            review: dict[str, Any] | None = None
            try:
                summary, review = self._read_task(service, task_id)
            except (CompactDatasetError, ValueError) as error:
                errors.append(str(error))
                # 读取失败时只重试这个数据集里未完成的分组，再读一次。
                try:
                    service.run_groups(task_id, only_unfinished=True)
                    retried.append(task_id)
                    summary, review = self._read_task(service, task_id)
                    errors.pop()
                except (CompactDatasetError, ValueError, KeyError) as retry_error:
                    errors.append(f"任务 {task_id} 重试后仍失败：{retry_error}")
                    continue
            if summary is None:
                continue
            tasks.append(
                {
                    "任务编号": summary["任务编号"],
                    "目标表名": summary["目标表名"],
                    "dataset_id": summary["dataset_id"],
                    "范围版本": summary["范围版本"],
                    "状态": summary["状态"],
                    "分片统计": summary["分片统计"],
                    "分组总数": summary["分组总数"],
                    "成功分组": summary["已完成分组"],
                    "失败分组": summary["失败分组"],
                    "空分组": summary["空分组"],
                    "文件数量下降": summary["文件数量下降"],
                    "容量下降MB": summary["容量下降MB"],
                    # 复核批次取自收益复核清单，与合并列表入口是同一份。
                    "复核批次": int((review or {}).get("batch") or 0),
                    "记账条目": int((review or {}).get("记账条目") or 0),
                }
            )
            totals["文件数量下降"] += int(summary["文件数量下降"])
            totals["容量下降MB"] = round(totals["容量下降MB"] + float(summary["容量下降MB"]), 1)
            totals["成功分组"] += int(summary["已完成分组"])
            totals["失败分组"] += int(summary["失败分组"])
            totals["空分组"] += int(summary["空分组"])

        stable = compact_registry.reports[-1] if compact_registry.reports else None
        if errors:
            # 失败时统计模块继续使用上一稳定口径。
            if stable is not None:
                fallback = {
                    key: value for key, value in stable.items()
                    if key not in ("tasks", "totals")
                }
                return {
                    **fallback,
                    "ok": True,
                    "tasks": [dict(task) for task in stable.get("tasks", [])],
                    "totals": dict(stable.get("totals", {})),
                    "口径": "上一稳定口径",
                    "本次错误": errors,
                    "重试任务": retried,
                    "兼容目标": compatibility,
                }
            return {
                "ok": False,
                "口径": "无可用数据",
                "dataset_version": COMPACT_DATASET_VERSION,
                "task_count": 0,
                "tasks": [],
                "totals": totals,
                "本次错误": errors,
                "重试任务": retried,
                "兼容目标": compatibility,
            }
        report = {
            "ok": True,
            "口径": "当前合并结果数据集",
            "dataset_version": COMPACT_DATASET_VERSION,
            "兼容目标": compatibility,
            "task_count": len(tasks),
            "tasks": tasks,
            "totals": totals,
            "重试任务": retried,
            "本次错误": [],
            "batch": max(
                (int(compact_registry.datasets[t]["batch"]) for t in compact_registry.datasets),
                default=1,
            ),
        }
        compact_registry.reports.append(report)
        return report

    def _read_task(
        self, service: Any, task_id: int
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """读一个任务的结果数据集，并按兼容字段清单校验。"""
        dataset = service.get_dataset(task_id)
        self._check_contract(dataset)
        summary = service.dataset_summary(dataset)
        review = service.get_review(task_id)
        return summary, review

    def compat_contract(self) -> dict[str, Any]:
        """先声明兼容目标，再引用已有数据。"""
        return {
            "dataset_version": COMPACT_DATASET_VERSION,
            "required_fields": list(COMPACT_DATASET_REQUIRED_FIELDS),
            "report_fields": list(COMPACT_REPORT_FIELDS),
            "aggregate_by": "成功分组",
            "收益口径": "每个分组只累计一次文件数量下降与容量下降",
        }

    def _check_contract(self, dataset: dict[str, Any]) -> None:
        missing = [field for field in COMPACT_DATASET_REQUIRED_FIELDS if field not in dataset]
        if missing:
            raise ValueError(
                f"合并结果数据集 {dataset.get('dataset_id', '未知')} 缺少兼容字段：{'、'.join(missing)}"
            )
