"""小文件合并结果数据集服务。

核心不变量：
1. 分组边界（分区 + 起始分片 + 结束分片）在生成计划时就固定，失败重试与
   中断续跑都从原边界开始，不重新切分、不重复处理分片。
2. 收益按分组记账：每成功一组往台账写一条，同一个分组身份只记一次；
   复核清单与统计报告都只从台账汇总，重跑多少次结果都不变。
3. 文件范围扩展时只处理新增分片（append）或按当前范围重排（replan）：
   旧计划里已完成的组如果被新计划完整覆盖就继承收益，覆盖不到的记为
   “过期完成项”并从台账剔除，复核清单最终只保留一次有效收益。
"""
from __future__ import annotations

from typing import Any

from app.compact_plan import (
    ACCOUNTED_GROUP_STATUSES,
    DATASET_CONFLICT,
    DATASET_DONE,
    DATASET_INTERRUPTED,
    DATASET_PARTIAL,
    DATASET_PENDING,
    DATASET_RUNNING,
    DATASET_STALE,
    GROUP_EMPTY,
    GROUP_FAILED,
    GROUP_PENDING,
    GROUP_RUNNING,
    GROUP_SKIPPED,
    GROUP_SUCCESS,
    aggregate_groups,
    build_shards,
    group_key,
    plan_groups,
    shard_stats,
)
from app.compact_state import compact_registry


class CompactDatasetError(Exception):
    """带可读说明的业务错误，由路由转成 {ok:false, message}。"""


class CompactDatasetService:
    # ------------------------------------------------------------------ 生成
    def create_plan(
        self,
        task_id: int,
        *,
        task_no: str,
        table_name: str,
        partitions: list[str] | None = None,
        shard_count: int = 64,
        target_mb: float = 64.0,
        scope_version: str = "v1",
    ) -> dict[str, Any]:
        """读取合并任务与分片统计后生成结果数据集；同一任务重复调用只保留一份。

        分片与分组边界一次性生成，之后执行/重试/续跑都读这份数据集，
        不会因为重复生成而让同一批分片被算两次。
        """
        self.ensure_ready()
        task_id = int(task_id)
        existing = compact_registry.datasets.get(task_id)
        if existing is not None:
            return existing
        if shard_count <= 0:
            raise CompactDatasetError("分片数量必须大于 0，空任务不会有合并结果数据集")
        partition_list = partitions or [f"dt={index:02d}" for index in range(max(1, shard_count // 8))]
        shards = build_shards(task_id, count=int(shard_count), partitions=partition_list, batch=1)
        compact_registry.specs[task_id] = {
            "task_id": task_id,
            "partitions": list(partition_list),
            "target_mb": float(target_mb),
            "分片数量": int(shard_count),
        }
        compact_registry.shards[task_id] = shards
        dataset = {
            "id": compact_registry.next_no("dataset"),
            "dataset_id": f"CDS-{task_id:04d}-{scope_version}",
            "task_id": task_id,
            "任务编号": task_no,
            "目标表名": table_name,
            "范围版本": scope_version,
            "分区字段": "dt",
            "partitions": list(partition_list),
            "target_mb": float(target_mb),
            "目标大小": f"{int(target_mb)}MB",
            "并行度": 4,
            "状态": DATASET_PENDING,
            "分片统计": shard_stats(shards),
            "groups": plan_groups(
                shards,
                list(partition_list),
                target_mb=float(target_mb),
                task_id=task_id,
                batch=1,
            ),
            "batch": 1,
            "范围扩展": [],
            "过期完成项": [],
            "数据集版本": "compact_dataset/v1",
        }
        compact_registry.datasets[task_id] = dataset
        self.sync_review(task_id, reason="生成合并结果数据集")
        return dataset

    def generate_missing(self) -> dict[str, Any]:
        """读取现有合并任务，为还没有结果数据集的任务补生成计划。"""
        self.ensure_ready()
        from app.store import store

        created: list[str] = []
        for entry in store.rows("compact"):
            task_id = int(entry.get("id") or 0)
            if task_id <= 0 or task_id in compact_registry.datasets:
                continue
            # 任务里登记过分片数量就按它来；没有登记按默认 64 个分片估算。
            shard_count = int(entry.get("分片数量") or 64)
            try:
                target_mb = float(str(entry.get("目标大小") or "64").rstrip("MBmb"))
            except ValueError:
                target_mb = 64.0
            self.create_plan(
                task_id,
                task_no=str(entry.get("任务编号") or f"TASK-{task_id:04d}"),
                table_name=str(entry.get("目标表名") or ""),
                shard_count=max(shard_count, 8),
                target_mb=target_mb if target_mb > 0 else 64.0,
            )
            created.append(f"CDS-{task_id:04d}-v1")
        return {"created": created, "total": len(created)}

    # ------------------------------------------------------------------ 读取
    def ensure_ready(self) -> None:
        if not compact_registry.seeded:
            from app.compact_seed import seed_compact_registry

            seed_compact_registry()

    def get_dataset(self, task_id: int) -> dict[str, Any]:
        self.ensure_ready()
        dataset = compact_registry.datasets.get(int(task_id))
        if dataset is None:
            raise CompactDatasetError(
                f"合并任务 {task_id} 还没有结果数据集，请先执行合并生成"
            )
        return dataset

    def get_shards(self, task_id: int) -> list[dict[str, Any]]:
        self.ensure_ready()
        return list(compact_registry.shards.get(int(task_id), []))

    def dataset_summary(self, dataset: dict[str, Any]) -> dict[str, Any]:
        """列表与详情共用的一份轻量视图，收益口径与统计报告完全一致。"""
        aggregates = aggregate_groups(dataset["groups"])
        return {
            "dataset_id": dataset["dataset_id"],
            "task_id": dataset["task_id"],
            "任务编号": dataset["任务编号"],
            "目标表名": dataset["目标表名"],
            "范围版本": dataset["范围版本"],
            "分区字段": dataset["分区字段"],
            "目标大小": dataset["目标大小"],
            "并行度": dataset.get("并行度"),
            "状态": dataset["状态"],
            "分片统计": dataset["分片统计"],
            "分组总数": aggregates["总分组"],
            "已完成分组": aggregates["成功分组"],
            "失败分组": aggregates["失败分组"],
            "未完成分组": aggregates["未完成分组"],
            "空分组": aggregates["空分组"],
            "文件数量下降": aggregates["文件数量下降"],
            "容量下降MB": aggregates["容量下降MB"],
            "剩余分片": aggregates["剩余分片"],
            "已处理分片": aggregates["已处理分片"],
            "账本条目": len(compact_registry.ledger_rows(int(dataset["task_id"]))),
            "范围扩展": dataset.get("范围扩展", []),
        }

    def list_datasets(self, *, keyword: str | None = None) -> list[dict[str, Any]]:
        self.ensure_ready()
        items = [self.dataset_summary(dataset) for dataset in compact_registry.datasets.values()]
        if keyword:
            items = [
                item
                for item in items
                if keyword in str(item.get("任务编号", ""))
                or keyword in str(item.get("目标表名", ""))
            ]
        return sorted(items, key=lambda item: str(item.get("任务编号")))

    def get_detail(self, task_id: int) -> dict[str, Any]:
        dataset = self.get_dataset(task_id)
        detail = self.dataset_summary(dataset)
        groups = dataset["groups"]
        success = [g for g in groups if g.get("status") == GROUP_SUCCESS]
        skipped = [g for g in groups if g.get("status") == GROUP_SKIPPED]
        failed = [g for g in groups if g.get("status") == GROUP_FAILED]
        pending = [g for g in groups if g.get("status") in (GROUP_PENDING, GROUP_RUNNING)]
        empty = [g for g in groups if g.get("status") == GROUP_EMPTY]
        detail.update(
            {
                "groups": [self._group_view(group) for group in groups],
                "已完成组": [self._group_view(group) for group in success],
                "无需合并组": [self._group_view(group) for group in skipped],
                "失败组": [self._group_view(group) for group in failed],
                "剩余组": [self._group_view(group) for group in pending],
                "空组说明": [
                    {
                        "group_id": group["group_id"],
                        "分区": group["分区"],
                        "reason": group["reason"],
                    }
                    for group in empty + skipped
                ],
                "复核清单": compact_registry.review(int(task_id)),
                "账本": compact_registry.ledger_rows(int(task_id)),
                "过期完成项": dataset.get("过期完成项", []),
                "batch": dataset.get("batch", 1),
            }
        )
        return detail

    def _group_view(self, group: dict[str, Any]) -> dict[str, Any]:
        return {
            "group_id": group["group_id"],
            "分区": group["分区"],
            "起始分片": group["起始分片"],
            "结束分片": group["结束分片"],
            "起始序号": group["起始序号"],
            "结束序号": group["结束序号"],
            "shard_count": group["shard_count"],
            "input_mb": group["input_mb"],
            "output_files": group["output_files"],
            "file_reduction": group["file_reduction"],
            "capacity_reduction": group["capacity_reduction"],
            "status": group["status"],
            "attempts": group["attempts"],
            "reason": group["reason"],
            "last_error": group["last_error"],
            "accounted": group["accounted"],
            "batch": group["batch"],
        }

    # ------------------------------------------------------------------ 执行
    def run_groups(
        self,
        task_id: int,
        *,
        limit: int | None = None,
        group_ids: list[str] | None = None,
        only_unfinished: bool = False,
    ) -> dict[str, Any]:
        """执行待执行/失败的分组；失败组重试时沿用原分片边界。

        重复调用是安全的：已成功或空组会被跳过，收益不会再次累计。
        """
        dataset = self.get_dataset(task_id)
        if dataset.get("状态") == DATASET_STALE:
            raise CompactDatasetError("结果数据集已过期，请按当前文件范围重新生成计划后再执行")
        wanted = set(group_ids or [])
        executed: list[str] = []
        failed: list[str] = []
        attempted = 0
        budget = limit if limit is not None else 10_000
        for group in dataset["groups"]:
            if attempted >= budget:
                break
            if wanted and group["group_id"] not in wanted:
                continue
            if group["status"] in (GROUP_SUCCESS, GROUP_SKIPPED, GROUP_EMPTY):
                continue
            if group["status"] not in (GROUP_PENDING, GROUP_RUNNING, GROUP_FAILED):
                continue
            if only_unfinished and group["status"] == GROUP_FAILED and group["attempts"] >= int(
                group.get("max_attempts") or 1
            ):
                # 已经用尽重试次数的失败组不再自动重跑，等人工确认后再执行。
                continue
            self._execute_group(group, dataset)
            attempted += 1
            if group["status"] == GROUP_FAILED:
                failed.append(group["group_id"])
            else:
                executed.append(group["group_id"])
        self._refresh_status(dataset)
        totals = aggregate_groups(dataset["groups"])
        return {
            "dataset_id": dataset["dataset_id"],
            "task_id": int(task_id),
            "执行组": executed,
            "失败组": failed,
            "状态": dataset["状态"],
            "收益": totals,
            "复核清单": self.sync_review(int(task_id), reason="执行合并后同步"),
        }

    def _execute_group(self, group: dict[str, Any], dataset: dict[str, Any]) -> None:
        """模拟一次分组执行；边界只读，绝不在这里重算分片范围。"""
        if group["status"] in ACCOUNTED_GROUP_STATUSES or group["status"] == GROUP_EMPTY:
            return
        group["attempts"] = int(group.get("attempts") or 0) + 1
        group["status"] = GROUP_RUNNING
        max_attempts = int(group.get("max_attempts") or 1)
        if group.get("fail_first") and group["attempts"] < max_attempts:
            group["status"] = GROUP_FAILED
            group["last_error"] = (
                f"第 {group['attempts']} 次执行在分片 {group['起始分片']} 处失败，"
                "重试将从原分片边界继续"
            )
            group["accounted"] = False
            return
        group["status"] = GROUP_SUCCESS
        group["last_error"] = ""
        group["file_reduction"] = max(int(group["shard_count"]) - int(group["output_files"]), 0)
        shard_ids = set(group.get("shard_ids") or [])
        group["capacity_reduction"] = round(
            sum(
                float(shard.get("可回收容量MB") or 0)
                for shard in compact_registry.shards.get(int(dataset["task_id"]), [])
                if shard.get("shard_id") in shard_ids
            ),
            1,
        )
        self._account_group(group, int(dataset["task_id"]))

    def _account_group(self, group: dict[str, Any], task_id: int) -> None:
        """把一组的收益写入台账；同一个分组身份只允许写一次。"""
        if group["status"] not in ACCOUNTED_GROUP_STATUSES:
            return
        ledger = compact_registry.ledger_rows(task_id)
        key = group_key(group)
        existing = next((row for row in ledger if row["分组键"] == key), None)
        if existing is not None:
            group["accounted"] = True
            return
        ledger.append(
            {
                "ledger_id": compact_registry.next_no("ledger"),
                "task_id": task_id,
                "group_id": group["group_id"],
                "分组键": key,
                "分区": group["分区"],
                "起始分片": group["起始分片"],
                "结束分片": group["结束分片"],
                "shard_count": group["shard_count"],
                "file_reduction": int(group.get("file_reduction") or 0),
                "capacity_reduction": round(float(group.get("capacity_reduction") or 0), 1),
                "batch": group.get("batch", 1),
                "attempts": group.get("attempts", 1),
                "状态": group["status"],
            }
        )
        group["accounted"] = True

    def _refresh_status(self, dataset: dict[str, Any]) -> None:
        groups = dataset["groups"]
        if any(group["status"] in (GROUP_PENDING, GROUP_RUNNING) for group in groups):
            dataset["状态"] = DATASET_RUNNING
            if dataset.get("冲突"):
                dataset["状态"] = DATASET_CONFLICT
            return
        # 全部组都落定，冲突现场也随之收敛，不再挂“范围冲突”。
        dataset.pop("冲突", None)
        if any(group["status"] == GROUP_FAILED for group in groups):
            dataset["状态"] = DATASET_PARTIAL
            return
        dataset["状态"] = DATASET_DONE

    # ------------------------------------------------------------------ 中断 / 续跑
    def interrupt(self, task_id: int, *, reason: str = "") -> dict[str, Any]:
        dataset = self.get_dataset(task_id)
        if dataset.get("状态") == DATASET_DONE:
            raise CompactDatasetError("合并任务已完成，没有可中断的分组")
        for group in dataset["groups"]:
            if group["status"] == GROUP_RUNNING:
                group["status"] = GROUP_PENDING
        dataset["状态"] = DATASET_INTERRUPTED
        dataset["中断原因"] = reason or "执行被中断，可从剩余分片继续"
        return {
            "dataset_id": dataset["dataset_id"],
            "状态": dataset["状态"],
            "中断原因": dataset["中断原因"],
            "收益": aggregate_groups(dataset["groups"]),
        }

    def resume(self, task_id: int, *, limit: int | None = None) -> dict[str, Any]:
        """从剩余分片续跑：复用原分组边界，只执行未完成/失败的分组。"""
        dataset = self.get_dataset(task_id)
        if dataset.get("状态") == DATASET_STALE:
            raise CompactDatasetError("结果数据集已过期，请按当前文件范围重新生成计划后再续跑")
        unfinished = [
            group for group in dataset["groups"]
            if group["status"] in (GROUP_PENDING, GROUP_RUNNING, GROUP_FAILED)
        ]
        if not unfinished:
            raise CompactDatasetError("没有未完成的分组，无法续跑")
        result = self.run_groups(task_id, limit=limit, only_unfinished=True)
        result["续跑"] = True
        result["中断原因"] = dataset.get("中断原因", "")
        dataset.pop("中断原因", None)
        return result

    # ------------------------------------------------------------------ 范围扩展
    def expand_scope(
        self,
        task_id: int,
        *,
        add_partitions: list[str] | None = None,
        add_shards: int = 0,
        mode: str = "append",
    ) -> dict[str, Any]:
        """文件范围扩展：append 只排新增分片；replan 按当前范围重排。

        - ``append``：旧分组边界原样保留，只给新增分片排队；
        - ``replan``：按当前范围重新分组，旧完成组被完整覆盖则继承收益，
          覆盖不到的记为“过期完成项”，从台账剔除，避免重复累计。
        """
        if mode not in ("append", "replan"):
            raise CompactDatasetError(f"范围扩展模式「{mode}」不支持，可选 append / replan")
        dataset = self.get_dataset(task_id)
        known = list(dataset["partitions"])
        known_set = set(known)
        appended = [part for part in (add_partitions or []) if part not in known_set]
        if not appended and int(add_shards) <= 0:
            raise CompactDatasetError("没有新增分区或分片，文件范围未发生变化")
        if int(add_shards) < 0:
            raise CompactDatasetError("新增分片数量不能为负数")

        current_shards = self.get_shards(task_id)
        next_global = max((int(shard.get("全局序号") or 0) for shard in current_shards), default=-1) + 1
        next_id = max((int(shard.get("id") or 0) for shard in current_shards), default=0) + 1
        # 迟到文件接着该分区已有分片序号往后排，不打乱已有分组的边界。
        offsets: dict[str, int] = {}
        for shard in current_shards:
            partition = str(shard.get("分区") or "")
            offsets[partition] = max(
                offsets.get(partition, 0), int(shard.get("序号") or 0) + 1
            )
        # 新增分片优先落到新增分区；没有新增分区时落到已有分区（迟到文件）。
        target_partitions = appended or known or ["dt=ext"]
        new_shards: list[dict[str, Any]] = []
        if int(add_shards) > 0:
            new_shards = build_shards(
                int(task_id),
                count=int(add_shards),
                partitions=target_partitions,
                shard_no_start=next_global,
                id_start=next_id,
                batch=int(dataset.get("batch") or 1) + 1,
                partition_offsets=offsets,
            )
        combined = current_shards + new_shards
        next_batch = int(dataset.get("batch") or 1) + 1
        old_groups = dataset["groups"]
        old_accounted = {
            group["group_id"]: dict(group)
            for group in old_groups
            if group.get("accounted") and group.get("status") in ACCOUNTED_GROUP_STATUSES
        }

        if mode == "append":
            new_groups = plan_groups(
                new_shards,
                target_partitions,
                target_mb=float(dataset["target_mb"]),
                task_id=int(task_id),
                batch=next_batch,
                include_missing_partitions=False,
            )
            start_no = len(old_groups)
            for offset, group in enumerate(new_groups, start=start_no + 1):
                group["group_id"] = f"g{offset:03d}"
            dataset["groups"] = old_groups + new_groups
            dataset["batch"] = next_batch
            dataset["partitions"] = known + appended
            # 之前重排留下的过期完成项仍然保留，方便追溯；append 不会恢复它们的收益。
            dataset.setdefault("过期完成项", [])
            inherited: list[str] = []
        else:
            rebuilt = plan_groups(
                combined,
                known + appended,
                target_mb=float(dataset["target_mb"]),
                task_id=int(task_id),
                batch=next_batch,
            )
            inherited = []
            expired: list[dict[str, Any]] = []
            for group_id, old in old_accounted.items():
                matched = self._find_covering_group(rebuilt, old)
                if matched is not None:
                    matched["file_reduction"] = int(old.get("file_reduction") or 0)
                    matched["capacity_reduction"] = round(
                        float(old.get("capacity_reduction") or 0), 1
                    )
                    matched["accounted"] = True
                    matched["status"] = GROUP_SUCCESS
                    matched["继承自"] = group_id
                    inherited.append(group_id)
                else:
                    expired.append(
                        {
                            "group_id": group_id,
                            "分区": old.get("分区"),
                            "起始分片": old.get("起始分片"),
                            "结束分片": old.get("结束分片"),
                            "shard_count": old.get("shard_count"),
                            "file_reduction": old.get("file_reduction"),
                            "capacity_reduction": old.get("capacity_reduction"),
                            "原因": "新文件范围已覆盖该分片区间，旧完成项作废",
                        }
                    )
            compact_registry.history.setdefault(int(task_id), []).append(dict(dataset))
            dataset["groups"] = rebuilt
            dataset["batch"] = next_batch
            dataset["partitions"] = known + appended
            dataset["过期完成项"] = expired
            dataset["冲突"] = True
            dataset["状态"] = DATASET_CONFLICT
        compact_registry.shards[int(task_id)] = combined
        dataset["分片统计"] = shard_stats(combined)
        # 新排出来的分组还没跑，状态按实际进度刷新；冲突标记保留，
        # 直到下一批跑完再由 _refresh_status 归一。
        self._refresh_status(dataset)
        dataset["范围扩展"].append(
            {
                "batch": next_batch,
                "mode": mode,
                "新增分区": appended,
                "新增分片": len(new_shards),
                "范围版本": self._next_scope_version(dataset),
                "旧分组数": len(old_groups),
                "新分组数": len(dataset["groups"]),
            }
        )
        self._rebuild_ledger(int(task_id))
        review = self.sync_review(int(task_id), reason=f"文件范围扩展（{mode}）")
        return {
            "dataset_id": dataset["dataset_id"],
            "状态": dataset["状态"],
            "mode": mode,
            "batch": next_batch,
            "新增分片": len(new_shards),
            "新增分区": appended,
            "总分组": len(dataset["groups"]),
            "继承完成组": inherited,
            "过期完成项": dataset.get("过期完成项", []),
            "收益": aggregate_groups(dataset["groups"]),
            "复核清单": review,
        }

    def _next_scope_version(self, dataset: dict[str, Any]) -> str:
        version = str(dataset.get("范围版本") or "v1")
        try:
            number = int(version.lstrip("v"))
        except ValueError:
            number = 1
        return f"v{number + 1}"

    def _find_covering_group(
        self, groups: list[dict[str, Any]], old: dict[str, Any]
    ) -> dict[str, Any] | None:
        """新计划里找与原组**分片集合完全相同**的组；边界一变就不能继承。"""
        old_ids = set(old.get("shard_ids") or [])
        if not old_ids:
            return None
        for group in groups:
            if set(group.get("shard_ids") or []) == old_ids:
                return group
        return None

    def _rebuild_ledger(self, task_id: int) -> None:
        """按当前分组重建台账，同一分组键只保留一条。"""
        dataset = compact_registry.datasets[int(task_id)]
        ledger: list[dict[str, Any]] = []
        seen: set[str] = set()
        for group in dataset["groups"]:
            if not group.get("accounted"):
                continue
            key = group_key(group)
            if key in seen:
                continue
            seen.add(key)
            ledger.append(
                {
                    "ledger_id": compact_registry.next_no("ledger"),
                    "task_id": int(task_id),
                    "group_id": group["group_id"],
                    "分组键": key,
                    "分区": group["分区"],
                    "起始分片": group["起始分片"],
                    "结束分片": group["结束分片"],
                    "shard_count": group["shard_count"],
                    "file_reduction": int(group.get("file_reduction") or 0),
                    "capacity_reduction": round(float(group.get("capacity_reduction") or 0), 1),
                    "batch": group.get("batch", 1),
                    "attempts": group.get("attempts", 1),
                    "状态": group["status"],
                }
            )
        compact_registry.ledger[int(task_id)] = ledger

    # ------------------------------------------------------------------ 复核清单
    def sync_review(self, task_id: int, *, reason: str = "") -> dict[str, Any]:
        """刷新收益复核清单：一个任务只有一条，收益只从台账汇总一次。"""
        dataset = self.get_dataset(task_id)
        ledger = compact_registry.ledger_rows(int(task_id))
        totals = aggregate_groups(dataset["groups"])
        review = compact_registry.review(int(task_id))
        if review is None:
            review = {
                "id": compact_registry.next_no("review"),
                "task_id": int(task_id),
                "任务编号": dataset["任务编号"],
                "目标表名": dataset["目标表名"],
                "dataset_id": dataset["dataset_id"],
                "范围版本": dataset["范围版本"],
                "文件数量下降": 0,
                "容量下降MB": 0.0,
                "成功分组": 0,
                "失败分组": 0,
                "空分组": 0,
                "已处理分片": 0,
                "剩余分片": 0,
                "记账条目": 0,
                "同步次数": 0,
                "batch": dataset.get("batch", 1),
                "状态": dataset["状态"],
                "说明": reason,
            }
            compact_registry.reviews[int(task_id)] = review
        review.update(
            {
                "dataset_id": dataset["dataset_id"],
                "范围版本": dataset["范围版本"],
                "文件数量下降": sum(int(row["file_reduction"]) for row in ledger),
                "容量下降MB": round(
                    sum(float(row["capacity_reduction"]) for row in ledger), 1
                ),
                "成功分组": totals["成功分组"],
                "失败分组": totals["失败分组"],
                "空分组": totals["空分组"],
                "已处理分片": totals["已处理分片"],
                "剩余分片": totals["剩余分片"],
                "记账条目": len(ledger),
                "batch": dataset.get("batch", 1),
                "状态": dataset["状态"],
                "说明": reason or review.get("说明", ""),
            }
        )
        review["同步次数"] = int(review.get("同步次数") or 0) + 1
        return review

    def list_reviews(self) -> list[dict[str, Any]]:
        self.ensure_ready()
        return sorted(
            (dict(row) for row in compact_registry.reviews.values()),
            key=lambda row: str(row.get("任务编号")),
        )

    def get_review(self, task_id: int) -> dict[str, Any] | None:
        self.ensure_ready()
        review = compact_registry.review(int(task_id))
        return dict(review) if review is not None else None
