"""小文件合并结果数据集与收益记账的核心回归用例。

运行：``cd backend && .venv/bin/python -m unittest discover -s tests -v``
"""
from __future__ import annotations

import unittest

from app.compact_plan import DATASET_DONE, GROUP_SUCCESS
from app.compact_seed import seed_compact_registry
from app.compact_state import compact_registry
from app.services.compact_dataset import CompactDatasetError, CompactDatasetService
from app.services.compact_reconcile import CompactReconcileService
from app.services.stats import StatsService


class CompactDatasetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        compact_registry.reset()
        seed_compact_registry()
        self.service = CompactDatasetService()

    def test_seed_plan_matches_orders_example(self) -> None:
        dataset = self.service.get_dataset(1)
        self.assertEqual(len(dataset["groups"]), 58)
        self.assertEqual(dataset["分片统计"]["分片总数"], 240)
        self.assertEqual(dataset["分片统计"]["P50MB"], 9.0)
        self.assertEqual(dataset["分片统计"]["P95MB"], 21.0)
        # 相邻分组的 shard 集合不重叠，合起来正好覆盖全部分片。
        shard_ids = [shard for group in dataset["groups"] for shard in group["shard_ids"]]
        self.assertEqual(len(shard_ids), len(set(shard_ids)))
        self.assertEqual(len(set(shard_ids)), 240)

    def test_rerun_does_not_double_count(self) -> None:
        first = dict(self.service.get_review(1))
        self.service.run_groups(1)
        self.service.run_groups(1)
        again = self.service.get_review(1)
        self.assertEqual(first["文件数量下降"], again["文件数量下降"])
        self.assertEqual(first["容量下降MB"], again["容量下降MB"])
        self.assertEqual(again["记账条目"], len(self.service.get_dataset(1)["groups"]))

    def test_failed_group_resumes_from_original_boundary(self) -> None:
        dataset = self.service.get_dataset(2)
        failed = [group for group in dataset["groups"] if group["status"] == "失败"]
        self.assertTrue(failed)
        boundaries = {
            group["group_id"]: (group["起始分片"], group["结束分片"]) for group in failed
        }
        self.service.resume(2)
        dataset = self.service.get_dataset(2)
        for group in dataset["groups"]:
            if group["group_id"] in boundaries:
                self.assertEqual(
                    (group["起始分片"], group["结束分片"]), boundaries[group["group_id"]]
                )
                self.assertEqual(group["status"], GROUP_SUCCESS)
                self.assertEqual(group["attempts"], 2)
        self.assertEqual(dataset["状态"], DATASET_DONE)

    def test_interrupt_then_resume_keeps_success_once(self) -> None:
        self.service.run_groups(2, limit=2)
        review = self.service.get_review(2)
        before = review["文件数量下降"]
        self.service.interrupt(2, reason="测试中断")
        self.service.resume(2)
        after = self.service.get_review(2)
        self.assertGreaterEqual(after["文件数量下降"], before)
        self.assertEqual(after["记账条目"], after["成功分组"])

    def test_append_scope_only_schedules_new_files(self) -> None:
        before = self.service.get_review(1)["文件数量下降"]
        result = self.service.expand_scope(
            1, add_partitions=["dt=29"], add_shards=16, mode="append"
        )
        self.assertEqual(result["状态"], "执行中")
        self.assertEqual(result["复核清单"]["文件数量下降"], before)
        self.service.run_groups(1)
        after = self.service.get_review(1)
        self.assertGreater(after["文件数量下降"], before)
        dataset = self.service.get_dataset(1)
        success = [group for group in dataset["groups"] if group["status"] == GROUP_SUCCESS]
        self.assertEqual(after["记账条目"], len(success))
        # 新增分片沿用同一目标大小，分组边界只覆盖新分片。
        appended = [group for group in dataset["groups"] if group["分区"] == "dt=29"]
        self.assertTrue(appended)
        self.assertTrue(any(group["batch"] == 2 for group in appended))

    def test_replan_prunes_expired_groups_and_keeps_single_benefit(self) -> None:
        base = self.service.get_review(1)["文件数量下降"]
        result = self.service.expand_scope(1, add_shards=8, mode="replan")
        review = result["复核清单"]
        # 重排后旧完成项要么被继承，要么作废，复核清单不会把同一批分片算两遍。
        self.assertLessEqual(review["文件数量下降"], base)
        self.assertTrue(result["过期完成项"] or result["继承完成组"])
        self.assertEqual(
            review["文件数量下降"],
            sum(row["file_reduction"] for row in self.service.get_detail(1)["账本"]),
        )
        # 续跑只补未完成组，跑完后重新核对仍然一致。
        self.service.resume(1)
        reconcile = CompactReconcileService()
        self.assertEqual(reconcile.reconcile(1)["核对结果"], "一致")
        review = self.service.get_review(1)
        self.assertEqual(
            review["文件数量下降"],
            sum(row["file_reduction"] for row in compact_registry.ledger[1]),
        )

    def test_late_files_keep_existing_group_boundaries(self) -> None:
        """范围扩展新增分片时，已有分组的边界不能被挪动。"""
        before = {
            group["group_id"]: (group["起始分片"], group["结束分片"])
            for group in self.service.get_dataset(1)["groups"]
        }
        self.service.expand_scope(1, add_shards=8, mode="replan")
        after = self.service.get_dataset(1)["groups"]
        inherited = [
            group for group in after if group.get("继承自") and group["status"] == GROUP_SUCCESS
        ]
        self.assertTrue(inherited)
        for group in inherited:
            old_start, old_end = before[group["继承自"]]
            self.assertEqual((group["起始分片"], group["结束分片"]), (old_start, old_end))
        # 迟到分片接在分区尾部，不与已有分片序号冲突。
        by_partition: dict[str, set[int]] = {}
        for shard in self.service.get_shards(1):
            by_partition.setdefault(shard["分区"], set()).add(shard["序号"])
        for partition, numbers in by_partition.items():
            self.assertEqual(len(numbers), max(numbers) + 1, partition)

    def test_reconcile_detects_duplicate_ledger(self) -> None:
        reconcile = CompactReconcileService()
        self.assertEqual(reconcile.reconcile(1)["核对结果"], "一致")
        compact_registry.ledger[1].append(dict(compact_registry.ledger[1][0]))
        result = reconcile.reconcile(1)
        self.assertEqual(result["核对结果"], "不一致")
        self.assertTrue(any("重复分组键" in item for item in result["问题"]))

    def test_stats_report_uses_same_review_batch(self) -> None:
        report = StatsService().compact_report()
        self.assertEqual(report["口径"], "当前合并结果数据集")
        reviews = {item["任务编号"]: item for item in self.service.list_reviews()}
        for task in report["tasks"]:
            self.assertEqual(task["复核批次"], reviews[task["任务编号"]]["batch"])
            self.assertEqual(
                task["文件数量下降"], reviews[task["任务编号"]]["文件数量下降"]
            )

    def test_stats_report_declares_compat_contract(self) -> None:
        report = StatsService().compact_report()
        self.assertEqual(report["兼容目标"]["dataset_version"], "compact_dataset/v1")
        self.assertIn("groups", report["兼容目标"]["required_fields"])

    def test_stats_report_falls_back_to_last_stable_caliber(self) -> None:
        stats = StatsService()
        stable = stats.compact_report()
        snapshot = dict(stable["totals"])
        broken = compact_registry.datasets[3].pop("groups")
        try:
            fallback = stats.compact_report()
        finally:
            compact_registry.datasets[3]["groups"] = broken
        self.assertEqual(fallback["口径"], "上一稳定口径")
        self.assertEqual(fallback["totals"], snapshot)
        self.assertTrue(fallback["本次错误"])

    def test_generate_missing_is_idempotent(self) -> None:
        first = self.service.generate_missing()
        self.assertEqual(first["created"], [])
        entry = {
            "id": 9,
            "任务编号": "COMP-0009",
            "目标表名": "iceberg.ods.generated",
            "分片数量": 32,
        }
        from app.store import store

        store.rows("compact").append(entry)
        try:
            created = self.service.generate_missing()
            self.assertEqual(created["created"], ["CDS-0009-v1"])
            self.assertEqual(self.service.generate_missing()["created"], [])
        finally:
            store.rows("compact").remove(entry)

    def test_resume_without_unfinished_groups_is_rejected(self) -> None:
        with self.assertRaises(CompactDatasetError):
            self.service.resume(1)


if __name__ == "__main__":
    unittest.main()
