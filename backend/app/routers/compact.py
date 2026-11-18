"""小文件合并接口：合并任务 CRUD，以及合并结果数据集、续跑、范围扩展与收益复核。

路由顺序很重要：``/export``、``/datasets``、``/reviews`` 这些静态路径必须
排在 ``/{entry_id}`` 之前，否则会被当成 id 解析。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.compact import CompactService
from app.services.compact_dataset import CompactDatasetError, CompactDatasetService
from app.services.compact_reconcile import CompactReconcileService

router = APIRouter(prefix="/api/compact", tags=["小文件合并"])

service = CompactService()
dataset_service = CompactDatasetService()
reconcile_service = CompactReconcileService()

LIST_FIELDS = ["任务编号", "目标表名", "文件范围", "合并策略", "目标大小", "并行度", "执行耗时", "任务状态"]
STATUSES = ["待执行", "执行中", "已完成", "已回滚"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="待执行、执行中、已完成、已回滚"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤小文件合并列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出小文件合并清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "compact", "total": total, "items": items}


# ---------------------------------------------------------------- 结果数据集
@router.get("/datasets")
def list_datasets(keyword: str | None = Query(default=None)) -> dict[str, Any]:
    """合并任务结果数据集列表：一任务一版本，收益按成功分组汇总。"""
    items = dataset_service.list_datasets(keyword=keyword)
    return {"items": items, "total": len(items)}


@router.post("/datasets/generate")
def generate_datasets() -> ActionResult:
    """读取现有合并任务与分片统计后生成结果数据集；已生成的任务不重复生成。"""
    result = dataset_service.generate_missing()
    if not result["created"]:
        return ActionResult(ok=True, message="所有合并任务都已有结果数据集，无需重复生成")
    return ActionResult(
        ok=True,
        message=f"已生成 {result['total']} 份合并任务结果数据集",
        entry=result,
    )


@router.get("/reviews")
def list_reviews() -> dict[str, Any]:
    """收益复核清单列表：一个任务一条记录，收益只累计一次。"""
    items = dataset_service.list_reviews()
    return {"items": items, "total": len(items)}


@router.get("/reviews/{task_id}")
def get_review(task_id: int) -> dict[str, Any]:
    review = dataset_service.get_review(task_id)
    if review is None:
        raise HTTPException(status_code=404, detail=f"合并任务 {task_id} 还没有收益复核记录")
    return review


@router.post("/reviews/{task_id}/reconcile")
def reconcile_review(task_id: int, payload: EntryPayload | None = None) -> dict[str, Any]:
    """收益复核模块读取合并结果数据集，把核对结果同步到收益复核清单。"""
    remark = (payload.remark if payload else None) or "复核模块核对"
    result = reconcile_service.reconcile(task_id, reason=remark)
    return result


@router.get("/datasets/{task_id}")
def get_dataset(task_id: int) -> dict[str, Any]:
    try:
        return dataset_service.get_detail(task_id)
    except CompactDatasetError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@router.post("/datasets/{task_id}/run")
def run_dataset(task_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """执行合并：只跑未完成/失败的分组，失败组复用原分片边界。"""
    values = payload.values if payload else {}
    limit = values.get("limit")
    group_ids = values.get("group_ids")
    try:
        result = dataset_service.run_groups(
            task_id,
            limit=int(limit) if limit not in (None, "") else None,
            group_ids=list(group_ids) if group_ids else None,
        )
    except CompactDatasetError as error:
        return ActionResult(ok=False, message=str(error))
    return ActionResult(
        ok=True,
        message=(
            f"本次执行 {len(result['执行组'])} 组，失败 {len(result['失败组'])} 组；"
            f"累计文件数量下降 {result['收益']['文件数量下降']}，"
            f"容量下降 {result['收益']['容量下降MB']}MB"
        ),
        entry=result,
    )


@router.post("/datasets/{task_id}/interrupt")
def interrupt_dataset(task_id: int, payload: EntryPayload | None = None) -> ActionResult:
    try:
        result = dataset_service.interrupt(task_id, reason=(payload.remark if payload else "") or "")
    except CompactDatasetError as error:
        return ActionResult(ok=False, message=str(error))
    return ActionResult(ok=True, message="合并任务已中断，可从剩余分片继续", entry=result)


@router.post("/datasets/{task_id}/resume")
def resume_dataset(task_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """续跑：已完成组跳过，失败组从原分片边界重试，收益不会重复累计。"""
    values = payload.values if payload else {}
    limit = values.get("limit")
    try:
        result = dataset_service.resume(
            task_id, limit=int(limit) if limit not in (None, "") else None
        )
    except CompactDatasetError as error:
        return ActionResult(ok=False, message=str(error))
    return ActionResult(
        ok=True,
        message=(
            f"续跑执行 {len(result['执行组'])} 组，剩余未完成 "
            f"{result['收益']['未完成分组']} 组；收益累计一次"
        ),
        entry=result,
    )


@router.post("/datasets/{task_id}/scope")
def expand_scope(task_id: int, payload: EntryPayload) -> ActionResult:
    """文件范围扩展：append 只排新增文件；replan 按当前范围重排。"""
    values = payload.values
    mode = str(values.get("mode") or "append").strip()
    partitions = values.get("add_partitions") or []
    if isinstance(partitions, str):
        partitions = [partitions]
    try:
        result = dataset_service.expand_scope(
            task_id,
            add_partitions=list(partitions),
            add_shards=int(values.get("add_shards") or 0),
            mode=mode,
        )
    except (CompactDatasetError, ValueError) as error:
        return ActionResult(ok=False, message=str(error))
    return ActionResult(
        ok=True,
        message=(
            f"文件范围已按 {mode} 扩展，新增 {result['新增分片']} 个分片；"
            f"当前收益只统计一次"
        ),
        entry=result,
    )


# ---------------------------------------------------------------- 任务明细
@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条合并任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"合并任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条合并任务，并立即生成结果数据集；缺字段时说明原因而不是静默丢弃。"""
    entry, missing, dataset = service.create_entry_with_dataset(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    message = "合并任务已登记"
    if dataset is not None:
        message += f"，结果数据集 {dataset['dataset_id']} 已生成"
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条合并任务执行执行合并、回滚合并、归档任务；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
