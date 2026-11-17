"""结果聚合归并接口：维护聚合任务，覆盖启动归并、重试归并、归档任务等动作。

另外提供聚合失败缺口保护的归并侧视图：
* GET  /api/merge/progress              聚合进度（三指标同源于归并清单）
* GET  /api/merge/{任务编号}/checklist   单任务缺口保护核对清单
* POST /api/merge/{任务编号}/gap-rewrite 缺口补写（业务时间窗口 / 分片编号水位）
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.merge import MergeService

router = APIRouter(prefix="/api/merge", tags=["结果聚合归并"])

service = MergeService()

LIST_FIELDS = ["任务编号", "采集计划", "分片总数", "已归并数", "归并策略", "输出路径", "数据总量", "任务状态"]
STATUSES = ["待归并", "归并中", "已完成", "归并失败"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="待归并、归并中、已完成、归并失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤结果聚合归并列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/progress")
def manifest_progress() -> dict[str, Any]:
    """聚合进度：分片总数、已归并数、数据总量从同一条归并清单计算。

    进度分类显式区分空结果、缺失分片、写到一半范围与已完成，字段可直接复核。
    """
    return service.manifest_progress()


@router.get("/manifest/{task_no}/checklist")
def protection_checklist(task_no: str) -> dict[str, Any]:
    """单任务聚合失败缺口保护核对清单：缺口、去重依据、交叉核对与历史补写。"""
    checklist, message = service.get_checklist(task_no)
    if checklist is None:
        raise HTTPException(status_code=404, detail=message)
    return checklist


@router.post("/manifest/{task_no}/gap-rewrite", response_model=ActionResult)
def rewrite_gap(task_no: str, payload: EntryPayload) -> ActionResult:
    """缺口补写：只处理当前缺口且与历史补写去重，不覆盖已确认数据。

    values 必传 boundary_type：
    * shard_watermark：传 缺失分片编号（列表），补写结果只含缺失分片；
    * time_window：传 窗口开始/窗口结束，补写结果附扫描但未覆盖的确认记录。
    可选 模拟失败分片 用于演示中断恢复；失败落账并保留续做范围，不产生第二份完整输出。
    """
    values = payload.values
    boundary_type = str(values.get("boundary_type") or "").strip()
    fail_shards = values.get("模拟失败分片")
    if fail_shards is not None and not isinstance(fail_shards, list):
        return ActionResult(ok=False, message="模拟失败分片必须是分片编号列表")
    result, message = service.rewrite_gap(
        task_no,
        boundary_type,
        values,
        fail_shards=[int(no) for no in fail_shards] if fail_shards else None,
    )
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message or "缺口补写已落账", entry=result)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条聚合任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条聚合任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="聚合任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条聚合任务执行启动归并、重试归并、归档任务；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出结果聚合归并清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "merge", "total": total, "items": items}
