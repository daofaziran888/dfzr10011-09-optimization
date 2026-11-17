"""节点状态巡检接口。

路由分两组，对应巡检模块里两类不同对象：

* /api/nodecheck/...                节点资源指标巡检（原有口径）
* /api/nodecheck/gap-protection/... 聚合失败缺口保护：保护队列、核对清单、
  缺口补写结果台账、聚合任务异常清单、与节点状态巡检的差异说明
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.nodecheck import NodecheckService

router = APIRouter(prefix="/api/nodecheck", tags=["节点状态巡检"])

service = NodecheckService()

LIST_FIELDS = ["记录编号", "节点编号", "CPU使用率", "内存使用率", "磁盘使用率", "网络延迟", "错误次数", "采样时间"]
STATUSES = ["正常", "亚健康", "异常", "已下线"]


# ---- 聚合失败缺口保护（巡检模块） ----------------------------------------


@router.get("/gap-protection/queue")
def protection_queue() -> dict[str, Any]:
    """聚合失败缺口保护队列：每次调用都按当前台账重算，不返回缓存。

    空结果任务、缺失分片、写到一半范围在队列里分类展示，并附中断恢复说明。
    """
    return service.protection_queue()


@router.get("/gap-protection/checklist/{task_no}")
def protection_checklist(task_no: str) -> dict[str, Any]:
    """单任务保护核对清单：缺口识别、去重依据、分片交叉核对、历史补写与异常。"""
    checklist, message = service.protection_checklist(task_no)
    if checklist is None:
        raise HTTPException(status_code=404, detail=message)
    return checklist


@router.get("/gap-protection/rewrites")
def gap_rewrites(
    task_no: str | None = Query(default=None, description="可选：按聚合任务编号过滤"),
) -> dict[str, Any]:
    """缺口补写结果数据：只追加台账；没有记录时返回空态与原因说明。"""
    return service.list_rewrites(task_no)


@router.get("/gap-protection/exceptions")
def gap_exceptions(
    status: str | None = Query(default=None, description="可选：处理中、已关闭"),
) -> dict[str, Any]:
    """聚合任务异常清单：每条异常都指向采用的补写边界与补写依据回指。"""
    return service.list_exceptions(status)


@router.get("/gap-protection/difference")
def module_difference() -> dict[str, Any]:
    """巡检模块（缺口保护）与节点状态巡检的差异，以及两种补写边界的做法对比。"""
    return service.module_difference()


# ---- 节点状态巡检（节点资源指标，原有口径） --------------------------------


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按记录编号检索"),
    status: str | None = Query(default=None, description="正常、亚健康、异常、已下线"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按记录编号与状态过滤节点状态巡检列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条巡检记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"巡检记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条巡检记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="巡检记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条巡检记录执行标记异常、隔离节点、恢复节点；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出节点状态巡检清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "nodecheck", "total": total, "items": items}
