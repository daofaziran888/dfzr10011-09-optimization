"""路由层联调（stdlib only）：用最小 shim 顶替 fastapi/pydantic，
直接调用真实路由函数，验证入参解析、404/400 与返回结构。

运行：python3 -I /workspace/scripts/verify_router_layer.py
"""
from __future__ import annotations

import sys
import types

# ---- 最小 pydantic shim ---------------------------------------------------
pydantic = types.ModuleType("pydantic")


class BaseModel:
    def __init__(self, **kwargs: object) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def Field(default=None, default_factory=None):  # noqa: N802
    return default_factory() if default_factory else default


pydantic.BaseModel = BaseModel
pydantic.Field = Field
sys.modules["pydantic"] = pydantic

# ---- 最小 fastapi shim ----------------------------------------------------
fastapi = types.ModuleType("fastapi")


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def Query(default=None, description: str | None = None):  # noqa: N802
    return default


class APIRouter:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.routes: list[tuple[str, str, object]] = []

    def _register(self, method: str):
        def decorator(func):
            self.routes.append((method, func.__name__, func))
            return func
        return decorator

    def get(self, *args: object, **kwargs: object):
        return self._register("GET")

    def post(self, *args: object, **kwargs: object):
        return self._register("POST")


fastapi.APIRouter = APIRouter
fastapi.HTTPException = HTTPException
fastapi.Query = Query
sys.modules["fastapi"] = fastapi

sys.path.insert(0, "/workspace/backend")

from app.routers.merge import router as merge_router  # noqa: E402
from app.routers.nodecheck import router as check_router  # noqa: E402
from app.schemas import EntryPayload, PageResult  # noqa: E402

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" -> {detail}" if detail else ""))
    if not condition:
        failures.append(name)


def route(router: APIRouter, name: str):
    for method, fname, func in router.routes:
        if fname == name:
            return func
    raise KeyError(name)


# ---- merge 路由 ------------------------------------------------------------
progress = route(merge_router, "manifest_progress")()
check("GET /merge/progress 返回同源进度", progress["total"] == 5
      and all(set(("分片总数", "已归并数", "数据总量", "进度分类", "计算来源")) <= set(row) for row in progress["items"]))

checklist = route(merge_router, "protection_checklist")("MERG-1002")
check("GET checklist 含缺口/交叉核对/历史补写",
      checklist["缺口"]["待补缺失分片"] == [3] and checklist["历史补写"][0]["补写编号"] == "GAPR-0001")

try:
    route(merge_router, "protection_checklist")("MERG-NOPE")
    check("未知任务 checklist 抛 404", False)
except HTTPException as exc:
    check("未知任务 checklist 抛 404", exc.status_code == 404 and "不存在" in exc.detail)

# 水位补写：续做 MERG-1002 分片 3
payload = EntryPayload(values={"boundary_type": "shard_watermark", "缺失分片编号": [3]})
result = route(merge_router, "rewrite_gap")("MERG-1002", payload)
check("POST gap-rewrite 水位补写返回 ActionResult", result.ok
      and result.entry["补写记录"]["处理分片"] == [3]
      and result.entry["巡检异常"]["汇总维度"] == "shard_no", result.message)

# 重复补写被路由层拦截
dup = route(merge_router, "rewrite_gap")("MERG-1002", EntryPayload(
    values={"boundary_type": "shard_watermark", "缺失分片编号": [3]}))
check("重复补写 ok=False 且说明不覆盖", not dup.ok and "不能被重试覆盖" in dup.message, dup.message)

# 窗口补写 MERG-1005 剩余缺口 6
window_payload = EntryPayload(values={
    "boundary_type": "time_window",
    "窗口开始": "2026-09-13T20:00",
    "窗口结束": "2026-09-14T00:00",
})
wres = route(merge_router, "rewrite_gap")("MERG-1005", window_payload)
check("窗口补写路由落账", wres.ok and wres.entry["补写记录"]["处理分片"] == [6], wres.message)

# 模拟失败分片类型错误
bad_type = route(merge_router, "rewrite_gap")("MERG-1001", EntryPayload(
    values={"boundary_type": "shard_watermark", "缺失分片编号": [1], "模拟失败分片": "x"}))
check("模拟失败分片非列表被拒", not bad_type.ok and "列表" in bad_type.message, bad_type.message)

# 分页与 size 上限
page = route(merge_router, "list_entries")(None, None, 1, 20)
check("merge 列表分页结构", isinstance(page, PageResult) and page.total >= 3)
try:
    route(merge_router, "list_entries")(None, None, 1, 500)
    check("size 超上限抛 400", False)
except HTTPException as exc:
    check("size 超上限抛 400", exc.status_code == 400)

# ---- nodecheck 巡检模块路由 ------------------------------------------------
queue = route(check_router, "protection_queue")()
check("GET gap-protection/queue 重算队列", queue["汇总"]["聚合任务数"] == 5
      and "中断恢复说明" in queue and len(queue["空态任务"]) == 1)

rewrites = route(check_router, "gap_rewrites")("MERG-1002")
check("GET rewrites 按任务过滤且可复核", rewrites["total"] == 2
      and all(r["任务编号"] == "MERG-1002" for r in rewrites["items"]))

empty_rw = route(check_router, "gap_rewrites")("MERG-1004")
check("补写台账空态说明", empty_rw["total"] == 0 and "暂无缺口补写记录" in empty_rw["空态说明"])

exceptions = route(check_router, "gap_exceptions")(None)
check("GET exceptions 每条指向补写边界", exceptions["total"] >= 1
      and all(e["采用补写边界"]["补写编号"] and e["补写依据回指"] for e in exceptions["items"]))

closed = route(check_router, "gap_exceptions")("已关闭")
check("异常按状态过滤", all(e["异常状态"] == "已关闭" for e in closed["items"]))

diff = route(check_router, "module_difference")()
check("模块差异与两种边界对比", diff["节点状态巡检"]["巡检对象"] == "采集节点"
      and len(diff["边界模式对比"]) == 2)

# 节点资源巡检原有接口仍可用
node_page = route(check_router, "list_entries")(None, None, 1, 20)
check("节点状态巡检原口径不受影响", node_page.total == 3
      and "CPU使用率" in node_page.items[0])

print()
if failures:
    print(f"共 {len(failures)} 项失败：{failures}")
    sys.exit(1)
print("路由层自检全部通过")
