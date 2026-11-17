"""聚合失败缺口保护：端到端逻辑自检（只用标准库，python3 -I 运行）。

覆盖：
1. 三指标同源（分片总数/已归并数/数据总量来自同一条归并清单）；
2. 进度分类区分空结果、缺失分片、写到一半；
3. 缺口识别去重、补写只处理当前缺口、同一分片不重复累计；
4. 失败续做不产生第二份完整输出，续做范围可继续；
5. 时间窗口补写列出扫描未覆盖确认记录，水位补写只含缺失分片；
6. 同一缺口两种边界冲突可同时进行，巡检异常按各自维度汇总并指向边界；
7. 保护队列/异常清单/补写台账随重算刷新，空态有说明。
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/backend")

from app.services import gap_protection as gap  # noqa: E402
from app.services.merge import MergeService  # noqa: E402
from app.services.nodecheck import NodecheckService  # noqa: E402
from app.store import store  # noqa: E402

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name}" + (f" -> {detail}" if detail else ""))
    if not condition:
        failures.append(name)


def snapshot_manifest(task_no: str) -> dict:
    row = next(r for r in store.rows(gap.MANIFEST_MODULE) if r["任务编号"] == task_no)
    return {int(s["分片编号"]): dict(s) for s in row["分片清单"]}


merge_svc = MergeService()
check_svc = NodecheckService()

# ---------- 1. 进度与三指标同源 ------------------------------------------
progress = merge_svc.manifest_progress()
by_task = {item["任务编号"]: item for item in progress["items"]}

p1001 = by_task["MERG-1001"]
check("已完成任务三指标同源", p1001["分片总数"] == 3 and p1001["已归并数"] == 3
      and p1001["数据总量"] == 1200 + 1350 + 980 and p1001["进度分类"] == gap.PROGRESS_COMPLETE,
      str({k: p1001[k] for k in ("分片总数", "已归并数", "数据总量", "进度分类")}))

p1004 = by_task["MERG-1004"]
check("空结果任务展示空态分类", p1004["进度分类"] == gap.PROGRESS_EMPTY and "没有可归并分片" in p1004["进度说明"]
      and p1004["分片总数"] == 0 and p1004["数据总量"] == 0)

p1002 = by_task["MERG-1002"]
check("缺失分片任务分类", p1002["进度分类"] == gap.PROGRESS_MISSING
      and p1002["待补缺失分片"] == [3] and p1002["已补写分片"] == [2],
      str({k: p1002[k] for k in ("进度分类", "待补缺失分片", "已补写分片")}))

p1003 = by_task["MERG-1003"]
# 同时存在缺失与写到一半时分类优先报缺失分片，但两个待补列表都必须可见可复核
check("写到一半范围与缺失分片同时可复核", p1003["待补写到一半分片"] == [2]
      and p1003["待补缺失分片"] == [3] and p1003["进度分类"] in (gap.PROGRESS_MISSING, gap.PROGRESS_PARTIAL))

# 纯写到一半场景：把 MERG-1003 的缺失分片补齐后，分类应落到写到一半
m1003 = next(r for r in store.rows(gap.MANIFEST_MODULE) if r["任务编号"] == "MERG-1003")
for item in m1003["分片清单"]:
    if int(item["分片编号"]) == 3:
        item["状态"] = gap.SHARD_MERGED
        item["数据量"] = 210
p1003b = merge_svc.manifest_progress()["items"]
p1003b = next(i for i in p1003b if i["任务编号"] == "MERG-1003")
check("纯写到一半任务分类与中断说明", p1003b["进度分类"] == gap.PROGRESS_PARTIAL
      and p1003b["待补写到一半分片"] == [2] and "写到一半" in p1003b["进度说明"])
# 还原现场，后续续做测试仍按原缺口执行
for item in m1003["分片清单"]:
    if int(item["分片编号"]) == 3:
        item["状态"] = gap.SHARD_MISSING
        item["数据量"] = 0
        item["校验和"] = ""

# ---------- 2. 缺口识别 + 去重（种子里 GAPR-0001 已补 MERG-1002 分片2） ---
g1002 = gap.identify_gaps(next(r for r in store.rows(gap.MANIFEST_MODULE) if r["任务编号"] == "MERG-1002"))
check("缺口识别剔除已补写分片", g1002["待补缺失分片"] == [3] and g1002["已补写缺失分片"] == [2]
      and g1002["去重依据"] == [2])

# ---------- 3. 重试不得覆盖：对已补写分片再次水位补写被拒绝 ---------------
res, msg = merge_svc.rewrite_gap("MERG-1002", gap.BOUNDARY_WATERMARK, {"缺失分片编号": [2]})
check("已补写分片拒绝重复累计", res is None and "不是当前待补缺口" in msg, msg)

# 对已归并确认分片补写同样拒绝
res, msg = merge_svc.rewrite_gap("MERG-1002", gap.BOUNDARY_WATERMARK, {"缺失分片编号": [1]})
check("已归并确认数据拒绝覆盖", res is None and "不是当前待补缺口" in msg, msg)

# ---------- 4. 失败续做：MERG-1003 先补 2、3，模拟 3 失败 ----------------
before = snapshot_manifest("MERG-1003")
res, msg = merge_svc.rewrite_gap(
    "MERG-1003", gap.BOUNDARY_WATERMARK, {"缺失分片编号": [2, 3]}, fail_shards=[3]
)
check("部分失败补写落账成功", res is not None and msg == "", msg)
if res:
    rec = res["补写记录"]
    check("失败补写只含成功分片结果", rec["处理分片"] == [2] and [r["分片编号"] for r in rec["补写结果"]] == [2]
          and rec["结果状态"] == "部分成功")
    check("失败补写保留续做范围", rec["续做范围"]["缺失分片编号"] == [3])
    check("成功分片回写清单且失败分片维持原态",
          res["续后进度"]["写到一半分片"] == [] and res["续后进度"]["缺失分片"] == [3])
    check("已确认数据未被回滚覆盖", snapshot_manifest("MERG-1003")[1] == before[1])
    ex = res["巡检异常"]
    check("巡检异常按分片编号维度汇总并指向边界",
          ex["汇总维度"] == "shard_no" and ex["采用补写边界"]["补写编号"] == rec["补写编号"]
          and ex["异常状态"] == "处理中" and ex["补写依据回指"] == rec["补写编号"])

# 续做：只补剩余的 3，不生成第二份完整输出
res2, msg2 = merge_svc.rewrite_gap("MERG-1003", gap.BOUNDARY_WATERMARK, {"缺失分片编号": [3]})
check("续做只处理剩余缺口", res2 is not None and res2["补写记录"]["处理分片"] == [3], msg2)
if res2:
    check("续做结果不包含历史分片(无第二份完整输出)",
          [r["分片编号"] for r in res2["补写记录"]["补写结果"]] == [3])
    check("续做后异常关闭且进度完成",
          res2["巡检异常"]["异常状态"] == "已关闭"
          and res2["续后进度"]["进度分类"] == gap.PROGRESS_COMPLETE)
rewrite_count_1003 = len([r for r in gap.list_rewrites("MERG-1003")])
check("两次补写两条独立台账记录", rewrite_count_1003 == 2, f"记录数={rewrite_count_1003}")

# 全部失败场景：不产生覆盖写入
res3, msg3 = merge_svc.rewrite_gap(
    "MERG-1002", gap.BOUNDARY_WATERMARK, {"缺失分片编号": [3]}, fail_shards=[3]
)
check("全失败补写登记失败且无处理分片", res3 is not None
      and res3["补写记录"]["结果状态"] == "补写失败"
      and res3["补写记录"]["处理分片"] == []
      and res3["补写记录"]["续做范围"]["缺失分片编号"] == [3], msg3)

# ---------- 5. 两种边界冲突：MERG-1005 同时支持窗口与水位 -----------------
# 水位补写：只包含缺失分片 2、3
resw, msgw = merge_svc.rewrite_gap(
    "MERG-1005", gap.BOUNDARY_WATERMARK, {"缺失分片编号": [2, 3]}
)
check("水位补写成功且只含缺失分片", resw is not None and resw["补写记录"]["处理分片"] == [2, 3]
      and resw["补写记录"]["扫描未覆盖记录"] == [], msgw)
if resw:
    check("水位异常按分片编号列出",
          resw["巡检异常"]["汇总维度"] == "shard_no"
          and resw["巡检异常"]["缺口明细"] == [2, 3, 6])

# 窗口补写：覆盖剩余缺失 6，窗口同时扫到已归并分片 5
res_t, msg_t = merge_svc.rewrite_gap(
    "MERG-1005", gap.BOUNDARY_WINDOW,
    {"窗口开始": "2026-09-13T16:00", "窗口结束": "2026-09-14T00:00"},
)
check("窗口补写成功处理窗口内缺口", res_t is not None and res_t["补写记录"]["处理分片"] == [6], msg_t)
if res_t:
    uncovered = res_t["补写记录"]["扫描未覆盖记录"]
    check("窗口补写列出扫描但未覆盖的确认记录",
          any(u["分片编号"] == 5 and "不得覆盖" in u["未覆盖原因"] for u in uncovered),
          str(uncovered))
    check("窗口异常按时间窗口汇总并指向窗口边界",
          res_t["巡检异常"]["汇总维度"] == "time_window"
          and res_t["巡检异常"]["采用补写边界"]["边界类型"] == gap.BOUNDARY_WINDOW
          and res_t["巡检异常"]["缺口明细"] == {"开始": "2026-09-13T16:00", "结束": "2026-09-14T00:00"})

# 同一缺口两种边界都留痕：MERG-1005 有两条不同维度的异常
ex_1005 = [e for e in gap.list_exceptions() if e["任务编号"] == "MERG-1005"]
check("冲突缺口两种处理并存且维度不同",
      {e["汇总维度"] for e in ex_1005} == {"shard_no", "time_window"}
      and all(e["采用补写边界"]["补写编号"] for e in ex_1005))

# 空边界/非法边界
res, msg = merge_svc.rewrite_gap("MERG-1005", gap.BOUNDARY_WATERMARK, {"缺失分片编号": []})
check("空水位边界被拒绝", res is None and "非空" in msg, msg)
res, msg = merge_svc.rewrite_gap("MERG-1005", gap.BOUNDARY_WINDOW,
                                 {"窗口开始": "2026-09-14T00:00", "窗口结束": "2026-09-13T00:00"})
check("非法时间窗口被拒绝", res is None and "非法" in msg, msg)
res, msg = merge_svc.rewrite_gap("MERG-1005", "unknown", {})
check("未知边界类型被拒绝", res is None and "未知补写边界类型" in msg, msg)

# 窗口内已无缺口：不产生输出也不登记异常
exc_before = len(gap.list_exceptions())
res, msg = merge_svc.rewrite_gap(
    "MERG-1001", gap.BOUNDARY_WINDOW,
    {"窗口开始": "2026-09-09T00:00", "窗口结束": "2026-09-10T00:00"},
)
check("无缺口窗口补写拒绝且不登记异常", res is None and "没有待补缺口" in msg
      and len(gap.list_exceptions()) == exc_before, msg)

# ---------- 6. 保护队列重算与空态 -----------------------------------------
queue = check_svc.protection_queue()
q1002 = next(e for e in queue["队列"] if e["任务编号"] == "MERG-1002")
check("保护队列区分失败续做范围", q1002["待补缺失分片"] == [3] and q1002["已补写分片"] == [2])
empty_task = next(e for e in queue["空态任务"] if e["任务编号"] == "MERG-1004")
check("保护队列空态说明", "没有可归并分片" in empty_task["空态说明"])
check("中断恢复说明按任务列出",
      any(i["任务编号"] == "MERG-1003" for i in queue["中断恢复说明"]) is False
      or all("续做分片" in i for i in queue["中断恢复说明"]))
check("队列汇总字段齐全", set(queue["汇总"]) == {
    "聚合任务数", "待补任务数", "空结果任务数", "缺失分片总数", "写到一半范围总数", "巡检异常总数"})

# 重算幂等：连续调用结果一致（别的业务入口跟着重算的语义）
queue2 = check_svc.protection_queue()
check("重算结果稳定可复现", queue2["汇总"] == queue["汇总"])

# ---------- 7. 核对清单字段可复核 + 分片交叉核对 ---------------------------
checklist, _ = check_svc.protection_checklist("MERG-1002")
cross = {c["分片编号"]: c for c in checklist["分片交叉核对"]}
check("核对清单交叉核对调度侧缺口",
      cross[2]["核对结论"].startswith("调度侧状态为") and cross[3]["调度侧记录"]["分片状态"] == "待分配",
      str(cross))
check("核对清单含去重依据与历史补写",
      checklist["缺口"]["去重依据"] == [2]
      and any(r["补写编号"] == "GAPR-0001" for r in checklist["历史补写"]))

# ---------- 8. 台账空态 ----------------------------------------------------
empty_rewrites = NodecheckService().list_rewrites("MERG-1004")
check("补写台账空态有说明", empty_rewrites["total"] == 0 and "暂无缺口补写记录" in empty_rewrites["空态说明"])

# ---------- 9. 模块差异说明 ------------------------------------------------
diff = check_svc.module_difference()
check("巡检模块与节点巡检差异可梳理",
      diff["节点状态巡检"]["巡检对象"] == "采集节点"
      and diff["聚合失败缺口保护"]["巡检对象"] == "聚合任务"
      and {m["边界类型"] for m in diff["边界模式对比"]} == {gap.BOUNDARY_WINDOW, gap.BOUNDARY_WATERMARK})

# ---------- 10. 别的业务入口触发重算：队列随台账实时刷新 -------------------
before_summary = check_svc.protection_queue()["汇总"]
# 模拟其他入口把 MERG-1004 的空清单登记一个已归并分片（空态 -> 已完成）
m1004 = next(r for r in store.rows(gap.MANIFEST_MODULE) if r["任务编号"] == "MERG-1004")
m1004["分片清单"].append({
    "分片编号": 1, "状态": "已归并", "窗口开始": "2026-09-12T00:00",
    "窗口结束": "2026-09-12T12:00", "数据量": 88, "校验和": "sha256:empty-1",
})
recomputed = check_svc.protection_queue()
check("别的业务入口重算后空态任务减少",
      recomputed["汇总"]["空结果任务数"] == before_summary["空结果任务数"] - 1
      and not any(e["任务编号"] == "MERG-1004" for e in recomputed["空态任务"]))
m1004_after = next(e for e in recomputed["队列"] if e["任务编号"] == "MERG-1004")
check("重算后新分片三指标同源更新",
      m1004_after["分片总数"] == 1 and m1004_after["已归并数"] == 1
      and m1004_after["数据总量"] == 88 and m1004_after["进度分类"] == gap.PROGRESS_COMPLETE)

# 无任何补写的任务重算补写台账，空态必须带原因说明；异常台账空过滤场景同理
fresh_rewrites = check_svc.list_rewrites("MERG-1001")
check("无补写任务台账空态带原因", fresh_rewrites["total"] == 0
      and "暂无缺口补写记录" in fresh_rewrites["空态说明"])
# MERG-1001 从未登记过聚合缺口异常，按任务视角应为空清单结构
all_exceptions = check_svc.list_exceptions()
check("异常台账始终返回空态说明字段", "空态说明" in all_exceptions
      and all(e["采用补写边界"]["补写编号"] for e in all_exceptions["items"]))

print()
if failures:
    print(f"共 {len(failures)} 项失败：{failures}")
    sys.exit(1)
print("全部自检通过")
