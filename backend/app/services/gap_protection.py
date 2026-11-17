"""聚合失败缺口保护：缺口识别、去重补写与巡检异常登记。

数据流定位
==========

分片任务（shard 表）产生「分片完成水位」，结果聚合归并（merge_manifest 表）保存
每个聚合任务的权威归并清单；本模块在这两份数据之上做三件事：

1. 识别归并缺口并输出「聚合失败缺口保护核对清单」（派生视图，随时重算）；
2. 执行缺口补写，结果只追加到 gap_rewrite 表，不覆盖任何已确认数据；
3. 把补写结果登记为巡检模块的聚合任务异常（inspect_exception 表）。

与节点状态巡检（nodecheck 表：节点 CPU/内存/磁盘等资源指标）的区别：本模块的
巡检对象是「聚合任务的缺口」，不依赖单台节点的采样指标。

关键约束
========

* 分片总数、已归并数、数据总量必须从同一条 merge_manifest 记录计算，保证口径一致；
* 每次补写只处理当前缺口，且与历史补写记录去重，同一分片不会被重复累计；
* 补写只追加，不覆盖；失败也落账并保留「续做范围」，下次只补剩余缺口，
  不会生成第二份完整输出；
* 同一缺口同时存在业务时间窗口与缺失分片编号冲突时，两种边界都能继续，
  巡检异常按各自维度汇总，异常清单必须指向实际采用的补写边界。
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from app.store import store

MANIFEST_MODULE = "merge_manifest"
REWRITE_MODULE = "gap_rewrite"
EXCEPTION_MODULE = "inspect_exception"
SHARD_MODULE = "shard"

# 分片在归并清单里的三种状态：已归并（确认数据）、缺失（分片完成水位没到）、
# 写到一半（输出只落了一部分，中断恢复的对象）。
SHARD_MERGED = "已归并"
SHARD_MISSING = "缺失"
SHARD_PARTIAL = "写到一半"

# 补写边界类型：业务时间窗口 / 分片编号水位。
BOUNDARY_WINDOW = "time_window"
BOUNDARY_WATERMARK = "shard_watermark"
BOUNDARY_LABELS = {
    BOUNDARY_WINDOW: "业务时间窗口",
    BOUNDARY_WATERMARK: "分片编号水位",
}

# 聚合进度的四种可复核分类。
PROGRESS_COMPLETE = "已完成"
PROGRESS_EMPTY = "空结果"
PROGRESS_MISSING = "存在缺失分片"
PROGRESS_PARTIAL = "存在写到一半范围"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _next_code(rows: list[dict[str, Any]], prefix: str, code_field: str) -> str:
    seq = 0
    for row in rows:
        code = str(row.get(code_field, ""))
        if code.startswith(prefix):
            tail = code[len(prefix):]
            if tail.isdigit():
                seq = max(seq, int(tail))
    return f"{prefix}{seq + 1:04d}"


def _manifest_map() -> dict[str, dict[str, Any]]:
    return {str(row["任务编号"]): row for row in store.rows(MANIFEST_MODULE)}


def _manifest_by_id(manifest_id: int) -> dict[str, Any] | None:
    return store.find(MANIFEST_MODULE, manifest_id)


def _manifest_by_task(task_no: str) -> dict[str, Any] | None:
    return _manifest_map().get(str(task_no))


def _shards_of(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """返回分片清单的副本，保证计算过程不会改到仓库里的确认数据。"""
    return [dict(item) for item in manifest.get("分片清单", [])]


def compute_progress(manifest: dict[str, Any]) -> dict[str, Any]:
    """从同一条聚合记录计算分片总数、已归并数、数据总量与进度分类。

    三个指标共用同一份分片清单快照，任何入口读到的口径都一致。
    """
    shards = _shards_of(manifest)
    total = len(shards)
    merged = [s for s in shards if s.get("状态") == SHARD_MERGED]
    partial = [s for s in shards if s.get("状态") == SHARD_PARTIAL]
    missing = [s for s in shards if s.get("状态") == SHARD_MISSING]

    def _volume(bucket: list[dict[str, Any]]) -> int:
        return sum(int(s.get("数据量") or 0) for s in bucket)

    if total == 0:
        category = PROGRESS_EMPTY
        reason = "没有可归并分片：分片清单为空，归并输出为空结果而非失败"
    elif missing:
        category = PROGRESS_MISSING
        reason = f"分片完成水位未覆盖缺失分片：{'、'.join(str(s['分片编号']) for s in missing)}"
    elif partial:
        category = PROGRESS_PARTIAL
        reason = f"归并输出写到一半的范围：{'、'.join(str(s['分片编号']) for s in partial)}，需中断恢复"
    else:
        category = PROGRESS_COMPLETE
        reason = "全部分片已归并，缺口为空"

    # 分片完成水位：连续已归并到的最大编号（缺口之后的已归并不计入水位）。
    watermark = 0
    for no in range(1, total + 1):
        current = next((s for s in shards if int(s.get("分片编号", 0)) == no), None)
        if current and current.get("状态") == SHARD_MERGED:
            watermark = no
        else:
            break

    return {
        "任务编号": manifest.get("任务编号"),
        "采集计划": manifest.get("采集计划"),
        "分片总数": total,
        "已归并数": len(merged),
        "数据总量": _volume(merged),
        "部分写入量": _volume(partial),
        "缺失分片": [int(s["分片编号"]) for s in missing],
        "写到一半分片": [int(s["分片编号"]) for s in partial],
        "分片完成水位": watermark,
        "进度分类": category,
        "进度说明": reason,
        "计算来源": f"merge_manifest#id={manifest.get('id')}",
    }


def _rewrites_of(task_no: str) -> list[dict[str, Any]]:
    return [r for r in store.rows(REWRITE_MODULE) if r.get("任务编号") == task_no]


def _already_rewritten(task_no: str) -> set[int]:
    """历史补写已经累计过的分片：去重的唯一口径，失败续做也不会重复。"""
    done: set[int] = set()
    for record in _rewrites_of(task_no):
        for no in record.get("处理分片", []):
            done.add(int(no))
    return done


def identify_gaps(manifest: dict[str, Any]) -> dict[str, Any]:
    """识别当前缺口并去重：缺失分片、写到一半范围各自成组，剔除已补写分片。"""
    progress = compute_progress(manifest)
    rewritten = _already_rewritten(str(manifest.get("任务编号")))
    missing_pending = [no for no in progress["缺失分片"] if no not in rewritten]
    missing_covered = [no for no in progress["缺失分片"] if no in rewritten]
    partial_pending = [no for no in progress["写到一半分片"] if no not in rewritten]

    shards = {int(s["分片编号"]): s for s in _shards_of(manifest)}

    def _window_of(no: int) -> dict[str, Any] | None:
        item = shards.get(no)
        if not item:
            return None
        return {"开始": item.get("窗口开始"), "结束": item.get("窗口结束")}

    return {
        "任务编号": manifest.get("任务编号"),
        "采集计划": manifest.get("采集计划"),
        "缺失分片": progress["缺失分片"],
        "待补缺失分片": sorted(missing_pending),
        "已补写缺失分片": sorted(missing_covered),
        "写到一半分片": progress["写到一半分片"],
        "待补写到一半分片": sorted(partial_pending),
        "待补窗口": [w for no in sorted(missing_pending + partial_pending) if (w := _window_of(no))],
        "分片完成水位": progress["分片完成水位"],
        "去重依据": sorted(rewritten),
    }


def _cross_check_shards(manifest: dict[str, Any], gap_nos: list[int]) -> list[dict[str, Any]]:
    """用分片调度表交叉核对缺口：调度侧有记录但归并清单缺失/半写，说明缺口属实。"""
    plan = manifest.get("采集计划")
    schedule = [
        row for row in store.rows(SHARD_MODULE)
        if row.get("采集计划") == plan and str(row.get("分片编号", "")).startswith("SHAR-")
    ]
    by_no: dict[int, dict[str, Any]] = {}
    for row in schedule:
        tail = str(row["分片编号"]).split("-")[-1]
        if tail.isdigit():
            # SHAR-2002 对应归并清单里的局部分片编号 2（取末位序号），
            # 而不是把完整流水号 2002 当作分片编号。
            by_no[int(tail[-1])] = row
    findings: list[dict[str, Any]] = []
    for no in gap_nos:
        row = by_no.get(no)
        findings.append({
            "分片编号": no,
            "调度侧记录": None if row is None else {
                "分片编号": row.get("分片编号"),
                "分片状态": row.get("分片状态"),
                "分配节点": row.get("分配节点") or "未分配",
            },
            "核对结论": "调度侧无记录，缺口来自归并水位" if row is None else f"调度侧状态为{row.get('分片状态')}，缺口属实",
        })
    return findings


def protection_checklist(manifest: dict[str, Any]) -> dict[str, Any]:
    """聚合失败缺口保护核对清单：单任务的完整复核视图（派生，每次重算）。"""
    progress = compute_progress(manifest)
    gaps = identify_gaps(manifest)
    pending = sorted(gaps["待补缺失分片"] + gaps["待补写到一半分片"])
    cross = _cross_check_shards(manifest, gaps["缺失分片"] + gaps["写到一半分片"])
    rewrites = [
        {
            "补写编号": r.get("补写编号"),
            "补写边界类型": r.get("补写边界类型"),
            "处理分片": r.get("处理分片"),
            "结果状态": r.get("结果状态"),
            "补写时间": r.get("补写时间"),
        }
        for r in _rewrites_of(str(manifest.get("任务编号")))
    ]
    exceptions = [
        {
            "异常编号": e.get("异常编号"),
            "汇总维度": e.get("汇总维度"),
            "异常状态": e.get("异常状态"),
            "采用补写边界": e.get("采用补写边界"),
        }
        for e in store.rows(EXCEPTION_MODULE)
        if e.get("任务编号") == manifest.get("任务编号")
    ]
    return {
        "任务编号": manifest.get("任务编号"),
        "采集计划": manifest.get("采集计划"),
        "进度": progress,
        "缺口": gaps,
        "待补分片": pending,
        "分片交叉核对": cross,
        "历史补写": rewrites,
        "巡检异常": exceptions,
        "重算时间": _now(),
    }


def protection_queue() -> dict[str, Any]:
    """聚合失败缺口保护队列：跨任务重算。别的业务入口触发重算时直接调这里。

    队列只反映当前仓库状态，不做缓存：补写落账、清单更新后下一次读取即新结果。
    """
    items: list[dict[str, Any]] = []
    empty: list[dict[str, Any]] = []
    for manifest in store.rows(MANIFEST_MODULE):
        progress = compute_progress(manifest)
        gaps = identify_gaps(manifest)
        entry = {
            "任务编号": progress["任务编号"],
            "采集计划": progress["采集计划"],
            "进度分类": progress["进度分类"],
            "进度说明": progress["进度说明"],
            "分片总数": progress["分片总数"],
            "已归并数": progress["已归并数"],
            "数据总量": progress["数据总量"],
            "分片完成水位": progress["分片完成水位"],
            "待补分片": sorted(gaps["待补缺失分片"] + gaps["待补写到一半分片"]),
            "待补缺失分片": gaps["待补缺失分片"],
            "待补写到一半分片": gaps["待补写到一半分片"],
            "已补写分片": gaps["已补写缺失分片"],
            "异常数": sum(
                1 for e in store.rows(EXCEPTION_MODULE)
                if e.get("任务编号") == manifest.get("任务编号") and e.get("异常状态") != "已关闭"
            ),
        }
        items.append(entry)
        if progress["进度分类"] == PROGRESS_EMPTY:
            empty.append({
                "任务编号": progress["任务编号"],
                "空态说明": progress["进度说明"],
            })

    actionable = [e for e in items if e["待补分片"]]
    summary = {
        "聚合任务数": len(items),
        "待补任务数": len(actionable),
        "空结果任务数": len(empty),
        "缺失分片总数": sum(len(e["待补缺失分片"]) for e in items),
        "写到一半范围总数": sum(len(e["待补写到一半分片"]) for e in items),
        "巡检异常总数": sum(1 for e in store.rows(EXCEPTION_MODULE) if e.get("异常状态") != "已关闭"),
    }
    return {"队列": items, "待处理": actionable, "空态任务": empty, "汇总": summary, "重算时间": _now()}


def _validate_boundary(
    boundary_type: str,
    values: dict[str, Any],
    gaps: dict[str, Any],
    shards_by_no: dict[int, dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    """校验补写边界并算出本次允许处理的分片集合（只含当前缺口、去重后）。"""
    pending_missing = set(gaps["待补缺失分片"])
    pending_partial = set(gaps["待补写到一半分片"])
    pending_all = pending_missing | pending_partial
    if boundary_type == BOUNDARY_WATERMARK:
        raw = values.get("缺失分片编号") or []
        if not isinstance(raw, list) or not raw:
            return None, "分片编号水位补写必须提供非空的缺失分片编号列表"
        try:
            requested = [int(no) for no in raw]
        except (TypeError, ValueError):
            return None, "缺失分片编号必须是整数列表"
        unknown = [no for no in requested if no not in shards_by_no]
        if unknown:
            return None, f"分片编号超出归并清单范围：{unknown}"
        not_gap = [no for no in requested if no not in pending_all]
        if not_gap:
            return None, (
                f"分片 {not_gap} 不是当前待补缺口（已归并或已补写），"
                "数据不能被重试覆盖，本次拒绝处理"
            )
        boundary = {
            "缺失分片编号": sorted(requested),
            "水位说明": f"分片编号水位：仅缺失分片 {'、'.join(map(str, sorted(requested)))}",
        }
        return {"boundary": boundary, "targets": sorted(set(requested))}, ""

    if boundary_type == BOUNDARY_WINDOW:
        start = str(values.get("窗口开始") or "").strip()
        end = str(values.get("窗口结束") or "").strip()
        if not start or not end:
            return None, "业务时间窗口补写必须提供窗口开始与窗口结束"
        if start >= end:
            return None, "业务时间窗口非法：窗口开始必须早于窗口结束"
        # 窗口边界按分片自带的业务时间窗口匹配，窗口内的待补缺口都纳入。
        targets = sorted(
            no for no in pending_all
            if str(shards_by_no[no].get("窗口开始") or "") >= start
            and str(shards_by_no[no].get("窗口结束") or "") <= end
        )
        # 扫描但未覆盖的确认记录：窗口内已经归并/已补写的分片，补写结果要列出来。
        scanned_uncovered = []
        for no, item in sorted(shards_by_no.items()):
            inside = str(item.get("窗口开始") or "") >= start and str(item.get("窗口结束") or "") <= end
            if not inside:
                continue
            if no in targets:
                continue
            scanned_uncovered.append({
                "分片编号": no,
                "状态": item.get("状态"),
                "未覆盖原因": "已归并确认数据，补写不得覆盖" if item.get("状态") == SHARD_MERGED else "该分片已在历史补写中累计，去重跳过",
            })
        boundary = {
            "业务时间窗口": {"开始": start, "结束": end},
            "窗口说明": f"业务时间窗口：{start} ~ {end}",
        }
        return {
            "boundary": boundary,
            "targets": targets,
            "scanned_uncovered": scanned_uncovered,
        }, ""

    return None, f"未知补写边界类型：{boundary_type}，仅支持 {BOUNDARY_WINDOW} / {BOUNDARY_WATERMARK}"


def rewrite_gap(
    *,
    task_no: str,
    boundary_type: str,
    values: dict[str, Any],
    fail_shards: list[int] | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """对当前缺口执行一次补写。

    去重与不覆盖在边界校验阶段完成；补写结果只包含本次新处理的分片。
    fail_shards 用于模拟写入失败：命中的分片保留在「续做范围」里，下次可继续，
    已成功的分片照常落账，不会回滚、也不会在续做时重复累计。
    """
    manifest = _manifest_by_task(task_no)
    if manifest is None:
        return None, f"聚合任务 {task_no} 不存在或已归档"
    gaps = identify_gaps(manifest)
    shards_by_no = {int(s["分片编号"]): s for s in _shards_of(manifest)}
    checked, error = _validate_boundary(boundary_type, values, gaps, shards_by_no)
    if error:
        return None, error
    assert checked is not None
    targets: list[int] = checked["targets"]
    if not targets:
        return None, "当前补写边界内没有待补缺口；空补写不产生输出，也不登记异常"

    fail_set = {int(no) for no in (fail_shards or [])}
    unknown_fail = fail_set - set(targets)
    if unknown_fail:
        return None, f"模拟失败分片 {sorted(unknown_fail)} 不在本次补写目标内"

    processed: list[int] = []
    results: list[dict[str, Any]] = []
    failed: list[int] = []
    for no in targets:
        source = shards_by_no[no]
        if no in fail_set:
            failed.append(no)
            continue
        # 补写生成的新数据：写到一半的分片按缺口重新归并，缺失分片按窗口补全。
        volume = int(source.get("数据量") or 0)
        if source.get("状态") == SHARD_MISSING or volume <= 0:
            volume = _simulated_volume(manifest, no)
        results.append({
            "分片编号": no,
            "状态": SHARD_MERGED,
            "窗口开始": source.get("窗口开始"),
            "窗口结束": source.get("窗口结束"),
            "数据量": volume,
            "校验和": f"sha256:gap-{manifest.get('任务编号')}-{no}",
        })
        processed.append(no)

    # 只把成功的分片回写到归并清单（确认数据更新），失败的维持原状态等续做。
    if processed:
        for item in manifest.get("分片清单", []):
            if int(item.get("分片编号", 0)) in processed:
                done = next(r for r in results if r["分片编号"] == int(item["分片编号"]))
                item["状态"] = SHARD_MERGED
                item["数据量"] = done["数据量"]
                item["校验和"] = done["校验和"]
        manifest["最后更新"] = _now()

    if failed and processed:
        status = "部分成功"
        reason = f"分片 {failed} 写入中断，已保留续做范围；成功分片 {processed} 已落账不回滚"
    elif failed:
        status = "补写失败"
        reason = f"分片 {failed} 全部写入失败，续做范围保持原缺口，未产生覆盖写入"
    else:
        status = "补写完成"
        reason = "本次缺口全部补写完成，仅写入当前缺口分片"

    # 续做范围：仍未补齐的缺口，按本次边界形态保存。
    progress_after = compute_progress(manifest)
    rewritten_after = _already_rewritten(task_no)
    remain = [
        no for no in (progress_after["缺失分片"] + progress_after["写到一半分片"])
        if no not in rewritten_after
    ]
    if boundary_type == BOUNDARY_WINDOW:
        resume_scope = {
            "业务时间窗口": checked["boundary"].get("业务时间窗口"),
            "缺失分片编号": sorted(remain),
        }
    else:
        resume_scope = {"缺失分片编号": sorted(remain), "业务时间窗口": None}

    rewrite_rows = store.rows(REWRITE_MODULE)
    record = {
        "id": _next_id(rewrite_rows),
        "补写编号": _next_code(rewrite_rows, "GAPR-", "补写编号"),
        "任务编号": task_no,
        "采集计划": manifest.get("采集计划"),
        "补写边界类型": boundary_type,
        "补写边界": checked["boundary"],
        "处理分片": processed,
        "扫描未覆盖记录": checked.get("scanned_uncovered", []),
        "补写结果": results,
        "结果状态": status,
        "失败原因": reason if failed else "",
        "续做范围": resume_scope,
        "补写依据": {
            "归并清单快照": [f"{task_no}@id={manifest.get('id')}"],
            "分片完成水位": progress_after["分片完成水位"],
            "补前去重依据": gaps["去重依据"],
            "边界内目标分片": targets,
        },
        "补写时间": _now(),
    }
    rewrite_rows.append(record)

    # 巡检异常登记：汇总维度跟随采用的补写边界。
    exception = _register_exception(
        manifest=manifest,
        rewrite=record,
        gaps_before=gaps,
        remain=remain,
        failed=failed,
    )
    return {"补写记录": record, "巡检异常": exception, "续后进度": compute_progress(manifest)}, ""


def _simulated_volume(manifest: dict[str, Any], shard_no: int) -> int:
    """演示环境的补写数据量：用任务编号与分片编号派生稳定值，保证可复核。"""
    seed = sum(ord(ch) for ch in str(manifest.get("任务编号", "")))
    return 200 + (seed % 400) + shard_no * 37


def _register_exception(
    *,
    manifest: dict[str, Any],
    rewrite: dict[str, Any],
    gaps_before: dict[str, Any],
    remain: list[int],
    failed: list[int],
) -> dict[str, Any]:
    """把补写结果登记为巡检模块的聚合任务异常，并保存补写依据回指。"""
    boundary_type = rewrite["补写边界类型"]
    if boundary_type == BOUNDARY_WINDOW:
        dimension = "time_window"
        gap_detail = rewrite["补写边界"].get("业务时间窗口")
        dimension_label = "按业务时间窗口汇总"
    else:
        dimension = "shard_no"
        gap_detail = sorted(gaps_before["待补缺失分片"] + gaps_before["待补写到一半分片"])
        dimension_label = "按分片编号列出"

    if failed:
        ex_status = "处理中"
        cause = (
            f"{dimension_label}：补写 {rewrite['补写编号']} 存在写入中断分片 {failed}，"
            f"续做范围 {remain}，异常清单指向边界 {rewrite['补写边界']}"
        )
    elif remain:
        ex_status = "处理中"
        cause = f"{dimension_label}：仍有缺口 {remain} 未补，等待下一次补写"
    else:
        ex_status = "已关闭"
        cause = f"{dimension_label}：缺口已由补写 {rewrite['补写编号']} 补齐，关闭异常"

    rows = store.rows(EXCEPTION_MODULE)
    exception = {
        "id": _next_id(rows),
        "异常编号": _next_code(rows, "INSP-GAP-", "异常编号"),
        "任务编号": manifest.get("任务编号"),
        "采集计划": manifest.get("采集计划"),
        "异常类型": "聚合失败缺口",
        "汇总维度": dimension,
        "采用补写边界": {
            "补写编号": rewrite["补写编号"],
            "边界类型": boundary_type,
            "边界内容": rewrite["补写边界"],
        },
        "缺口明细": gap_detail,
        "异常状态": ex_status,
        "原因说明": cause,
        "补写依据回指": rewrite["补写编号"],
        "登记时间": _now(),
    }
    rows.append(exception)
    return exception


def list_rewrites(task_no: str | None = None) -> list[dict[str, Any]]:
    """缺口补写结果数据：只读追加台账，可按任务过滤。"""
    rows = store.rows(REWRITE_MODULE)
    if task_no:
        rows = [r for r in rows if r.get("任务编号") == task_no]
    return [deepcopy(r) for r in rows]


def list_exceptions(status: str | None = None) -> list[dict[str, Any]]:
    """巡检模块的聚合任务异常清单：只含聚合失败缺口，不含节点资源巡检。"""
    rows = store.rows(EXCEPTION_MODULE)
    if status:
        rows = [r for r in rows if r.get("异常状态") == status]
    return [deepcopy(r) for r in rows]


def recompute() -> dict[str, Any]:
    """供别的业务入口调用的重算入口：重算保护队列并给出空态/中断说明。"""
    queue = protection_queue()
    interruptions = [
        {
            "任务编号": item["任务编号"],
            "中断原因": item["进度说明"],
            "续做分片": item["待补写到一半分片"],
        }
        for item in queue["队列"]
        if item["进度分类"] == PROGRESS_PARTIAL
    ]
    queue["中断恢复说明"] = interruptions
    return queue
