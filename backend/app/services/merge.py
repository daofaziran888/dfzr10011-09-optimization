"""结果聚合归并业务规则：状态流转、字段校验与筛选口径都收在这里。

聚合进度（分片总数、已归并数、数据总量、缺口分类）统一从 merge_manifest 归并清单
计算，三个指标共用同一条聚合记录，避免各入口口径漂移。
"""
from __future__ import annotations

from typing import Any

from app.services import gap_protection as gap
from app.store import store

MODULE = "merge"
REQUIRED_FIELDS = ["任务编号", "采集计划", "分片总数"]
STATUS_ORDER = ["待归并", "归并中", "已完成", "归并失败"]
ACTION_RULES = {"启动归并": "待归并", "重试归并": "归并中", "归档任务": "归并失败"}
NEGATIVE_ACTIONS = []


class MergeService:
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

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"聚合任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于结果聚合归并可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"聚合任务已{action}"

    # ---- 聚合失败缺口保护：归并侧视图 -------------------------------------

    def manifest_progress(self) -> dict[str, Any]:
        """聚合进度看板：分片总数/已归并数/数据总量全部来自同一条归并清单。

        空结果、缺失分片、写到一半范围在这里显式区分，前端可直接复核字段。
        """
        items: list[dict[str, Any]] = []
        for manifest in store.rows(gap.MANIFEST_MODULE):
            progress = gap.compute_progress(manifest)
            gaps = gap.identify_gaps(manifest)
            items.append({
                "任务编号": progress["任务编号"],
                "采集计划": progress["采集计划"],
                "分片总数": progress["分片总数"],
                "已归并数": progress["已归并数"],
                "数据总量": progress["数据总量"],
                "部分写入量": progress["部分写入量"],
                "分片完成水位": progress["分片完成水位"],
                "进度分类": progress["进度分类"],
                "进度说明": progress["进度说明"],
                "待补缺失分片": gaps["待补缺失分片"],
                "待补写到一半分片": gaps["待补写到一半分片"],
                "已补写分片": gaps["已补写缺失分片"],
                "计算来源": progress["计算来源"],
            })
        return {
            "items": items,
            "total": len(items),
            "空态说明": (
                "暂无归并清单：没有可归并分片，归并输出为空结果"
                if not items else ""
            ),
        }

    def get_checklist(self, task_no: str) -> tuple[dict[str, Any] | None, str]:
        manifest = gap._manifest_by_task(task_no)
        if manifest is None:
            return None, f"聚合任务 {task_no} 不存在或已归档"
        return gap.protection_checklist(manifest), ""

    def rewrite_gap(
        self,
        task_no: str,
        boundary_type: str,
        values: dict[str, Any],
        fail_shards: list[int] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        return gap.rewrite_gap(
            task_no=task_no,
            boundary_type=boundary_type,
            values=values,
            fail_shards=fail_shards,
        )
