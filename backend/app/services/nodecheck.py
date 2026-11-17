"""节点状态巡检业务规则：节点资源指标巡检 + 聚合失败缺口保护（巡检模块）。

两类对象在这里刻意分开：

* 节点状态巡检（nodecheck 表）：节点编号、CPU/内存/磁盘、网络延迟等采样指标，
  回答「某台节点现在健不健康」；
* 聚合失败缺口保护（merge_manifest / gap_rewrite / inspect_exception 三张台账）：
  巡检对象是聚合任务的归并缺口，回答「哪个聚合任务缺了哪些分片、补写依据是什么」。

保护队列与核对清单都是派生视图，任何业务入口调用 recompute() 都会按当前台账
重新计算，不缓存旧结果。
"""
from __future__ import annotations

from typing import Any

from app.services import gap_protection as gap
from app.store import store

MODULE = "nodecheck"
REQUIRED_FIELDS = ["记录编号", "节点编号", "CPU使用率"]
STATUS_ORDER = ["正常", "亚健康", "异常", "已下线"]
ACTION_RULES = {"标记异常": "亚健康", "隔离节点": "异常", "恢复节点": "已下线"}
NEGATIVE_ACTIONS = []


class NodecheckService:
    # ---- 节点状态巡检：节点资源指标（原有口径，保持不变） ------------------

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
            rows = [row for row in rows if keyword in str(row.get("记录编号", ""))]
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
            return None, f"巡检记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于节点状态巡检可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"巡检记录已{action}"

    # ---- 聚合失败缺口保护：巡检模块对聚合任务的巡检 -----------------------

    def protection_queue(self) -> dict[str, Any]:
        """重算聚合失败缺口保护队列：区分空结果、缺失分片、写到一半范围。"""
        return gap.recompute()

    def protection_checklist(self, task_no: str) -> tuple[dict[str, Any] | None, str]:
        manifest = gap._manifest_by_task(task_no)
        if manifest is None:
            return None, f"聚合任务 {task_no} 不存在或已归档"
        return gap.protection_checklist(manifest), ""

    def list_rewrites(self, task_no: str | None = None) -> dict[str, Any]:
        """缺口补写结果数据：只追加台账，返回时附带空态说明。"""
        items = gap.list_rewrites(task_no)
        return {
            "items": items,
            "total": len(items),
            "空态说明": "暂无缺口补写记录：当前没有触发过缺口补写" if not items else "",
        }

    def list_exceptions(self, status: str | None = None) -> dict[str, Any]:
        """聚合任务异常清单：每条异常都指向采用的补写边界与补写依据。"""
        items = gap.list_exceptions(status)
        return {
            "items": items,
            "total": len(items),
            "空态说明": "暂无聚合失败缺口异常：所有聚合任务均无待补缺口" if not items else "",
        }

    def module_difference(self) -> dict[str, Any]:
        """梳理巡检模块（缺口保护）与节点状态巡检的差异，供页面与接口复用。"""
        node_rows = store.rows(MODULE)
        abnormal_nodes = sum(1 for row in node_rows if row.get("status") in ("亚健康", "异常"))
        queue = gap.protection_queue()
        return {
            "节点状态巡检": {
                "巡检对象": "采集节点",
                "数据来源": "nodecheck 采样记录",
                "核心字段": ["节点编号", "CPU使用率", "内存使用率", "磁盘使用率", "网络延迟", "错误次数"],
                "记录数": len(node_rows),
                "异常节点数": abnormal_nodes,
            },
            "聚合失败缺口保护": {
                "巡检对象": "聚合任务",
                "数据来源": ["merge_manifest 归并清单", "gap_rewrite 缺口补写台账", "inspect_exception 巡检异常台账"],
                "核心字段": ["任务编号", "分片完成水位", "缺失分片", "写到一半分片", "补写边界", "补写依据"],
                "待补任务数": queue["汇总"]["待补任务数"],
                "巡检异常总数": queue["汇总"]["巡检异常总数"],
            },
            "边界模式对比": [
                {
                    "边界类型": gap.BOUNDARY_WINDOW,
                    "名称": gap.BOUNDARY_LABELS[gap.BOUNDARY_WINDOW],
                    "异常汇总维度": "time_window（按时间窗口汇总）",
                    "补写结果内容": "窗口内补写分片 + 扫描但未覆盖的确认记录",
                    "续做范围": "业务时间窗口 + 剩余缺失分片编号",
                },
                {
                    "边界类型": gap.BOUNDARY_WATERMARK,
                    "名称": gap.BOUNDARY_LABELS[gap.BOUNDARY_WATERMARK],
                    "异常汇总维度": "shard_no（按分片编号列出）",
                    "补写结果内容": "只包含缺失分片",
                    "续做范围": "剩余缺失分片编号",
                },
            ],
        }
