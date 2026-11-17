<template>
  <section class="page" data-module="merge">
    <header class="page-head">
      <div>
        <h2>结果聚合归并管理</h2>
        <p class="page-desc">维护采集结果的聚合策略、进度追踪与输出管理；聚合进度与缺口保护字段来自同一条归并清单，可直接复核。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记聚合任务</button>
        <button class="btn" type="button" @click="exportRows">导出结果聚合归并清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <h3>聚合进度（分片总数 / 已归并数 / 数据总量同源计算）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>任务编号</th><th>采集计划</th><th>进度分类</th><th>分片总数</th><th>已归并数</th>
          <th>数据总量</th><th>部分写入量</th><th>分片完成水位</th>
          <th>待补缺失分片</th><th>写到一半分片</th><th>已补写分片</th><th>缺口操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in progressRows" :key="row.任务编号">
          <td>{{ row.任务编号 }}</td>
          <td>{{ row.采集计划 }}</td>
          <td>
            <span :class="['badge', badgeClass(row.进度分类)]">{{ row.进度分类 }}</span>
            <p class="cell-note">{{ row.进度说明 }}</p>
          </td>
          <td>{{ row.分片总数 }}</td>
          <td>{{ row.已归并数 }}</td>
          <td>{{ row.数据总量 }}</td>
          <td>{{ row.部分写入量 }}</td>
          <td>{{ row.分片完成水位 }}</td>
          <td>{{ formatList(row.待补缺失分片) }}</td>
          <td>{{ formatList(row.待补写到一半分片) }}</td>
          <td>{{ formatList(row.已补写分片) }}</td>
          <td>
            <button class="link" type="button" @click="openRewrite(row)">缺口补写</button>
          </td>
        </tr>
        <tr v-if="!progressRows.length">
          <td colspan="12" class="empty-state">{{ progressEmpty || '暂无归并清单：没有可归并分片，归并输出为空结果' }}</td>
        </tr>
      </tbody>
    </table>
    <p v-if="progressRows.length" class="cell-note">计算来源逐行标注（merge_manifest 记录 id），三指标共用同一快照，不存在口径漂移。</p>

    <form v-if="rewriteForm.visible" class="filter-bar rewrite-panel" @submit.prevent="submitRewrite">
      <h4>缺口补写 · {{ rewriteForm.任务编号 }}（只处理当前缺口，不覆盖确认数据）</h4>
      <label class="filter-item">
        <span>补写边界</span>
        <select v-model="rewriteForm.boundary_type">
          <option value="shard_watermark">分片编号水位（补写结果只含缺失分片）</option>
          <option value="time_window">业务时间窗口（附扫描未覆盖确认记录）</option>
        </select>
      </label>
      <label v-if="rewriteForm.boundary_type === 'shard_watermark'" class="filter-item">
        <span>缺失分片编号（逗号分隔）</span>
        <input v-model="rewriteForm.shardNos" placeholder="如 2,3" />
      </label>
      <template v-else>
        <label class="filter-item">
          <span>窗口开始</span>
          <input v-model="rewriteForm.windowStart" placeholder="2026-09-13T16:00" />
        </label>
        <label class="filter-item">
          <span>窗口结束</span>
          <input v-model="rewriteForm.windowEnd" placeholder="2026-09-14T00:00" />
        </label>
      </template>
      <label class="filter-item">
        <span>模拟失败分片（可选，演示中断续做）</span>
        <input v-model="rewriteForm.failShards" placeholder="如 3" />
      </label>
      <button class="btn primary" type="submit">提交补写</button>
      <button class="btn ghost" type="button" @click="rewriteForm.visible = false">取消</button>
      <p v-if="rewriteResult" class="cell-note">{{ rewriteResult }}</p>
    </form>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无结果聚合归并数据，可先登记聚合任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条结果聚合归并记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type ProgressRow = Record<string, any>

const ENDPOINT = '/api/merge'
const columns = ["任务编号", "采集计划", "分片总数", "已归并数", "归并策略", "输出路径", "数据总量", "任务状态"]
const actions = ["启动归并", "重试归并", "归档任务"]
const stats = [{ "label": "聚合任务总数", "value": 0 }, { "label": "归并中任务", "value": 0 }, { "label": "已完成任务", "value": 0 }]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const progressRows = ref<ProgressRow[]>([])
const progressEmpty = ref('')
const rewriteResult = ref('')
const rewriteForm = reactive({
  visible: false,
  任务编号: '',
  boundary_type: 'shard_watermark',
  shardNos: '',
  windowStart: '',
  windowEnd: '',
  failShards: '',
})

function formatList(value: unknown): string {
  return Array.isArray(value) && value.length ? value.join('、') : '—'
}

function badgeClass(category: string): string {
  if (category === '已完成') return 'badge-ok'
  if (category === '空结果') return 'badge-muted'
  if (category === '存在写到一半范围') return 'badge-warn'
  return 'badge-danger'
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '聚合任务登记入口尚未接入审批流'
}

function openRewrite(row: ProgressRow) {
  rewriteForm.visible = true
  rewriteForm.任务编号 = row.任务编号
  rewriteForm.boundary_type = 'shard_watermark'
  rewriteForm.shardNos = [...(row.待补缺失分片 ?? []), ...(row.待补写到一半分片 ?? [])].join(',')
  rewriteForm.windowStart = ''
  rewriteForm.windowEnd = ''
  rewriteForm.failShards = ''
  rewriteResult.value = ''
}

async function submitRewrite() {
  errorMessage.value = ''
  rewriteResult.value = ''
  const values: Record<string, any> = { boundary_type: rewriteForm.boundary_type }
  if (rewriteForm.boundary_type === 'shard_watermark') {
    const nos = rewriteForm.shardNos.split(',').map((s) => parseInt(s.trim(), 10)).filter((n) => !Number.isNaN(n))
    values.缺失分片编号 = nos
  } else {
    values.窗口开始 = rewriteForm.windowStart.trim()
    values.窗口结束 = rewriteForm.windowEnd.trim()
  }
  const fails = rewriteForm.failShards.split(',').map((s) => parseInt(s.trim(), 10)).filter((n) => !Number.isNaN(n))
  if (fails.length) values.模拟失败分片 = fails

  try {
    const response = await request(`${ENDPOINT}/manifest/${encodeURIComponent(rewriteForm.任务编号)}/gap-rewrite`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '缺口补写未生效')
    }
    const rec = payload.entry.补写记录
    rewriteResult.value = `补写 ${rec.补写编号} 已落账：${rec.结果状态}，处理分片 ${formatList(rec.处理分片)}，续做范围 ${JSON.stringify(rec.续做范围)}`
    rewriteForm.visible = false
    await Promise.all([loadProgress(), reload()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '缺口补写失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('结果聚合归并动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并操作失败'
  }
}

async function loadProgress() {
  try {
    const response = await request(`${ENDPOINT}/progress`)
    if (!response.ok) throw new Error('聚合进度读取失败')
    const payload = await response.json()
    progressRows.value = payload.items ?? []
    progressEmpty.value = payload.空态说明 ?? ''
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '聚合进度读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('聚合任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并列表读取失败'
  }
}

onMounted(() => {
  void loadProgress()
  void reload()
})
</script>

<style scoped>
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  background: #e2e8f0;
  color: #334155;
  white-space: nowrap;
}
.badge-ok { background: #dcfce7; color: #166534; }
.badge-warn { background: #fef3c7; color: #92400e; }
.badge-danger { background: #fee2e2; color: #991b1b; }
.badge-muted { background: #f1f5f9; color: #64748b; }
.cell-note { margin: 2px 0; font-size: 12px; color: var(--muted); }
.rewrite-panel {
  align-items: flex-end;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: #fff;
  padding: 12px;
  margin: 12px 0;
}
.rewrite-panel h4 { margin: 0 0 8px; width: 100%; }
</style>
