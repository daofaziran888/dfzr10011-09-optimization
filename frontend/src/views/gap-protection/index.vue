<template>
  <section class="page" data-module="gap-protection">
    <header class="page-head">
      <div>
        <h2>聚合失败缺口保护</h2>
        <p class="page-desc">
          巡检模块对聚合任务的缺口巡检：保护队列随台账实时重算，区分空结果、缺失分片与写到一半范围；
          缺口补写只追加、不覆盖，巡检异常必须指向采用的补写边界。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="reloadAll">重算保护队列</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in summaryCards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <nav class="filter-bar">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="btn"
        :class="{ primary: activeTab === tab.key }"
        type="button"
        @click="switchTab(tab.key)"
      >
        {{ tab.label }}
      </button>
    </nav>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <!-- 保护队列 -->
    <div v-if="activeTab === 'queue'">
      <table class="data-table">
        <thead>
          <tr>
            <th>任务编号</th>
            <th>采集计划</th>
            <th>进度分类</th>
            <th>分片总数</th>
            <th>已归并数</th>
            <th>数据总量</th>
            <th>分片完成水位</th>
            <th>待补缺失分片</th>
            <th>写到一半分片</th>
            <th>已补写分片</th>
            <th>巡检异常数</th>
            <th>核对清单</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in queueRows" :key="row.任务编号">
            <td>{{ row.任务编号 }}</td>
            <td>{{ row.采集计划 }}</td>
            <td>
              <span :class="['badge', badgeClass(row.进度分类)]">{{ row.进度分类 }}</span>
              <p class="cell-note">{{ row.进度说明 }}</p>
            </td>
            <td>{{ row.分片总数 }}</td>
            <td>{{ row.已归并数 }}</td>
            <td>{{ row.数据总量 }}</td>
            <td>{{ row.分片完成水位 }}</td>
            <td>{{ formatList(row.待补缺失分片) }}</td>
            <td>{{ formatList(row.待补写到一半分片) }}</td>
            <td>{{ formatList(row.已补写分片) }}</td>
            <td>{{ row.异常数 }}</td>
            <td><button class="link" type="button" @click="openChecklist(row.任务编号)">查看核对清单</button></td>
          </tr>
          <tr v-if="!queueRows.length">
            <td colspan="12" class="empty-state">暂无聚合任务，归并清单为空态：没有可归并分片</td>
          </tr>
        </tbody>
      </table>
      <footer class="page-foot">
        <span v-if="emptyTasks.length">
          空结果任务：<template v-for="task in emptyTasks" :key="task.任务编号">{{ task.任务编号 }}（{{ task.空态说明 }}）；</template>
        </span>
        <span v-else>无空结果任务</span>
        <span>重算时间：{{ queueMeta.重算时间 ?? '—' }}</span>
      </footer>
      <footer v-if="interruptions.length" class="page-foot">
        <span class="error-text">
          中断恢复：<template v-for="item in interruptions" :key="item.任务编号">
            {{ item.任务编号 }} 续做分片 {{ formatList(item.续做分片) }}（{{ item.中断原因 }}）；
          </template>
        </span>
      </footer>
    </div>

    <!-- 核对清单 -->
    <div v-else-if="activeTab === 'checklist'">
      <form class="filter-bar" @submit.prevent="loadChecklist">
        <label class="filter-item">
          <span>聚合任务编号</span>
          <input v-model="checklistTask" placeholder="如 MERG-1002" />
        </label>
        <button class="btn primary" type="submit">加载核对清单</button>
      </form>
      <template v-if="checklist">
        <h3>{{ checklist.任务编号 }} · {{ checklist.采集计划 }}</h3>
        <table class="data-table">
          <tbody>
            <tr><th>分片总数 / 已归并数 / 数据总量</th>
              <td>{{ checklist.进度.分片总数 }} / {{ checklist.进度.已归并数 }} / {{ checklist.进度.数据总量 }}（同源：{{ checklist.进度.计算来源 }}）</td></tr>
            <tr><th>进度分类与说明</th><td>{{ checklist.进度.进度分类 }} — {{ checklist.进度.进度说明 }}</td></tr>
            <tr><th>分片完成水位</th><td>{{ checklist.分片完成水位 }}</td></tr>
            <tr><th>待补缺失分片</th><td>{{ formatList(checklist.缺口.待补缺失分片) }}</td></tr>
            <tr><th>待补写到一半分片</th><td>{{ formatList(checklist.缺口.待补写到一半分片) }}</td></tr>
            <tr><th>已补写缺失分片（去重依据）</th><td>{{ formatList(checklist.缺口.去重依据) }}</td></tr>
          </tbody>
        </table>

        <h4>分片交叉核对（调度侧 vs 归并水位）</h4>
        <table class="data-table">
          <thead><tr><th>分片编号</th><th>调度侧记录</th><th>核对结论</th></tr></thead>
          <tbody>
            <tr v-for="cross in checklist.分片交叉核对" :key="cross.分片编号">
              <td>{{ cross.分片编号 }}</td>
              <td v-if="cross.调度侧记录">{{ cross.调度侧记录.分片编号 }} · {{ cross.调度侧记录.分片状态 }} · {{ cross.调度侧记录.分配节点 }}</td>
              <td v-else class="empty-state">无调度侧记录</td>
              <td>{{ cross.核对结论 }}</td>
            </tr>
            <tr v-if="!checklist.分片交叉核对.length"><td colspan="3" class="empty-state">该任务暂无缺口，无需交叉核对</td></tr>
          </tbody>
        </table>

        <h4>历史缺口补写</h4>
        <table class="data-table">
          <thead><tr><th>补写编号</th><th>边界类型</th><th>处理分片</th><th>结果状态</th><th>补写时间</th></tr></thead>
          <tbody>
            <tr v-for="r in checklist.历史补写" :key="r.补写编号">
              <td>{{ r.补写编号 }}</td>
              <td>{{ boundaryLabel(r.补写边界类型) }}</td>
              <td>{{ formatList(r.处理分片) }}</td>
              <td>{{ r.结果状态 }}</td>
              <td>{{ r.补写时间 }}</td>
            </tr>
            <tr v-if="!checklist.历史补写.length"><td colspan="5" class="empty-state">暂无缺口补写记录：当前没有触发过缺口补写</td></tr>
          </tbody>
        </table>
      </template>
      <p v-else class="empty-state">输入任务编号加载聚合失败缺口保护核对清单</p>
    </div>

    <!-- 缺口补写结果台账 -->
    <div v-else-if="activeTab === 'rewrite'">
      <table class="data-table">
        <thead>
          <tr>
            <th>补写编号</th><th>任务编号</th><th>补写边界</th><th>处理分片</th>
            <th>补写结果</th><th>扫描未覆盖记录</th><th>结果状态</th><th>失败原因/续做范围</th><th>补写依据</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in rewriteRows" :key="r.补写编号">
            <td>{{ r.补写编号 }}</td>
            <td>{{ r.任务编号 }}</td>
            <td>
              <span class="badge">{{ boundaryLabel(r.补写边界类型) }}</span>
              <p class="cell-note">{{ boundaryText(r.补写边界) }}</p>
            </td>
            <td>{{ formatList(r.处理分片) }}</td>
            <td>
              <p v-for="item in r.补写结果" :key="item.分片编号" class="cell-note">
                分片{{ item.分片编号 }}：{{ item.状态 }} / {{ item.数据量 }} / {{ item.校验和 }}
              </p>
              <span v-if="!r.补写结果.length" class="empty-state">无成功写入</span>
            </td>
            <td>
              <p v-for="u in r.扫描未覆盖记录" :key="u.分片编号" class="cell-note">
                分片{{ u.分片编号 }}（{{ u.状态 }}）：{{ u.未覆盖原因 }}
              </p>
              <span v-if="!r.扫描未覆盖记录.length" class="empty-state">—</span>
            </td>
            <td>{{ r.结果状态 }}</td>
            <td>
              <p v-if="r.失败原因" class="cell-note">{{ r.失败原因 }}</p>
              <p class="cell-note">续做：{{ resumeText(r.续做范围) }}</p>
            </td>
            <td>
              <p class="cell-note">水位：{{ r.补写依据.分片完成水位 }}</p>
              <p class="cell-note">去重：{{ formatList(r.补写依据.补前去重依据) }}</p>
              <p class="cell-note">目标：{{ formatList(r.补写依据.边界内目标分片) }}</p>
            </td>
          </tr>
          <tr v-if="!rewriteRows.length">
            <td colspan="9" class="empty-state">{{ rewriteMeta.空态说明 || '暂无缺口补写记录' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 巡检异常清单 -->
    <div v-else-if="activeTab === 'exception'">
      <table class="data-table">
        <thead>
          <tr>
            <th>异常编号</th><th>任务编号</th><th>异常类型</th><th>汇总维度</th>
            <th>缺口明细</th><th>采用的补写边界</th><th>异常状态</th><th>原因说明</th><th>补写依据回指</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="e in exceptionRows" :key="e.异常编号">
            <td>{{ e.异常编号 }}</td>
            <td>{{ e.任务编号 }}</td>
            <td>{{ e.异常类型 }}</td>
            <td>{{ e.汇总维度 === 'time_window' ? '按业务时间窗口汇总' : '按分片编号列出' }}</td>
            <td>{{ typeof e.缺口明细 === 'string' ? e.缺口明细 : formatList(e.缺口明细) }}</td>
            <td>
              <p class="cell-note">{{ e.采用补写边界.补写编号 }} · {{ boundaryLabel(e.采用补写边界.边界类型) }}</p>
              <p class="cell-note">{{ boundaryText(e.采用补写边界.边界内容) }}</p>
            </td>
            <td><span :class="['badge', e.异常状态 === '已关闭' ? 'badge-ok' : 'badge-warn']">{{ e.异常状态 }}</span></td>
            <td>{{ e.原因说明 }}</td>
            <td>{{ e.补写依据回指 }}</td>
          </tr>
          <tr v-if="!exceptionRows.length">
            <td colspan="9" class="empty-state">{{ exceptionMeta.空态说明 || '暂无聚合失败缺口异常' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 模块差异与边界对比 -->
    <div v-else-if="activeTab === 'difference' && difference">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">节点状态巡检（{{ difference.节点状态巡检.巡检对象 }}）</span>
          <p class="cell-note">数据来源：{{ difference.节点状态巡检.数据来源 }}</p>
          <p class="cell-note">核心字段：{{ difference.节点状态巡检.核心字段.join('、') }}</p>
          <p class="cell-note">记录数 {{ difference.节点状态巡检.记录数 }}，异常节点 {{ difference.节点状态巡检.异常节点数 }}</p>
        </article>
        <article class="stat-card">
          <span class="stat-label">聚合失败缺口保护（{{ difference.聚合失败缺口保护.巡检对象 }}）</span>
          <p class="cell-note">数据来源：{{ difference.聚合失败缺口保护.数据来源.join('、') }}</p>
          <p class="cell-note">核心字段：{{ difference.聚合失败缺口保护.核心字段.join('、') }}</p>
          <p class="cell-note">待补任务 {{ difference.聚合失败缺口保护.待补任务数 }}，巡检异常 {{ difference.聚合失败缺口保护.巡检异常总数 }}</p>
        </article>
      </div>
      <h4>按业务时间窗口补写 vs 按分片编号水位补写</h4>
      <table class="data-table">
        <thead><tr><th>边界类型</th><th>异常汇总维度</th><th>补写结果内容</th><th>续做范围</th></tr></thead>
        <tbody>
          <tr v-for="m in difference.边界模式对比" :key="m.边界类型">
            <td>{{ m.名称 }}（{{ m.边界类型 }}）</td>
            <td>{{ m.异常汇总维度 }}</td>
            <td>{{ m.补写结果内容 }}</td>
            <td>{{ m.续做范围 }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <footer class="page-foot">
      <span>数据来源：merge_manifest / gap_rewrite / inspect_exception 台账，保护队列每次读取实时重算</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type AnyRow = Record<string, any>

const BASE = '/api/nodecheck/gap-protection'
const tabs = [
  { key: 'queue', label: '保护队列' },
  { key: 'checklist', label: '缺口核对清单' },
  { key: 'rewrite', label: '缺口补写结果' },
  { key: 'exception', label: '巡检异常清单' },
  { key: 'difference', label: '模块差异与边界对比' },
] as const

const activeTab = ref<(typeof tabs)[number]['key']>('queue')
const errorMessage = ref('')
const queueRows = ref<AnyRow[]>([])
const emptyTasks = ref<AnyRow[]>([])
const interruptions = ref<AnyRow[]>([])
const queueMeta = ref<AnyRow>({})
const rewriteRows = ref<AnyRow[]>([])
const rewriteMeta = ref<AnyRow>({})
const exceptionRows = ref<AnyRow[]>([])
const exceptionMeta = ref<AnyRow>({})
const difference = ref<AnyRow | null>(null)
const checklist = ref<AnyRow | null>(null)
const checklistTask = ref('MERG-1002')

const summaryCards = computed(() => {
  const summary = queueMeta.value.汇总 ?? {}
  return [
    { label: '聚合任务数', value: summary.聚合任务数 ?? 0 },
    { label: '待补任务数', value: summary.待补任务数 ?? 0 },
    { label: '缺失分片总数', value: summary.缺失分片总数 ?? 0 },
    { label: '写到一半范围', value: summary.写到一半范围总数 ?? 0 },
    { label: '空结果任务', value: summary.空结果任务数 ?? 0 },
    { label: '巡检异常总数', value: summary.巡检异常总数 ?? 0 },
  ]
})

function formatList(value: unknown): string {
  return Array.isArray(value) && value.length ? value.join('、') : '—'
}

function boundaryLabel(type: string): string {
  return type === 'time_window' ? '业务时间窗口' : type === 'shard_watermark' ? '分片编号水位' : type
}

function boundaryText(boundary: AnyRow | undefined): string {
  if (!boundary) return '—'
  if (boundary.业务时间窗口) {
    return `${boundary.业务时间窗口.开始} ~ ${boundary.业务时间窗口.结束}`
  }
  if (Array.isArray(boundary.缺失分片编号)) {
    return `缺失分片 ${formatList(boundary.缺失分片编号)}`
  }
  return boundary.水位说明 ?? JSON.stringify(boundary)
}

function resumeText(scope: AnyRow | undefined): string {
  if (!scope) return '—'
  if (scope.业务时间窗口) {
    return `窗口 ${scope.业务时间窗口.开始} ~ ${scope.业务时间窗口.结束}，剩余分片 ${formatList(scope.缺失分片编号)}`
  }
  return `剩余分片 ${formatList(scope.缺失分片编号)}`
}

function badgeClass(category: string): string {
  if (category === '已完成') return 'badge-ok'
  if (category === '空结果') return 'badge-muted'
  if (category === '存在写到一半范围') return 'badge-warn'
  return 'badge-danger'
}

async function getJson(path: string): Promise<AnyRow> {
  const response = await request(path)
  if (!response.ok) {
    const payload = await response.json().catch(() => ({ detail: '接口返回异常' }))
    throw new Error(payload.detail ?? `接口返回 ${response.status}`)
  }
  return response.json()
}

async function loadQueue() {
  const data = await getJson(`${BASE}/queue`)
  queueRows.value = data.队列 ?? []
  emptyTasks.value = data.空态任务 ?? []
  interruptions.value = data.中断恢复说明 ?? []
  queueMeta.value = data
}

async function loadRewrites() {
  const data = await getJson(`${BASE}/rewrites`)
  rewriteRows.value = data.items ?? []
  rewriteMeta.value = data
}

async function loadExceptions() {
  const data = await getJson(`${BASE}/exceptions`)
  exceptionRows.value = data.items ?? []
  exceptionMeta.value = data
}

async function loadDifference() {
  difference.value = await getJson(`${BASE}/difference`)
}

async function loadChecklist() {
  errorMessage.value = ''
  checklist.value = null
  if (!checklistTask.value.trim()) {
    errorMessage.value = '请输入聚合任务编号'
    return
  }
  try {
    checklist.value = await getJson(`${BASE}/checklist/${encodeURIComponent(checklistTask.value.trim())}`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '核对清单加载失败'
  }
}

function openChecklist(taskNo: string) {
  checklistTask.value = taskNo
  activeTab.value = 'checklist'
  void loadChecklist()
}

async function switchTab(key: (typeof tabs)[number]['key']) {
  activeTab.value = key
  errorMessage.value = ''
  try {
    if (key === 'queue') await loadQueue()
    else if (key === 'rewrite') await loadRewrites()
    else if (key === 'exception') await loadExceptions()
    else if (key === 'difference') await loadDifference()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据加载失败'
  }
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    await Promise.all([loadQueue(), loadRewrites(), loadExceptions(), loadDifference()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重算失败'
  }
}

onMounted(reloadAll)
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
</style>
