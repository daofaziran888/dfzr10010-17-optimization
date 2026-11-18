<template>
  <section class="page" data-module="compact">
    <header class="page-head">
      <div>
        <h2>小文件合并管理</h2>
        <p class="page-desc">
          按分片边界生成合并分组，失败组从原边界续跑；收益按成功分组只累计一次，并同步到收益复核清单。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记合并任务</button>
        <button class="btn" type="button" @click="exportRows">导出小文件合并清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <nav class="tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab"
        :class="{ active: activeTab === tab.key }"
        type="button"
        @click="switchTab(tab.key)"
      >
        {{ tab.label }}
      </button>
    </nav>

    <!-- 收益详情：查看结果数据集后可返回合并列表 -->
    <section v-if="detail" class="panel" data-panel="dataset-detail">
      <header class="panel-head">
        <div>
          <h3>收益详情 · {{ detail.任务编号 }}</h3>
          <p class="panel-desc">
            {{ detail.目标表名 }} · 结果数据集 {{ detail.dataset_id }}（{{ detail.范围版本 }}）·
            状态 {{ detail.状态 }} · 批次 {{ detail.batch }}
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" type="button" @click="closeDetail">返回合并列表</button>
          <button class="btn" type="button" @click="runDataset(detail.task_id)">执行剩余分组</button>
          <button class="btn" type="button" @click="resumeDataset(detail.task_id)">续跑</button>
          <button class="btn" type="button" @click="interruptDataset(detail.task_id)">中断</button>
          <button class="btn" type="button" @click="reconcile(detail.task_id)">同步复核清单</button>
        </div>
      </header>

      <div class="stat-row">
        <article class="stat-card"><span class="stat-label">分组总数</span><strong class="stat-value">{{ detail.分组总数 }}</strong></article>
        <article class="stat-card"><span class="stat-label">已完成组</span><strong class="stat-value">{{ detail.已完成分组 }}</strong></article>
        <article class="stat-card"><span class="stat-label">剩余分片</span><strong class="stat-value">{{ detail.剩余分片 }}</strong></article>
        <article class="stat-card"><span class="stat-label">空组</span><strong class="stat-value">{{ detail.空分组 }}</strong></article>
        <article class="stat-card"><span class="stat-label">文件数量下降</span><strong class="stat-value">{{ detail.文件数量下降 }}</strong></article>
        <article class="stat-card"><span class="stat-label">容量下降</span><strong class="stat-value">{{ detail.容量下降MB }}MB</strong></article>
      </div>

      <p class="hint">
        分片统计：共 {{ detail.分片统计.分片总数 }} 个分片，P50={{ detail.分片统计.P50MB }}MB，
        P95={{ detail.分片统计.P95MB }}MB，总容量 {{ detail.分片统计.总容量MB }}MB。
      </p>

      <h4 class="sub-title">已完成组（{{ detail.已完成组.length }}）</h4>
      <table class="data-table">
        <thead>
          <tr><th>分组</th><th>分区</th><th>起始分片</th><th>结束分片</th><th>分片数</th><th>文件下降</th><th>容量下降(MB)</th><th>尝试次数</th><th>记账</th></tr>
        </thead>
        <tbody>
          <tr v-for="group in detail.已完成组.slice(0, 12)" :key="group.group_id">
            <td>{{ group.group_id }}</td>
            <td>{{ group.分区 }}</td>
            <td>{{ group.起始分片 }}</td>
            <td>{{ group.结束分片 }}</td>
            <td>{{ group.shard_count }}</td>
            <td>{{ group.file_reduction }}</td>
            <td>{{ group.capacity_reduction }}</td>
            <td>{{ group.attempts }}</td>
            <td>{{ group.accounted ? '已累计一次' : '未记账' }}</td>
          </tr>
          <tr v-if="!detail.已完成组.length"><td colspan="9" class="empty-state">还没有已完成分组</td></tr>
        </tbody>
      </table>

      <h4 class="sub-title">失败组（{{ detail.失败组.length }}）</h4>
      <table class="data-table">
        <thead>
          <tr><th>分组</th><th>分区</th><th>原分片边界</th><th>尝试次数</th><th>失败说明</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="group in detail.失败组" :key="group.group_id">
            <td>{{ group.group_id }}</td>
            <td>{{ group.分区 }}</td>
            <td>{{ group.起始分片 }} → {{ group.结束分片 }}</td>
            <td>{{ group.attempts }}</td>
            <td>{{ group.last_error }}</td>
            <td><button class="link" type="button" @click="retryGroup(detail.task_id, group.group_id)">从原边界重试</button></td>
          </tr>
          <tr v-if="!detail.失败组.length"><td colspan="6" class="empty-state">没有失败分组</td></tr>
        </tbody>
      </table>

      <h4 class="sub-title">剩余组（{{ detail.剩余组.length }}）</h4>
      <table class="data-table">
        <thead>
          <tr><th>分组</th><th>分区</th><th>原分片边界</th><th>分片数</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="group in detail.剩余组.slice(0, 12)" :key="group.group_id">
            <td>{{ group.group_id }}</td>
            <td>{{ group.分区 }}</td>
            <td>{{ group.起始分片 }} → {{ group.结束分片 }}</td>
            <td>{{ group.shard_count }}</td>
            <td>{{ group.status }}</td>
          </tr>
          <tr v-if="!detail.剩余组.length"><td colspan="5" class="empty-state">没有剩余分组，全部完成</td></tr>
        </tbody>
      </table>

      <h4 class="sub-title">空组 / 无需合并说明（{{ detail.空组说明.length }}）</h4>
      <ul class="reason-list">
        <li v-for="item in detail.空组说明" :key="item.group_id">
          <strong>{{ item.group_id }}</strong> · {{ item.分区 }}：{{ item.reason }}
        </li>
        <li v-if="!detail.空组说明.length" class="empty-state">没有空组或无需合并的分组</li>
      </ul>

      <template v-if="detail.过期完成项.length">
        <h4 class="sub-title">过期完成项（{{ detail.过期完成项.length }}，不计入复核清单）</h4>
        <ul class="reason-list">
          <li v-for="item in detail.过期完成项" :key="item.group_id">
            <strong>{{ item.group_id }}</strong> · {{ item.分区 }} {{ item.起始分片 }}→{{ item.结束分片 }}：{{ item.原因 }}
          </li>
        </ul>
      </template>

      <h4 class="sub-title">收益复核清单</h4>
      <p v-if="detail.复核清单" class="hint">
        批次 {{ detail.复核清单.batch }} · 文件数量下降 {{ detail.复核清单.文件数量下降 }} ·
        容量下降 {{ detail.复核清单.容量下降MB }}MB · 记账条目 {{ detail.复核清单.记账条目 }} ·
        {{ detail.复核清单.说明 }}
      </p>
      <p v-else class="hint">该任务还没有收益复核记录。</p>

      <form class="filter-bar" @submit.prevent="expandScope(detail.task_id)">
        <label class="filter-item">
          <span>范围扩展方式</span>
          <select v-model="scopeForm.mode">
            <option value="append">append（保留原边界，只排新增文件）</option>
            <option value="replan">replan（按当前范围重排）</option>
          </select>
        </label>
        <label class="filter-item">
          <span>新增分区</span>
          <input v-model="scopeForm.partitions" placeholder="如 dt=29,dt=30" />
        </label>
        <label class="filter-item">
          <span>新增分片数</span>
          <input v-model="scopeForm.shards" type="number" min="0" />
        </label>
        <button class="btn" type="submit">扩展文件范围</button>
      </form>
    </section>

    <!-- 合并任务列表 -->
    <template v-if="!detail && activeTab === 'tasks'">
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
              <button class="link" type="button" @click="openDetail(Number(row.id))">收益详情</button>
            </td>
          </tr>
          <tr v-if="!rows.length">
            <td :colspan="columns.length + 1" class="empty-state">暂无小文件合并数据，可先登记合并任务</td>
          </tr>
        </tbody>
      </table>
    </template>

    <!-- 合并结果数据集 -->
    <template v-if="!detail && activeTab === 'datasets'">
      <div class="panel-actions">
        <button class="btn" type="button" @click="generateDatasets">读取合并任务并生成结果数据集</button>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>结果数据集</th><th>任务编号</th><th>目标表名</th><th>范围版本</th><th>状态</th>
            <th>分片(P50/P95)</th><th>分组</th><th>已完成组</th><th>文件数量下降</th>
            <th>容量下降(MB)</th><th>剩余分片</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in datasets" :key="row.dataset_id">
            <td>{{ row.dataset_id }}</td>
            <td>{{ row.任务编号 }}</td>
            <td>{{ row.目标表名 }}</td>
            <td>{{ row.范围版本 }}</td>
            <td>{{ row.状态 }}</td>
            <td>{{ row.分片统计.P50MB }} / {{ row.分片统计.P95MB }}MB</td>
            <td>{{ row.分组总数 }}</td>
            <td>{{ row.已完成分组 }}</td>
            <td>{{ row.文件数量下降 }}</td>
            <td>{{ row.容量下降MB }}</td>
            <td>{{ row.剩余分片 }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="openDetail(row.task_id)">收益详情</button>
              <button class="link" type="button" @click="runDataset(row.task_id)">执行合并</button>
              <button class="link" type="button" @click="resumeDataset(row.task_id)">续跑</button>
            </td>
          </tr>
          <tr v-if="!datasets.length"><td colspan="12" class="empty-state">暂无合并结果数据集</td></tr>
        </tbody>
      </table>
    </template>

    <!-- 收益复核清单 -->
    <template v-if="!detail && activeTab === 'reviews'">
      <table class="data-table">
        <thead>
          <tr>
            <th>任务编号</th><th>结果数据集</th><th>范围版本</th><th>批次</th><th>文件数量下降</th>
            <th>容量下降(MB)</th><th>成功分组</th><th>记账条目</th><th>状态</th><th>说明</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in reviews" :key="row.id">
            <td>{{ row.任务编号 }}</td>
            <td>{{ row.dataset_id }}</td>
            <td>{{ row.范围版本 }}</td>
            <td>{{ row.batch }}</td>
            <td>{{ row.文件数量下降 }}</td>
            <td>{{ row.容量下降MB }}</td>
            <td>{{ row.成功分组 }}</td>
            <td>{{ row.记账条目 }}</td>
            <td>{{ row.核对结果 ?? row.状态 }}</td>
            <td>{{ row.说明 }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="reconcile(row.task_id)">复核</button>
            </td>
          </tr>
          <tr v-if="!reviews.length"><td colspan="11" class="empty-state">暂无收益复核记录</td></tr>
        </tbody>
      </table>
    </template>

    <footer class="page-foot">
      <span>{{ footText }}</span>
      <span v-if="notice" class="ok-text">{{ notice }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type Group = {
  group_id: string
  分区: string
  起始分片: string
  结束分片: string
  shard_count: number
  file_reduction: number
  capacity_reduction: number
  status: string
  attempts: number
  reason: string
  last_error: string
  accounted: boolean
}
type DatasetSummary = {
  dataset_id: string
  task_id: number
  任务编号: string
  目标表名: string
  范围版本: string
  状态: string
  分片统计: Record<string, number>
  分组总数: number
  已完成分组: number
  失败分组: number
  未完成分组: number
  空分组: number
  文件数量下降: number
  容量下降MB: number
  剩余分片: number
  已处理分片: number
  账本条目: number
}
type DatasetDetail = DatasetSummary & {
  batch: number
  groups: Group[]
  已完成组: Group[]
  失败组: Group[]
  剩余组: Group[]
  空组说明: { group_id: string; 分区: string; reason: string }[]
  过期完成项: { group_id: string; 分区: string; 起始分片: string; 结束分片: string; 原因: string }[]
  复核清单: Record<string, string | number> | null
}
type Review = {
  id: number
  task_id: number
  任务编号: string
  dataset_id: string
  范围版本: string
  batch: number
  文件数量下降: number
  容量下降MB: number
  成功分组: number
  记账条目: number
  状态: string
  说明: string
  核对结果?: string
}

const ENDPOINT = '/api/compact'
const columns = ["任务编号", "目标表名", "文件范围", "合并策略", "目标大小", "并行度", "执行耗时", "任务状态"]
const actions = ["执行合并", "回滚合并", "归档任务"]
const filterFields = columns.slice(0, 3)
const tabs = [
  { key: 'tasks', label: '合并任务列表' },
  { key: 'datasets', label: '合并任务结果数据集' },
  { key: 'reviews', label: '收益复核清单' },
]

const rows = ref<Row[]>([])
const total = ref(0)
const datasets = ref<DatasetSummary[]>([])
const reviews = ref<Review[]>([])
const detail = ref<DatasetDetail | null>(null)
const activeTab = ref('tasks')
const errorMessage = ref('')
const notice = ref('')
const filters = ref<Record<string, string>>({})
const scopeForm = ref({ mode: 'append', partitions: '', shards: '0' })

const stats = computed(() => {
  const items = datasets.value
  return [
    { label: '结果数据集', value: items.length },
    { label: '已完成任务', value: items.filter((item) => item.状态 === '已完成').length },
    { label: '未完成分片', value: items.reduce((sum, item) => sum + Number(item.剩余分片 || 0), 0) },
    { label: '文件数量下降', value: items.reduce((sum, item) => sum + Number(item.文件数量下降 || 0), 0) },
    { label: '容量下降(MB)', value: items.reduce((sum, item) => sum + Number(item.容量下降MB || 0), 0) },
  ]
})

const footText = computed(() => {
  if (detail.value) return `当前查看 ${detail.value.任务编号} 的收益详情`
  if (activeTab.value === 'datasets') return `共 ${datasets.value.length} 个合并结果数据集`
  if (activeTab.value === 'reviews') return `共 ${reviews.value.length} 条收益复核记录`
  return `共 ${total.value} 条小文件合并记录`
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

async function openCreate() {
  errorMessage.value = ''
  notice.value = ''
  try {
    const stamp = Date.now().toString().slice(-6)
    const payload = await post('', {
      values: {
        任务编号: `COMP-${stamp}`,
        目标表名: `iceberg.ods.new_${stamp}`,
        文件范围: 'dt>=2026-11-01',
        分片数量: 64,
        目标大小: '64MB',
      },
    })
    notice.value = payload.message
    await reload()
    await loadDatasets()
    await loadReviews()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并任务登记失败'
  }
}

async function switchTab(key: string) {
  activeTab.value = key
  detail.value = null
  if (key === 'datasets') await loadDatasets()
  if (key === 'reviews') await loadReviews()
}

async function openDetail(taskId: number) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/datasets/${taskId}`)
    if (!response.ok) throw new Error('合并结果数据集读取失败')
    detail.value = (await response.json()) as DatasetDetail
    await loadReviews()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '收益详情读取失败'
  }
}

function closeDetail() {
  detail.value = null
  void loadDatasets()
  void loadReviews()
  void reload()
}

async function post(path: string, body: unknown) {
  const response = await request(`${ENDPOINT}${path}`, {
    method: 'POST',
    body: JSON.stringify(body ?? {}),
  })
  const payload = await response.json().catch(() => ({}))
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.message || payload.detail || '操作未生效，请稍后重试')
  }
  return payload
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) throw new Error('小文件合并动作未生效，请稍后重试')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '小文件合并操作失败'
  }
}

async function runDataset(taskId: number) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const payload = await post(`/datasets/${taskId}/run`, { values: {} })
    notice.value = payload.message
    await refreshAfterMutation(taskId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '执行合并失败'
  }
}

async function retryGroup(taskId: number, groupId: string) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const payload = await post(`/datasets/${taskId}/run`, { values: { group_ids: [groupId] } })
    notice.value = payload.message
    await refreshAfterMutation(taskId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重试失败'
  }
}

async function resumeDataset(taskId: number) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const payload = await post(`/datasets/${taskId}/resume`, { values: {} })
    notice.value = payload.message
    await refreshAfterMutation(taskId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '续跑失败'
  }
}

async function interruptDataset(taskId: number) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const payload = await post(`/datasets/${taskId}/interrupt`, {})
    notice.value = payload.message
    await refreshAfterMutation(taskId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '中断失败'
  }
}

async function reconcile(taskId: number) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/reviews/${taskId}/reconcile`, {
      method: 'POST',
      body: JSON.stringify({}),
    })
    const payload = await response.json().catch(() => ({}))
    if (!response.ok) throw new Error(payload.detail || '复核失败')
    notice.value = `复核${payload.核对结果 ?? '完成'}，复核清单已同步批次 ${payload.复核清单?.batch ?? '—'}`
    await loadReviews()
    if (detail.value) await openDetail(taskId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复核失败'
  }
}

async function expandScope(taskId: number) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const partitions = scopeForm.value.partitions
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
    const payload = await post(`/datasets/${taskId}/scope`, {
      values: {
        mode: scopeForm.value.mode,
        add_partitions: partitions,
        add_shards: Number(scopeForm.value.shards || 0),
      },
    })
    notice.value = payload.message
    await refreshAfterMutation(taskId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '范围扩展失败'
  }
}

async function refreshAfterMutation(taskId: number) {
  await loadDatasets()
  await loadReviews()
  if (detail.value && detail.value.task_id === taskId) await openDetail(taskId)
}

async function generateDatasets() {
  errorMessage.value = ''
  notice.value = ''
  try {
    const payload = await post('/datasets/generate', {})
    notice.value = payload.message
    await loadDatasets()
    await loadReviews()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成结果数据集失败'
  }
}

async function loadDatasets() {
  try {
    const response = await request(`${ENDPOINT}/datasets`)
    if (!response.ok) throw new Error('合并结果数据集读取失败')
    const payload = await response.json()
    datasets.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并结果数据集读取失败'
  }
}

async function loadReviews() {
  try {
    const response = await request(`${ENDPOINT}/reviews`)
    if (!response.ok) throw new Error('收益复核清单读取失败')
    const payload = await response.json()
    reviews.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '收益复核清单读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) throw new Error('合并任务列表读取失败')
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '小文件合并列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadDatasets()
  void loadReviews()
})
</script>

<style scoped>
.tabs { display: flex; gap: 6px; margin: 0 0 12px; border-bottom: 1px solid var(--border); }
.tab { border: 1px solid transparent; border-bottom: none; background: transparent; padding: 8px 14px; cursor: pointer; color: var(--muted); }
.tab.active { background: #fff; border-color: var(--border); color: #1f2937; border-radius: 6px 6px 0 0; }
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; margin-bottom: 14px; }
.panel-actions { display: flex; justify-content: flex-end; margin-bottom: 8px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0 0 4px; }
.panel-desc { color: var(--muted); font-size: 13px; margin: 0 0 10px; }
.sub-title { margin: 16px 0 6px; font-size: 14px; }
.hint { color: var(--muted); font-size: 13px; margin: 6px 0; }
.reason-list { margin: 0; padding-left: 18px; color: var(--muted); font-size: 13px; }
.reason-list li { margin: 4px 0; }
.ok-text { color: #067647; }
select, input { border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; background: #fff; }
</style>
