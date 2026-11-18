<template>
  <section class="page" data-module="stats">
    <header class="page-head">
      <div>
        <h2>表统计信息管理</h2>
        <p class="page-desc">维护表统计的行数、文件数与容量分析，为表统计信息提供可审计的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记统计报告</button>
        <button class="btn" type="button" @click="exportRows">导出表统计信息清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section class="panel" data-panel="compact-report">
      <header class="panel-head">
        <div>
          <h3>合并收益统计报告</h3>
          <p class="panel-desc">
            接入 {{ report?.兼容目标?.dataset_version ?? 'compact_dataset/v1' }} 结果数据集，
            按成功分组汇总文件数量下降与容量下降；单个分组只累计一次。
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" type="button" @click="loadReport">刷新统计报告</button>
        </div>
      </header>
      <p v-if="report" class="hint">
        口径：{{ report.口径 }} · 任务数 {{ report.task_count ?? report.tasks?.length ?? 0 }}
        <template v-if="report.重试任务 && report.重试任务.length"> · 本次重试未完成项：{{ report.重试任务.join('、') }}</template>
        <template v-if="report.本次错误 && report.本次错误.length"> · 读取问题：{{ report.本次错误.join('；') }}</template>
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>任务编号</th><th>目标表名</th><th>结果数据集</th><th>范围版本</th><th>状态</th>
            <th>分片(P50/P95)</th><th>分组</th><th>成功分组</th><th>空分组</th>
            <th>文件数量下降</th><th>容量下降(MB)</th><th>复核批次</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="task in report?.tasks ?? []" :key="task.任务编号">
            <td>{{ task.任务编号 }}</td>
            <td>{{ task.目标表名 }}</td>
            <td>{{ task.dataset_id }}</td>
            <td>{{ task.范围版本 }}</td>
            <td>{{ task.状态 }}</td>
            <td>{{ task.分片统计?.P50MB }} / {{ task.分片统计?.P95MB }}MB</td>
            <td>{{ task.分组总数 }}</td>
            <td>{{ task.成功分组 }}</td>
            <td>{{ task.空分组 }}</td>
            <td>{{ task.文件数量下降 }}</td>
            <td>{{ task.容量下降MB }}</td>
            <td>{{ task.复核批次 ?? '—' }}</td>
          </tr>
          <tr v-if="!report?.tasks?.length">
            <td colspan="12" class="empty-state">暂无合并收益数据，可先到小文件合并执行合并</td>
          </tr>
        </tbody>
      </table>
      <p v-if="report" class="hint">
        合计：文件数量下降 {{ report.totals?.文件数量下降 ?? 0 }}，
        容量下降 {{ report.totals?.容量下降MB ?? 0 }}MB，
        成功分组 {{ report.totals?.成功分组 ?? 0 }} 个。
      </p>
    </section>

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
          <td :colspan="columns.length + 1" class="empty-state">暂无表统计信息数据，可先登记统计报告</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条表统计信息记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

type CompactReport = {
  口径: string
  dataset_version?: string
  兼容目标?: { dataset_version: string; required_fields: string[]; aggregate_by: string }
  task_count?: number
  tasks: {
    任务编号: string
    目标表名: string
    dataset_id: string
    范围版本: string
    状态: string
    分片统计?: Record<string, number>
    分组总数: number
    成功分组: number
    空分组: number
    文件数量下降: number
    容量下降MB: number
    复核批次?: number
  }[]
  totals?: Record<string, number>
  重试任务?: number[]
  本次错误?: string[]
}

const ENDPOINT = '/api/stats'
const columns = ["报告编号", "表名称", "行总数", "文件总数", "总容量", "列统计", "统计时间", "报告状态"]
const actions = ["采集统计", "标记过期", "清理报告"]
const statuses = ["草稿", "已采集", "已过期", "已清理"]
const stats = [{"label": "统计报告总数", "value": 0}, {"label": "已采集报告", "value": 0}, {"label": "已过期报告", "value": 0}]

const rows = ref<Row[]>([])
const total = ref(0)
const report = ref<CompactReport | null>(null)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

async function loadReport() {
  try {
    const response = await request(`${ENDPOINT}/compact-report`)
    if (!response.ok) throw new Error('合并收益统计报告读取失败')
    report.value = (await response.json()) as CompactReport
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并收益统计报告读取失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '统计报告登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('表统计信息动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '表统计信息操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('统计报告列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '表统计信息列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadReport()
})
</script>
