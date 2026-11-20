<template>
  <section class="lake-panel" data-testid="archive-candidate-panel">
    <h3>归档候选（跨分区查询数据页）</h3>
    <p class="panel-desc">
      候选与落湖检索共用筛选口径，从查询快照数据页读取有效记录；点开候选可回查其对应的查询记录与读取边界。
    </p>

    <form class="filter-bar" @submit.prevent="loadCandidates">
      <label class="filter-item">
        <span>归档策略ID</span>
        <input v-model.number="policyId" data-testid="archive-policy-id" type="number" min="1" required />
      </label>
      <label class="filter-item">
        <span>数据页大小</span>
        <input v-model.number="pageSize" data-testid="archive-page-size" type="number" min="1" max="500" />
      </label>
      <button class="btn primary" data-testid="archive-load" type="button" @click="loadCandidates">
        生成归档候选
      </button>
      <button class="btn" type="button" @click="loadRuns">刷新归档历史</button>
    </form>

    <div v-if="pack" class="snapshot-meta">
      <span>查询快照：<code>{{ pack.snapshot_id }}</code></span>
      <span>范围：{{ pack.scope }}</span>
      <span>有效记录 {{ pack.total_records }} 条，读取数据页 {{ pack.pages_read }} 页</span>
      <span>候选 {{ pack.candidates.length }} 条</span>
    </div>

    <table v-if="pack" class="data-table">
      <thead>
        <tr>
          <th>快照序号</th>
          <th>事件ID</th>
          <th>分区来源</th>
          <th>来源文件 / 布局</th>
          <th>分区版本</th>
          <th>读取边界</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="candidate in pack.candidates" :key="`${candidate.snapshot_id}-${candidate.sequence}`">
          <td>{{ candidate.sequence }}</td>
          <td>{{ candidate.event_id }}</td>
          <td>{{ candidate.partition }}</td>
          <td>
            {{ candidate.source_file }}
            <small>({{ candidate.file_layout === 'batch_array' ? '批次数组' : '单条文件' }})</small>
          </td>
          <td><code>{{ candidate.partition_version.slice(0, 8) }}</code></td>
          <td><code>{{ candidate.snapshot_id.slice(0, 18) }}…#{{ candidate.sequence }}</code></td>
          <td>
            <button class="link" type="button" @click="openCandidate(candidate)">
              查找对应查询记录
            </button>
          </td>
        </tr>
        <tr v-if="!pack.candidates.length">
          <td colspan="7" class="empty-state">查询范围内暂无归档候选</td>
        </tr>
      </tbody>
    </table>

    <h4>归档执行历史（分区版本可回查）</h4>
    <table class="data-table">
      <thead>
        <tr>
          <th>执行记录</th>
          <th>策略ID</th>
          <th>快照</th>
          <th>候选/归档文件</th>
          <th>时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="run in runs" :key="String(run.run_id)">
          <td><code>{{ run.run_id }}</code></td>
          <td>{{ run.policy_id }}</td>
          <td><code>{{ String(run.snapshot_id).slice(0, 18) }}…</code></td>
          <td>{{ run.candidate_total }} / {{ run.archived }}</td>
          <td>{{ run.timestamp }}</td>
        </tr>
        <tr v-if="!runs.length">
          <td colspan="5" class="empty-state">暂无归档执行记录</td>
        </tr>
      </tbody>
    </table>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div v-if="traceRecord" class="modal-backdrop" data-testid="archive-trace-modal" @click.self="traceRecord = null">
      <div class="modal">
        <h3>候选对应的查询记录 #{{ traceRecord._lake.sequence }}</h3>
        <dl class="detail-list">
          <dt>事件ID</dt>
          <dd>{{ traceRecord.event_id }}</dd>
          <dt>分区来源</dt>
          <dd>{{ traceRecord._lake.partition }}</dd>
          <dt>来源文件</dt>
          <dd>{{ traceRecord._lake.source_file }}</dd>
          <dt>分区版本</dt>
          <dd><code>{{ traceRecord._lake.partition_version }}</code></dd>
          <dt>读取边界</dt>
          <dd><pre>{{ JSON.stringify(traceRecord._lake.read_boundary, null, 2) }}</pre></dd>
          <dt>事件载荷</dt>
          <dd><pre>{{ JSON.stringify(traceRecord.payload, null, 2) }}</pre></dd>
        </dl>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="traceRecord = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  getArchiveCandidates,
  getArchiveRuns,
  getLakeSnapshotRecord,
  type ArchiveCandidate,
  type ArchiveCandidatePack,
  type LakeEvent,
} from '@/api/lake-service'

const policyId = ref(1)
const pageSize = ref(200)
const pack = ref<ArchiveCandidatePack | null>(null)
const runs = ref<Record<string, unknown>[]>([])
const traceRecord = ref<LakeEvent | null>(null)
const errorMessage = ref('')

async function loadCandidates() {
  errorMessage.value = ''
  try {
    pack.value = await getArchiveCandidates(policyId.value, pageSize.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档候选生成失败'
  }
}

async function loadRuns() {
  errorMessage.value = ''
  try {
    runs.value = await getArchiveRuns()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档历史读取失败'
  }
}

async function openCandidate(candidate: ArchiveCandidate) {
  errorMessage.value = ''
  try {
    traceRecord.value = await getLakeSnapshotRecord(candidate.snapshot_id, candidate.sequence)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '查询记录回查失败'
  }
}

onMounted(loadRuns)
</script>

<style scoped>
.lake-panel {
  border: 1px solid rgb(15 23 42 / 12%);
  border-radius: 12px;
  padding: 16px;
  margin: 12px 0 20px;
  background: #f8fafc;
}
.panel-desc { color: var(--muted); font-size: 12px; margin: 4px 0 12px; }
.snapshot-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  font-size: 12px;
  color: var(--muted);
  margin-bottom: 10px;
}
.detail-list { display: grid; grid-template-columns: 90px 1fr; gap: 6px 12px; margin: 0; }
.detail-list dt { color: var(--muted); font-size: 12px; }
.detail-list dd { margin: 0; word-break: break-all; }
.detail-list pre {
  margin: 0;
  background: #f1f5f9;
  border-radius: 8px;
  padding: 8px;
  font-size: 12px;
  max-height: 220px;
  overflow: auto;
}
h4 { margin: 18px 0 8px; }
</style>
