<template>
  <section class="lake-panel" data-testid="lake-query-panel">
    <h3>跨分区落湖检索</h3>
    <p class="panel-desc">
      首屏按事件顺序冻结查询快照，翻页只交付快照内记录；并发落湖的新事件不会污染后续分页。
    </p>

    <form class="filter-bar" @submit.prevent="searchFirstPage">
      <label class="filter-item">
        <span>事件类型</span>
        <input v-model="filters.event_type" data-testid="lake-filter-type" placeholder="留空跨全部类型" />
      </label>
      <label class="filter-item">
        <span>起始日期</span>
        <input v-model="filters.start_day" data-testid="lake-filter-start" type="date" />
      </label>
      <label class="filter-item">
        <span>结束日期</span>
        <input v-model="filters.end_day" data-testid="lake-filter-end" type="date" />
      </label>
      <label class="filter-item">
        <span>关键词</span>
        <input v-model="filters.keyword" data-testid="lake-filter-keyword" placeholder="事件ID/载荷" />
      </label>
      <label class="filter-item">
        <span>每页条数</span>
        <input v-model.number="pageSize" data-testid="lake-filter-size" type="number" min="1" max="500" />
      </label>
      <button class="btn primary" data-testid="lake-search" type="submit">查询首屏</button>
    </form>

    <div v-if="page" class="snapshot-meta">
      <span>快照：<code>{{ page.snapshot_id }}</code></span>
      <span>范围：{{ page.scope }}</span>
      <span>命中 {{ page.total }} 条，已交付 {{ page.offset + page.items.length }} 条</span>
      <span>分区版本数：{{ Object.keys(page.partitions).length }}</span>
    </div>

    <table v-if="page" class="data-table">
      <thead>
        <tr>
          <th>序号</th>
          <th>事件ID</th>
          <th>事件类型</th>
          <th>分区来源</th>
          <th>来源文件 / 布局</th>
          <th>分区版本</th>
          <th>读取边界</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in page.items" :key="`${item._lake.snapshot_id}-${item._lake.sequence}`">
          <td>{{ item._lake.sequence }}</td>
          <td>{{ item.event_id }}</td>
          <td>{{ item.event_type }}</td>
          <td>{{ item._lake.partition }}</td>
          <td>
            {{ item._lake.source_file }}
            <small>({{ layoutLabel(item._lake.file_layout) }})</small>
          </td>
          <td><code>{{ item._lake.partition_version.slice(0, 8) }}</code></td>
          <td>
            <code>{{ item._lake.read_boundary.snapshot_id.slice(0, 18) }}…#{{ item._lake.sequence }}</code>
          </td>
          <td>
            <button class="link" type="button" @click="openRecord(item)">查看查询记录</button>
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="page" class="pager">
      <button class="btn" type="button" :disabled="cursorHistory.length <= 1" @click="goPrevPage">
        上一页
      </button>
      <button
        class="btn primary"
        type="button"
        :disabled="!page.next_cursor"
        @click="goNextPage"
      >
        下一页
      </button>
      <span v-if="!page.has_more" class="pager-end">已到快照末页</span>
    </div>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div v-if="detail" class="modal-backdrop" data-testid="lake-record-modal" @click.self="detail = null">
      <div class="modal">
        <h3>查询记录 #{{ detail._lake.sequence }}</h3>
        <dl class="detail-list">
          <dt>事件ID</dt>
          <dd>{{ detail.event_id }}</dd>
          <dt>分区来源</dt>
          <dd>{{ detail._lake.partition }}</dd>
          <dt>来源文件</dt>
          <dd>{{ detail._lake.source_file }}（{{ layoutLabel(detail._lake.file_layout) }}）</dd>
          <dt>分区版本</dt>
          <dd><code>{{ detail._lake.partition_version }}</code></dd>
          <dt>读取边界</dt>
          <dd><pre>{{ JSON.stringify(detail._lake.read_boundary, null, 2) }}</pre></dd>
          <dt>事件载荷</dt>
          <dd><pre>{{ JSON.stringify(detail.payload, null, 2) }}</pre></dd>
        </dl>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  queryLakePages,
  type LakeEvent,
  type LakeQueryPage,
  type LakeQueryRequest,
} from '@/api/lake-service'

const filters = ref<LakeQueryRequest>({ event_type: '', start_day: '', end_day: '', keyword: '' })
const pageSize = ref(20)
const page = ref<LakeQueryPage | null>(null)
const detail = ref<LakeEvent | null>(null)
const errorMessage = ref('')
/** 已访问分页的“起点游标”历史：首屏为 null，回退时弹出当前页再取栈顶。 */
const cursorHistory = ref<(string | null)[]>([])

function layoutLabel(layout: string): string {
  return layout === 'batch_array' ? '批次数组' : '单条文件'
}

async function runQuery(payload: LakeQueryRequest) {
  errorMessage.value = ''
  try {
    page.value = await queryLakePages(payload)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '落湖检索失败'
  }
}

function searchFirstPage() {
  cursorHistory.value = [null]
  void runQuery({
    event_type: filters.value.event_type || null,
    start_day: filters.value.start_day || null,
    end_day: filters.value.end_day || null,
    keyword: filters.value.keyword || null,
    size: pageSize.value,
  })
}

function goNextPage() {
  if (!page.value?.next_cursor) return
  cursorHistory.value.push(page.value.next_cursor)
  void runQuery({ cursor: page.value.next_cursor, size: pageSize.value })
}

function goPrevPage() {
  if (cursorHistory.value.length <= 1) return
  cursorHistory.value.pop()
  const target = cursorHistory.value[cursorHistory.value.length - 1]
  void runQuery({ cursor: target, size: pageSize.value })
}

function openRecord(item: LakeEvent) {
  detail.value = item
}
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
.pager { display: flex; gap: 10px; align-items: center; margin-top: 12px; }
.pager-end { color: var(--muted); font-size: 12px; }
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
</style>
