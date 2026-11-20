import { request } from './client'

/** 落湖事件上的分区来源与读取边界，与后端 app/lake_scan.py 信封结构对应。 */
export type LakeSource = {
  partition: string
  source_file: string
  file_layout: 'single_event' | 'batch_array'
  file_index: number | null
  partition_version: string
  event_id: string
  ingested_at: string | null
  snapshot_id: string
  sequence: number
  read_boundary: {
    snapshot_id: string
    sequence: number
    partition: string
    partition_version: string
    source_file: string
    file_index: number | null
  }
}

export type LakeEvent = {
  event_id: string
  event_type: string
  source_channel_id: number
  payload: Record<string, unknown>
  ingested_at: string
  _lake: LakeSource
  [field: string]: unknown
}

export type LakeQueryPage = {
  ok: boolean
  snapshot_id: string
  offset: number
  size: number
  total: number
  has_more: boolean
  next_cursor: string | null
  items: LakeEvent[]
  partitions: Record<string, { partition: string; version: string; file_count: number }>
  scope: string
  created_at: string
}

export type LakeQueryRequest = {
  event_type?: string | null
  start_day?: string | null
  end_day?: string | null
  size?: number
  cursor?: string | null
  keyword?: string | null
  source_channel_id?: number | null
}

export type ArchiveCandidate = {
  event_id: string
  partition: string
  partition_version: string
  source_file: string
  file_layout: string
  file_index: number | null
  sequence: number
  snapshot_id: string
  read_boundary: LakeSource['read_boundary']
  ingested_at: string | null
}

export type ArchiveCandidatePack = {
  snapshot_id: string
  scope: string
  filter: Record<string, unknown>
  total_records: number
  pages_read: number
  partitions: Record<string, unknown>
  candidates: ArchiveCandidate[]
}

export type PartitionVersion = {
  partition: string
  version: string
  file_count: number
  record_files: string[]
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

/** 跨分区落湖检索：首屏冻结快照，翻页携带游标，并发落湖不影响后续分页。 */
export function queryLakePages(payload: LakeQueryRequest): Promise<LakeQueryPage> {
  return postJson('/api/pipeline/lake/query', payload)
}

/** 回查一次查询快照的全量冻结记录与分区版本。 */
export function getLakeSnapshot(snapshotId: string): Promise<{ records: LakeEvent[] }> {
  return getJson(`/api/pipeline/lake/snapshots/${encodeURIComponent(snapshotId)}`)
}

/** 按 (快照, 跨分区序号) 定位归档候选对应的查询记录。 */
export function getLakeSnapshotRecord(snapshotId: string, sequence: number): Promise<LakeEvent> {
  return getJson(
    `/api/pipeline/lake/snapshots/${encodeURIComponent(snapshotId)}/records/${sequence}`,
  )
}

/** 分区版本清单：历史分区按文件清单指纹稳定回查。 */
export async function getPartitionVersions(
  eventType?: string,
): Promise<PartitionVersion[]> {
  const query = eventType ? `?event_type=${encodeURIComponent(eventType)}` : ''
  const data = await getJson<{ ok: boolean; partitions: PartitionVersion[] }>(
    `/api/pipeline/lake/partition-versions${query}`,
  )
  return data.partitions
}

/** 归档候选：与查询共用筛选口径，从跨分区查询数据页读取有效记录。 */
export function getArchiveCandidates(policyId: number, pageSize = 200): Promise<ArchiveCandidatePack> {
  return postJson('/api/pipeline/archive/candidates', { policy_id: policyId, page_size: pageSize })
}

/** 归档执行历史回查。 */
export async function getArchiveRuns(): Promise<Record<string, unknown>[]> {
  const data = await getJson<{ ok: boolean; runs: Record<string, unknown>[] }>(
    '/api/pipeline/archive/runs',
  )
  return data.runs
}
