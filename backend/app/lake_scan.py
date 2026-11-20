"""数据湖跨分区读取层。

背景：落湖目录按 ``events/<事件类型>/<年/月/日>`` 组织，单条事件文件
（dict）与批次数组文件（list）混存在同一分区里。按天逐个目录读取时，
跨日分页边界无法对齐事件顺序，也难以核对一条记录到底来自哪个文件。

本模块把“扫描目录 → 展开单条/批次文件 → 稳定排序 → 分页游标”固化为
数据落湖查询与事件归档清理共用的取数口径：

- 每次查询首屏生成一份只读 *快照（snapshot）*，后续翻页只交付快照内的
  记录，因此并发落湖产生的新文件不会重复或污染后续分页；
- 每条记录携带分区来源（分区路径、文件、文件内序号）与读取边界
  （snapshot_id、分区版本、跨分区序号），归档候选可据此回查查询记录；
- 分区版本由分区内文件清单（文件名+大小+mtime_ns）指纹得到，历史分区
  只要文件未变，版本保持稳定，可随时回查。
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable


EVENTS_ROOT = "events"
# 快照注册表在 JsonStore 中的分区键，与业务模块键隔离。
SNAPSHOT_KEY = "_lake_query_snapshots"
# 归档执行历史在 JsonStore 中的分区键。
ARCHIVE_RUN_KEY = "_lake_archive_runs"
MAX_SNAPSHOT_KEPT = 50


# ──────────────────────────────────────────────────────────
# 路径与分区工具
# ──────────────────────────────────────────────────────────
def default_lake_base() -> str:
    """数据湖默认根目录：backend/data_store/lake。"""
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "data_store", "lake")


def partition_of(event_type: str, day: date) -> str:
    """事件类型 + 日期 → 落湖分区相对路径，如 ``events/order/2026/11/18``。"""
    return "/".join([EVENTS_ROOT, event_type, day.strftime("%Y/%m/%d")])


def parse_partition(partition: str) -> tuple[str, date] | None:
    """解析分区相对路径，返回 (事件类型, 日期)；非落湖分区返回 None。"""
    parts = [p for p in str(partition).replace("\\", "/").split("/") if p]
    if len(parts) != 5 or parts[0] != EVENTS_ROOT:
        return None
    try:
        day = datetime.strptime("/".join(parts[2:]), "%Y/%m/%d").date()
    except ValueError:
        return None
    return parts[1], day


def daterange(start: date, end: date) -> list[date]:
    """闭区间日期序列，按时间正序。"""
    if end < start:
        start, end = end, start
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def list_partition_days(lake_base: str, event_type: str) -> list[date]:
    """枚举某事件类型目录下实际存在的历史分区日期（含历史分区，按日正序）。"""
    type_root = os.path.join(lake_base, EVENTS_ROOT, event_type)
    days: list[date] = []
    if not os.path.isdir(type_root):
        return days
    for year in sorted(os.listdir(type_root)):
        y_dir = os.path.join(type_root, year)
        if not os.path.isdir(y_dir) or not year.isdigit():
            continue
        for month in sorted(os.listdir(y_dir)):
            m_dir = os.path.join(y_dir, month)
            if not os.path.isdir(m_dir) or not month.isdigit():
                continue
            for day_name in sorted(os.listdir(m_dir)):
                d_dir = os.path.join(m_dir, day_name)
                if not os.path.isdir(d_dir) or not day_name.isdigit():
                    continue
                try:
                    days.append(date(int(year), int(month), int(day_name)))
                except ValueError:
                    continue
    return sorted(days)


# ──────────────────────────────────────────────────────────
# 分区版本：文件清单指纹，历史分区可回查
# ──────────────────────────────────────────────────────────
def partition_files(lake_base: str, partition: str) -> list[dict[str, Any]]:
    """列出分区内全部 JSON 数据文件及其指纹要素（不含临时文件）。"""
    part_dir = os.path.join(lake_base, *partition.split("/"))
    if not os.path.isdir(part_dir):
        return []
    files: list[dict[str, Any]] = []
    for name in sorted(os.listdir(part_dir)):
        if not name.endswith(".json") or name.endswith(".tmp"):
            continue
        full = os.path.join(part_dir, name)
        if not os.path.isfile(full):
            continue
        try:
            stat = os.stat(full)
        except OSError:
            continue
        files.append({
            "file": name,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        })
    return files


def fingerprint_partition(files: list[dict[str, Any]]) -> str:
    """根据文件清单计算分区版本指纹。"""
    digest = hashlib.sha256()
    for item in files:
        digest.update(f"{item['file']}:{item['size']}:{item['mtime_ns']}\n".encode("utf-8"))
    return digest.hexdigest()[:16]


def read_partition(lake_base: str, partition: str) -> tuple[list[dict[str, Any]], str]:
    """读取一个分区，展开单条文件与批次数组文件为统一事件序列。

    返回 (事件信封列表, 分区版本)。信封在原始事件之外补充来源坐标，
    不修改落湖文件本身的内容。
    """
    files = partition_files(lake_base, partition)
    version = fingerprint_partition(files)
    envelopes: list[dict[str, Any]] = []
    part_dir = os.path.join(lake_base, *partition.split("/"))

    for item in files:
        file_name = item["file"]
        full_path = os.path.join(part_dir, file_name)
        try:
            with open(full_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError):
            # 写入中断或损坏文件：跳过但保留在版本指纹中，修复后版本会变化。
            continue

        if isinstance(data, list):
            for index, event in enumerate(data):
                if isinstance(event, dict):
                    envelopes.append(_envelope(partition, file_name, index, event, version))
        elif isinstance(data, dict):
            envelopes.append(_envelope(partition, file_name, None, data, version))
    return envelopes, version


def _envelope(partition: str, file_name: str, file_index: int | None,
              event: dict[str, Any], version: str) -> dict[str, Any]:
    """为一条落湖事件附加分区来源与读取坐标。"""
    envelope = dict(event)
    envelope["_lake"] = {
        "partition": partition,
        "source_file": file_name,
        "file_layout": "batch_array" if file_index is not None else "single_event",
        "file_index": file_index,
        "partition_version": version,
        "event_id": str(event.get("event_id") or ""),
        "ingested_at": event.get("ingested_at"),
    }
    return envelope


# ──────────────────────────────────────────────────────────
# 筛选口径：查询与归档共用
# ──────────────────────────────────────────────────────────
@dataclass
class LakeFilter:
    """跨分区查询 / 归档候选共用的筛选口径。"""

    event_type: str | None = None
    start_day: date | None = None
    end_day: date | None = None
    event_ids: set[str] | None = None
    source_channel_id: int | None = None
    keyword: str | None = None
    keyword_fields: tuple[str, ...] = ("event_id", "event_type")
    extra: dict[str, Any] = field(default_factory=dict)

    def matches(self, envelope: dict[str, Any]) -> bool:
        lake = envelope.get("_lake", {})
        parsed = parse_partition(lake.get("partition", ""))
        if self.event_type and parsed and parsed[0] != self.event_type:
            return False
        if parsed and (self.start_day or self.end_day):
            day = parsed[1]
            if self.start_day and day < self.start_day:
                return False
            if self.end_day and day > self.end_day:
                return False
        if self.event_ids is not None and lake.get("event_id") not in self.event_ids:
            return False
        if self.source_channel_id is not None:
            try:
                if int(envelope.get("source_channel_id", 0)) != int(self.source_channel_id):
                    return False
            except (TypeError, ValueError):
                return False
        if self.keyword:
            kw = self.keyword.casefold()
            haystacks = [str(envelope.get(name, "")) for name in self.keyword_fields]
            payload = envelope.get("payload")
            if isinstance(payload, dict):
                haystacks.extend(str(v) for v in payload.values())
            if not any(kw in text.casefold() for text in haystacks):
                return False
        for field_name, expected in self.extra.items():
            if expected is None or str(expected).strip() == "":
                continue
            if str(expected).casefold() not in str(envelope.get(field_name, "")).casefold():
                return False
        return True


def make_filter(**kwargs: Any) -> LakeFilter:
    """构造筛选口径，接受 ``YYYY/MM/DD`` 或 ``YYYY-MM-DD`` 字符串日期。"""
    parsed: dict[str, Any] = {}
    for key in ("event_type", "event_ids", "source_channel_id",
                "keyword", "keyword_fields", "extra"):
        if key in kwargs:
            parsed[key] = kwargs[key]
    for key in ("start_day", "end_day"):
        value = kwargs.get(key)
        if isinstance(value, str):
            value = _parse_day(value)
        if value is not None:
            parsed[key] = value
    return LakeFilter(**parsed)


def _parse_day(value: str) -> date | None:
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


# ──────────────────────────────────────────────────────────
# 排序与游标
# ──────────────────────────────────────────────────────────
def _sort_key(envelope: dict[str, Any]) -> tuple[str, str, str, int]:
    """跨分区事件顺序：分区日 → 文件 → 批次数组内序号。

    单条文件与批次数组在同一排序空间内比较，分页边界因此可以跨越两者。
    """
    lake = envelope.get("_lake", {})
    return (
        str(lake.get("partition", "")),
        str(lake.get("source_file", "")),
        str(lake.get("event_id") or ""),
        lake.get("file_index") if lake.get("file_index") is not None else -1,
    )


def encode_cursor(snapshot_id: str, offset: int) -> str:
    """生成不透明分页游标（base64，客户端只回传不解析）。"""
    raw = f"{snapshot_id}|{offset}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> tuple[str, int]:
    """解析分页游标；非法游标抛 ValueError。"""
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(cursor + padding).decode("utf-8")
        snapshot_id, offset_text = raw.rsplit("|", 1)
        offset = int(offset_text)
        if offset < 0:
            raise ValueError
        return snapshot_id, offset
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"非法分页游标: {cursor}") from exc


# ──────────────────────────────────────────────────────────
# 跨分区读取器
# ──────────────────────────────────────────────────────────
class LakeReader:
    """跨分区快照读取器：扫描、快照化、按游标分页。"""

    def __init__(self, lake_base: str | None = None, store: Any | None = None) -> None:
        self._lake_base = lake_base or default_lake_base()
        self._store = store
        self._lock = threading.Lock()

    # 目录扫描 --------------------------------------------------
    def scan_partitions(self, lake_filter: LakeFilter,
                        partitions: Iterable[str] | None = None) -> list[dict[str, Any]]:
        """按筛选口径扫描分区，返回按事件顺序排好的事件信封列表。"""
        target_partitions = list(partitions) if partitions is not None \
            else self._resolve_partitions(lake_filter)
        envelopes: list[dict[str, Any]] = []
        for partition in target_partitions:
            rows, _version = read_partition(self._lake_base, partition)
            envelopes.extend(rows)
        envelopes = [row for row in envelopes if lake_filter.matches(row)]
        envelopes.sort(key=_sort_key)
        return envelopes

    def _resolve_partitions(self, lake_filter: LakeFilter) -> list[str]:
        if lake_filter.event_type is None:
            event_types = self._list_event_types()
        else:
            event_types = [lake_filter.event_type]

        partitions: list[str] = []
        for event_type in event_types:
            if lake_filter.start_day and lake_filter.end_day:
                days = daterange(lake_filter.start_day, lake_filter.end_day)
            else:
                days = list_partition_days(self._lake_base, event_type)
                if lake_filter.start_day:
                    days = [d for d in days if d >= lake_filter.start_day]
                if lake_filter.end_day:
                    days = [d for d in days if d <= lake_filter.end_day]
            partitions.extend(partition_of(event_type, day) for day in days)
        return sorted(set(partitions))

    def _list_event_types(self) -> list[str]:
        root = os.path.join(self._lake_base, EVENTS_ROOT)
        if not os.path.isdir(root):
            return []
        return sorted(
            name for name in os.listdir(root)
            if os.path.isdir(os.path.join(root, name))
        )

    # 分区版本 --------------------------------------------------
    def partition_versions(self, partitions: Iterable[str] | None = None) -> list[dict[str, Any]]:
        """返回分区版本清单，兼容历史分区（按现存文件清单计算指纹）。"""
        target = list(partitions) if partitions is not None else self._all_partitions()
        result = []
        for partition in target:
            files = partition_files(self._lake_base, partition)
            result.append({
                "partition": partition,
                "version": fingerprint_partition(files),
                "file_count": len(files),
                "record_files": [item["file"] for item in files],
            })
        return sorted(result, key=lambda item: item["partition"])

    def _all_partitions(self) -> list[str]:
        partitions: list[str] = []
        for event_type in self._list_event_types():
            partitions.extend(
                partition_of(event_type, day)
                for day in list_partition_days(self._lake_base, event_type)
            )
        return sorted(partitions)

    # 快照与分页 ------------------------------------------------
    def open_snapshot(self, lake_filter: LakeFilter,
                      partitions: Iterable[str] | None = None,
                      query_scope: str | None = None) -> dict[str, Any]:
        """首屏查询：固化快照并返回第一页。

        快照包含命中事件的来源坐标与各分区版本，持久化到 JsonStore，
        之后的翻页请求始终读取这份冻结清单。
        """
        with self._lock:
            envelopes = self.scan_partitions(lake_filter, partitions)
            versions = {
                item["partition"]: item
                for item in self.partition_versions(
                    sorted({row["_lake"]["partition"] for row in envelopes})
                )
            }
            now = datetime.now(timezone.utc)
            snapshot_id = "snap_" + now.strftime("%Y%m%d%H%M%S%f") + "_" + \
                hashlib.sha1(os.urandom(8)).hexdigest()[:8]

            records = [self._index_record(snapshot_id, idx, row)
                       for idx, row in enumerate(envelopes)]
            snapshot = {
                "snapshot_id": snapshot_id,
                "created_at": now.isoformat(),
                "scope": query_scope or self._describe_scope(lake_filter),
                "filter": self._filter_payload(lake_filter),
                "total": len(records),
                "partitions": versions,
                "records": records,
            }
            self._persist_snapshot(snapshot)
            return snapshot

    def page_snapshot(self, snapshot_id: str, offset: int, size: int) -> dict[str, Any]:
        """翻页：从已冻结的快照按偏移取一页，不重新扫描目录。"""
        snapshot = self.get_snapshot(snapshot_id)
        if snapshot is None:
            raise KeyError(f"查询快照 {snapshot_id} 不存在或已过期")
        records = snapshot["records"]
        offset = max(0, offset)
        page_rows = records[offset:offset + size]
        return {
            "snapshot_id": snapshot_id,
            "offset": offset,
            "size": size,
            "total": len(records),
            "has_more": offset + len(page_rows) < len(records),
            "next_cursor": encode_cursor(snapshot_id, offset + len(page_rows))
            if offset + len(page_rows) < len(records) else None,
            "items": page_rows,
            "partitions": snapshot.get("partitions", {}),
            "scope": snapshot.get("scope"),
            "created_at": snapshot.get("created_at"),
        }

    def first_page(self, lake_filter: LakeFilter, size: int = 20,
                   partitions: Iterable[str] | None = None,
                   query_scope: str | None = None) -> dict[str, Any]:
        """打开快照并直接返回第一页数据。"""
        snapshot = self.open_snapshot(lake_filter, partitions, query_scope)
        page = self.page_snapshot(snapshot["snapshot_id"], 0, size)
        page["filter"] = snapshot["filter"]
        return page

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        if self._store is None:
            return None
        for row in self._store.get_all(SNAPSHOT_KEY):
            if row.get("snapshot_id") == snapshot_id:
                return row.get("snapshot")
        return None

    def _persist_snapshot(self, snapshot: dict[str, Any]) -> None:
        if self._store is None:
            return
        existing = self._store.get_all(SNAPSHOT_KEY)
        record = {
            "snapshot_id": snapshot["snapshot_id"],
            "created_at": snapshot["created_at"],
            "scope": snapshot["scope"],
            "snapshot": snapshot,
        }
        self._store.insert(SNAPSHOT_KEY, record)
        # 控制注册表体积，只保留最近 MAX_SNAPSHOT_KEPT 份。
        stale = existing[:-MAX_SNAPSHOT_KEPT + 1] if len(existing) >= MAX_SNAPSHOT_KEPT else []
        for row in stale:
            self._store.delete(SNAPSHOT_KEY, int(row["id"]))

    def _index_record(self, snapshot_id: str, seq: int,
                      envelope: dict[str, Any]) -> dict[str, Any]:
        """给快照内事件写入跨分区序号，补齐读取边界。"""
        lake = envelope["_lake"]
        lake["snapshot_id"] = snapshot_id
        lake["sequence"] = seq
        lake["read_boundary"] = {
            "snapshot_id": snapshot_id,
            "sequence": seq,
            "partition": lake["partition"],
            "partition_version": lake["partition_version"],
            "source_file": lake["source_file"],
            "file_index": lake["file_index"],
        }
        return envelope

    @staticmethod
    def _describe_scope(lake_filter: LakeFilter) -> str:
        start = lake_filter.start_day.isoformat() if lake_filter.start_day else "*"
        end = lake_filter.end_day.isoformat() if lake_filter.end_day else "*"
        return f"{lake_filter.event_type or 'ALL'}[{start}~{end}]"

    @staticmethod
    def _filter_payload(lake_filter: LakeFilter) -> dict[str, Any]:
        return {
            "event_type": lake_filter.event_type,
            "start_day": lake_filter.start_day.isoformat() if lake_filter.start_day else None,
            "end_day": lake_filter.end_day.isoformat() if lake_filter.end_day else None,
            "event_ids": sorted(lake_filter.event_ids) if lake_filter.event_ids is not None else None,
            "source_channel_id": lake_filter.source_channel_id,
            "keyword": lake_filter.keyword,
            "extra": lake_filter.extra,
        }


def iter_snapshot_pages(reader: LakeReader, snapshot_id: str,
                        size: int) -> Iterable[dict[str, Any]]:
    """顺序迭代快照全部分页（归档候选按页读取有效记录时使用）。"""
    offset = 0
    while True:
        page = reader.page_snapshot(snapshot_id, offset, size)
        if not page["items"]:
            break
        yield page
        if not page["has_more"]:
            break
        offset += len(page["items"])


def select_archive_candidates(records: Iterable[dict[str, Any]],
                              predicate: Callable[[dict[str, Any]], bool] | None = None
                              ) -> list[dict[str, Any]]:
    """从跨分区查询数据页的有效记录中筛选归档候选。

    只接受带 ``_lake`` 来源坐标的信封记录，保证每个候选都能回查到
    对应的查询记录与读取边界。
    """
    candidates: list[dict[str, Any]] = []
    for record in records:
        lake = record.get("_lake")
        if not isinstance(lake, dict):
            continue
        if predicate is not None and not predicate(record):
            continue
        candidates.append({
            "event_id": lake.get("event_id"),
            "partition": lake.get("partition"),
            "partition_version": lake.get("partition_version"),
            "source_file": lake.get("source_file"),
            "file_layout": lake.get("file_layout"),
            "file_index": lake.get("file_index"),
            "sequence": lake.get("sequence"),
            "snapshot_id": lake.get("snapshot_id"),
            "read_boundary": lake.get("read_boundary"),
            "ingested_at": lake.get("ingested_at") or record.get("ingested_at"),
        })
    return candidates
