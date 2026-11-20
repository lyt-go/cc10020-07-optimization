"""数据落湖领域服务：数据写入本地存储、分区组织与生命周期管理。

将接入的数据按日期/来源/类型分区写入本地JSON文件，
支持多种写入格式和压缩策略。
跨模块关联：webhook_channel（事件来源）、batch_ingest（批量来源）、event_archive（归档接口）。

分区目录形如 ``data_store/lake/events/<事件类型>/<年/月/日>``，单条事件
文件（dict）与批次数组文件（list）混存。跨分区检索统一走
``app.lake_scan.LakeReader`` 的快照分页，本服务只负责写入与对外封装。
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timezone
from typing import Any

from app.lake_scan import (
    LakeFilter,
    LakeReader,
    decode_cursor,
    default_lake_base,
    list_partition_days,
    make_filter,
    partition_of,
    read_partition,
)
from app.persistence import json_store
from app.services.base import BaseModuleService


class DataLakeSinkService(BaseModuleService):
    MODULE = "data_lake_sink"
    ENTITY = "落湖配置"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["配置编号", "数据来源", "目标路径", "分区策略", "写入格式", "压缩算法", "保留天数", "落湖水位"]
    REQUIRED_FIELDS = ["配置编号", "数据来源", "目标路径"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def __init__(self, lake_base: str | None = None) -> None:
        super().__init__()
        self._lake_base = lake_base or default_lake_base()
        os.makedirs(self._lake_base, exist_ok=True)
        self._write_stats: dict[str, int] = {}

    # ──────────────────────────────────────────────────────────
    # 读取器：跨分区快照分页（查询与归档共用同一取数口径）
    # ──────────────────────────────────────────────────────────
    def _reader(self) -> LakeReader:
        return LakeReader(lake_base=self._lake_base, store=json_store)

    # ──────────────────────────────────────────────────────────
    # 写入
    # ──────────────────────────────────────────────────────────
    def _partition_path(self, event_type: str, day: date) -> str:
        partition = partition_of(event_type, day)
        path = os.path.join(self._lake_base, *partition.split("/"))
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _atomic_dump(path: str, data: Any) -> None:
        """同目录临时文件 + 原子 rename，避免并发落湖产生半截文件。"""
        directory = os.path.dirname(path)
        fd, tmp_path = tempfile.mkstemp(prefix=".lake_", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, default=str)
            os.replace(tmp_path, path)
        except OSError:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def write_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    source_channel_id: int = 0,
                    event_day: date | str | None = None) -> dict[str, Any]:
        """将事件数据写入数据湖。

        写入流程：
        1. 确定分区路径（按日期/事件类型）；``event_day`` 用于迟到事件，
           可把同一天迟到的事件显式写入前一日分区
        2. 序列化数据（默认JSON格式，单条事件文件）
        3. 原子写入分区文件
        4. 更新写入统计

        Returns:
            { ok, message, partition, file_path, record_hash }
        """
        now = datetime.now(timezone.utc)
        day = self._coerce_day(event_day, now)
        partition_path = self._partition_path(event_type, day)
        date_partition = day.strftime("%Y/%m/%d")

        record = {
            "event_id": event_id,
            "event_type": event_type,
            "source_channel_id": source_channel_id,
            "payload": payload,
            "ingested_at": now.isoformat(),
            "event_day": day.isoformat(),
            "record_hash": hashlib.sha256(
                json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()[:16],
        }

        # 微秒+随机后缀避免并发落湖时文件名冲突。
        suffix = hashlib.sha1(os.urandom(6)).hexdigest()[:6]
        file_name = f"{now.strftime('%H%M%S%f')}_{event_id[:8]}_{suffix}.json"
        file_path = os.path.join(partition_path, file_name)

        try:
            self._atomic_dump(file_path, record)
            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + 1
            return {
                "ok": True,
                "message": f"数据已写入数据湖分区 {date_partition}",
                "partition": partition_of(event_type, day),
                "file_path": file_path,
                "record_hash": record["record_hash"],
            }
        except OSError as e:
            return {"ok": False, "message": f"写入失败: {e}", "partition": "", "file_path": ""}

    def write_batch(self, records: list[dict[str, Any]], event_type: str = "batch",
                    source_channel_id: int = 0,
                    event_day: date | str | None = None) -> dict[str, Any]:
        """批量写入多条记录到数据湖（批次数组文件，按写入时刻聚合成文件）。

        ``event_day`` 支持把批次写入历史/前一日分区，与单条事件文件混存
        在同一分区，由跨分区读取器统一展开。
        """
        if not records:
            return {"ok": True, "message": "无数据需要写入", "written": 0}

        now = datetime.now(timezone.utc)
        day = self._coerce_day(event_day, now)
        partition_path = self._partition_path(event_type, day)

        batch_file = f"batch_{now.strftime('%H%M%S%f')}.json"
        file_path = os.path.join(partition_path, batch_file)

        batch_records = []
        for i, record in enumerate(records):
            batch_records.append({
                "event_id": f"batch_{now.strftime('%Y%m%d%H%M%S%f')}_{i:04d}",
                "event_type": event_type,
                "source_channel_id": source_channel_id,
                "payload": record,
                "ingested_at": now.isoformat(),
                "event_day": day.isoformat(),
            })

        try:
            self._atomic_dump(file_path, batch_records)
            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + len(batch_records)
            return {
                "ok": True,
                "message": f"批量 {len(batch_records)} 条写入完成",
                "written": len(batch_records),
                "partition": partition_of(event_type, day),
                "file_path": file_path,
            }
        except OSError as e:
            return {"ok": False, "message": f"批量写入失败: {e}", "written": 0}

    @staticmethod
    def _coerce_day(value: date | str | None, now: datetime) -> date:
        if value is None:
            return now.date()
        if isinstance(value, date):
            return value
        for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
        return now.date()

    # ──────────────────────────────────────────────────────────
    # 查询：跨分区快照分页
    # ──────────────────────────────────────────────────────────
    def build_filter(self, **kwargs: Any) -> LakeFilter:
        """对外暴露统一筛选口径，归档候选生成复用同一入口。"""
        return make_filter(**kwargs)

    def query_lake_pages(self, event_type: str | None = None,
                         start_day: str | None = None, end_day: str | None = None,
                         *, size: int = 20, cursor: str | None = None,
                         keyword: str | None = None,
                         source_channel_id: int | None = None,
                         event_ids: list[str] | set[str] | None = None,
                         extra_filters: dict[str, Any] | None = None,
                         query_scope: str | None = None) -> dict[str, Any]:
        """跨分区检索落湖记录，分页按事件顺序组成跨分区数据页。

        - 首屏（无 cursor）：扫描日期范围内全部分区，展开单条文件与批次
          数组，排序后冻结为快照，返回第一页与 next_cursor；
        - 翻页（带 cursor）：只从快照切片，并发落湖的新记录不会进入后续
          分页，后续分页只交付快照内尚未交付的记录；
        - 每条事件带 ``_lake`` 分区来源与读取边界。
        """
        reader = self._reader()
        size = max(1, min(int(size), 500))

        if cursor:
            snapshot_id, offset = decode_cursor(cursor)
            page = reader.page_snapshot(snapshot_id, offset, size)
            page["ok"] = True
            return page

        lake_filter = make_filter(
            event_type=event_type,
            start_day=start_day,
            end_day=end_day,
            keyword=keyword,
            source_channel_id=source_channel_id,
            event_ids=set(event_ids) if event_ids else None,
            extra=extra_filters or {},
        )
        page = reader.first_page(lake_filter, size=size, query_scope=query_scope)
        page["ok"] = True
        return page

    def get_query_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """按快照 ID 回查一次跨分区查询的冻结范围、分区版本与全量记录。"""
        return self._reader().get_snapshot(snapshot_id)

    def find_query_record(self, snapshot_id: str, sequence: int) -> dict[str, Any] | None:
        """按 (快照, 跨分区序号) 定位某条归档候选对应的查询记录。"""
        snapshot = self.get_query_snapshot(snapshot_id)
        if snapshot is None:
            return None
        for record in snapshot.get("records", []):
            lake = record.get("_lake", {})
            if lake.get("sequence") == sequence:
                return record
        return None

    def get_partition_versions(self, event_type: str | None = None) -> list[dict[str, Any]]:
        """分区版本清单：历史分区按文件清单指纹稳定回查。"""
        reader = self._reader()
        if event_type is None:
            return reader.partition_versions()
        partitions = [partition_of(event_type, day)
                      for day in list_partition_days(self._lake_base, event_type)]
        return reader.partition_versions(partitions)

    # ──────────────────────────────────────────────────────────
    # 兼容旧接口：保留按天读取，但底层改为统一展开逻辑
    # ──────────────────────────────────────────────────────────
    def query_lake_data(self, event_type: str, date_str: str | None = None,
                        limit: int = 100) -> dict[str, Any]:
        """查询数据湖中的指定分区数据（旧接口，按天单分区）。"""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        partition = partition_of(event_type, self._coerce_day(date_str, datetime.now(timezone.utc)))
        records, version = read_partition(self._lake_base, partition)

        return {
            "ok": True,
            "records": records[:limit],
            "total": len(records),
            "partition": partition,
            "partition_version": version,
        }

    # ──────────────────────────────────────────────────────────
    # 统计
    # ──────────────────────────────────────────────────────────
    def get_lake_stats(self) -> dict[str, Any]:
        """获取数据湖存储统计信息。"""
        total_files = 0
        total_size = 0
        by_type: dict[str, int] = {}

        for root, dirs, files in os.walk(self._lake_base):
            for f in files:
                if f.endswith(".json"):
                    total_files += 1
                    filepath = os.path.join(root, f)
                    try:
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        pass

                parts = root.split(os.sep)
                if "events" in parts:
                    idx = parts.index("events")
                    if len(parts) > idx + 1:
                        etype = parts[idx + 1]
                        by_type[etype] = by_type.get(etype, 0) + 1

        return {
            "base_dir": self._lake_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "by_type": dict(sorted(by_type.items(), key=lambda x: x[1], reverse=True)),
            "write_stats": self._write_stats,
        }

    def get_partition_info(self) -> list[dict[str, Any]]:
        """获取所有分区信息。"""
        partitions = []
        for root, dirs, files in os.walk(self._lake_base):
            json_files = [f for f in files if f.endswith(".json")]
            if json_files:
                rel_path = os.path.relpath(root, self._lake_base)
                total = 0
                for f in json_files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
                partitions.append({
                    "path": rel_path,
                    "file_count": len(json_files),
                    "size_bytes": total,
                })
        return partitions

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据落湖的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        lake_stats = self.get_lake_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据落湖数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "lake_files": lake_stats.get("total_files", 0),
            "lake_size_mb": lake_stats.get("total_size_mb", 0),
            "operation": self.OPERATION_TERM,
        }
