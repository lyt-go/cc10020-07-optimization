"""事件归档清理领域服务：历史事件归档、压缩与过期清理。

管理历史事件数据的归档策略，支持按条件归档、
数据压缩和过期数据的清理调度。
跨模块关联：data_lake_sink（归档来源）、integration_monitor（归档统计）。

归档候选与跨分区落湖查询共用同一套筛选口径（``app.lake_scan.LakeFilter``）：
先按策略条件打开跨分区查询快照、逐页读取有效记录，再从数据页中筛选归档
候选；每个候选都带快照 ID 与跨分区序号，可回查到对应的查询记录和分区版本。
"""
from __future__ import annotations

import os
import shutil
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any

from app.lake_scan import (
    ARCHIVE_RUN_KEY,
    LakeReader,
    iter_snapshot_pages,
    make_filter,
    select_archive_candidates,
)
from app.persistence import json_store
from app.services.base import BaseModuleService
from app.services.data_lake_sink import DataLakeSinkService


class EventArchiveService(BaseModuleService):
    MODULE = "event_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略编号", "事件类型", "归档条件", "归档路径", "压缩格式", "保留天数", "清理方式", "恢复超时"]
    REQUIRED_FIELDS = ["策略编号", "事件类型", "归档条件"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def __init__(self, lake_base: str | None = None, archive_base: str | None = None) -> None:
        super().__init__()
        sink = DataLakeSinkService(lake_base=lake_base)
        self._lake_base = sink._lake_base
        if archive_base is None:
            archive_base = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                "data_store", "archive",
            )
        self._archive_base = archive_base
        os.makedirs(self._archive_base, exist_ok=True)

    def _reader(self) -> LakeReader:
        return LakeReader(lake_base=self._lake_base, store=json_store)

    # ──────────────────────────────────────────────────────────
    # 筛选口径：与跨分区查询共用
    # ──────────────────────────────────────────────────────────
    @staticmethod
    def _parse_days_threshold(archive_condition: str) -> int:
        """解析 ``days>N`` 归档条件，缺省 30 天。"""
        try:
            return int(str(archive_condition).split(">")[-1].strip())
        except (ValueError, IndexError):
            return 30

    def build_archive_filter(self, policy: dict[str, Any]) -> Any:
        """把归档策略翻译成与查询一致的 LakeFilter。

        - 事件类型 ``ALL`` 时跨全部事件类型分区；
        - ``days>N`` 翻译成快照日期上界（只扫描 N 天前及更早的历史分区，
          当天迟到落入前一日分区的事件只要落在查询范围内仍会被读到）。
        """
        event_type = str(policy.get("事件类型", "ALL")).strip()
        threshold = self._parse_days_threshold(policy.get("归档条件", "days>30"))
        end_day = datetime.now(timezone.utc).date() - timedelta(days=threshold)
        return make_filter(
            event_type=None if event_type in ("", "ALL", "*") else event_type,
            end_day=end_day,
        )

    def collect_archive_candidates(self, policy: dict[str, Any],
                                   page_size: int = 200) -> dict[str, Any]:
        """生成归档候选：打开跨分区查询快照，逐页读取有效记录再筛选。

        返回快照 ID、查询范围、分区版本与候选清单；候选保留每条记录的
        分区来源和读取边界，点开候选即可找到其对应的查询记录。
        """
        reader = self._reader()
        lake_filter = self.build_archive_filter(policy)
        first_page = reader.first_page(
            lake_filter, size=page_size,
            query_scope=f"archive:policy={policy.get('id')}",
        )
        snapshot_id = first_page["snapshot_id"]

        candidates: list[dict[str, Any]] = []
        pages_read = 0
        for page in iter_snapshot_pages(reader, snapshot_id, page_size):
            pages_read += 1
            candidates.extend(select_archive_candidates(page["items"]))

        return {
            "snapshot_id": snapshot_id,
            "scope": first_page.get("scope"),
            "filter": reader.get_snapshot(snapshot_id)["filter"],
            "total_records": first_page["total"],
            "pages_read": pages_read,
            "partitions": first_page.get("partitions", {}),
            "candidates": candidates,
        }

    # ──────────────────────────────────────────────────────────
    # 归档执行
    # ──────────────────────────────────────────────────────────
    def execute_archive(self, policy_id: int) -> dict[str, Any]:
        """执行事件归档任务。

        流程：
        1. 加载归档策略并校验生效状态
        2. 用与查询一致的筛选口径打开跨分区快照，逐页读取有效记录
        3. 从数据页筛选归档候选（带分区来源与读取边界）
        4. 按候选来源文件去重后压缩归档，兼容历史分区
        5. 清理热存储中的过期归档并持久化本次执行记录
        """
        policy = json_store.get_by_id(self.MODULE, policy_id)
        if policy is None:
            return {"ok": False, "message": f"归档策略 {policy_id} 不存在", "archived": 0}

        if policy.get("status") != "已生效":
            return {"ok": False, "message": f"归档策略未生效({policy['status']})", "archived": 0}

        compress_format = str(policy.get("压缩格式", "zlib"))
        retention_days = int(policy.get("保留天数", "90") or "90")
        candidate_pack = self.collect_archive_candidates(policy)

        now = datetime.now(timezone.utc)
        archive_date = now.strftime("%Y%m%d")
        archive_batch = os.path.join(self._archive_base, f"arch_{archive_date}")

        # 同一来源文件可能包含多条候选（批次数组），按文件去重后归档一次。
        source_files: dict[str, dict[str, Any]] = {}
        for candidate in candidate_pack["candidates"]:
            key = f"{candidate['partition']}/{candidate['source_file']}"
            source_files.setdefault(key, candidate)

        archived_files: list[dict[str, Any]] = []
        total_size = 0
        for rel_key, candidate in sorted(source_files.items()):
            src_path = os.path.join(
                self._lake_base,
                *candidate["partition"].split("/"),
                candidate["source_file"],
            )
            if not os.path.exists(src_path):
                continue
            try:
                os.makedirs(archive_batch, exist_ok=True)
                archive_name = rel_key.replace("/", "_")
                archive_path = os.path.join(archive_batch, archive_name)

                if compress_format == "zlib":
                    with open(src_path, "rb") as src:
                        compressed = zlib.compress(src.read())
                    with open(archive_path + ".zlib", "wb") as dst:
                        dst.write(compressed)
                else:
                    shutil.copy2(src_path, archive_path)

                total_size += os.path.getsize(src_path)
                archived_files.append({
                    "partition": candidate["partition"],
                    "partition_version": candidate["partition_version"],
                    "source_file": candidate["source_file"],
                    "snapshot_id": candidate["snapshot_id"],
                    "sequence_range": [candidate["sequence"]],
                    "archive_file": os.path.basename(archive_path)
                    + (".zlib" if compress_format == "zlib" else ""),
                })
            except OSError:
                continue

        cleaned = self._clean_expired_archives(retention_days)

        run_record = {
            "run_id": f"arch_run_{now.strftime('%Y%m%d%H%M%S%f')}",
            "policy_id": policy_id,
            "snapshot_id": candidate_pack["snapshot_id"],
            "scope": candidate_pack["scope"],
            "filter": candidate_pack["filter"],
            "candidate_total": len(candidate_pack["candidates"]),
            "archived": len(archived_files),
            "size_bytes": total_size,
            "cleaned": cleaned,
            "partitions": candidate_pack["partitions"],
            "files": archived_files,
            "timestamp": now.isoformat(),
        }
        persisted = json_store.insert(ARCHIVE_RUN_KEY, run_record)

        return {
            "ok": True,
            "message": f"归档完成: {len(archived_files)} 文件, {round(total_size / 1024, 2)} KB",
            "archived": len(archived_files),
            "candidate_total": len(candidate_pack["candidates"]),
            "total_size_bytes": total_size,
            "cleaned_old": cleaned,
            "snapshot_id": candidate_pack["snapshot_id"],
            "scope": candidate_pack["scope"],
            "run_id": persisted.get("run_id"),
            "files": archived_files,
        }

    def _clean_expired_archives(self, retention_days: int) -> int:
        """清理超过保留期的归档文件。"""
        cleaned = 0
        now = datetime.now(timezone.utc)
        if not os.path.exists(self._archive_base):
            return 0

        for root, dirs, files in os.walk(self._archive_base):
            for f in files:
                filepath = os.path.join(root, f)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
                    if (now - mtime).days > retention_days:
                        os.remove(filepath)
                        cleaned += 1
                except OSError:
                    continue
        return cleaned

    def get_archive_stats(self) -> dict[str, Any]:
        """获取归档存储统计。"""
        total_files = 0
        total_size = 0
        for root, dirs, files in os.walk(self._archive_base):
            for f in files:
                total_files += 1
                try:
                    total_size += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass

        return {
            "base_dir": self._archive_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "archive_batches": len([d for d in os.listdir(self._archive_base)
                                    if os.path.isdir(os.path.join(self._archive_base, d))]) if os.path.exists(self._archive_base) else 0,
            "history": self.list_archive_runs()[-10:],
        }

    def list_archive_runs(self) -> list[dict[str, Any]]:
        """回查归档执行历史（含快照坐标与分区版本）。"""
        return json_store.get_all(ARCHIVE_RUN_KEY)

    def get_archive_run(self, run_id: str) -> dict[str, Any] | None:
        """按 run_id 回查单次归档，并附带快照来源信息。"""
        for row in self.list_archive_runs():
            if row.get("run_id") == run_id:
                return row
        return None

    def restore_from_archive(self, archive_file: str) -> dict[str, Any]:
        """从归档恢复数据。"""
        for root, dirs, files in os.walk(self._archive_base):
            for f in files:
                if f == archive_file or f.startswith(archive_file):
                    filepath = os.path.join(root, f)
                    try:
                        if f.endswith(".zlib"):
                            with open(filepath, "rb") as src:
                                decompressed = zlib.decompress(src.read())
                            return {"ok": True, "data": decompressed.decode()}
                        else:
                            with open(filepath, "r") as src:
                                return {"ok": True, "data": src.read()}
                    except (OSError, zlib.error):
                        return {"ok": False, "message": "归档文件损坏或不存在"}
        return {"ok": False, "message": "归档文件未找到"}

    def domain_snapshot(self) -> dict[str, Any]:
        """输出事件归档清理的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        archive_stats = self.get_archive_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件归档清理数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "archive_files": archive_stats.get("total_files", 0),
            "archive_size_mb": archive_stats.get("total_size_mb", 0),
            "operation": self.OPERATION_TERM,
        }
