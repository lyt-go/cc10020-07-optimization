"""跨分区落湖检索与归档候选链路测试。

覆盖需求点：
1. 单条事件文件与批次数组混存时，统一展开并按事件顺序排序；
2. 分页边界跨越单条文件与批次数组、跨日分区组成连续数据页；
3. 快照隔离：首屏之后并发落湖的新记录不进入后续分页；
4. 分区版本指纹稳定可回查，分区内容变化后版本更新；
5. 迟到事件可落入前一日分区，并落在同一次查询范围内；
6. 归档候选与查询共用筛选口径，候选可回查到对应查询记录；
7. 归档执行从跨分区查询数据页读取有效记录，历史可回查；
8. 旧版按天查询接口保持兼容。
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence as persistence
from app.lake_scan import ARCHIVE_RUN_KEY, SNAPSHOT_KEY, decode_cursor
from app.persistence import JsonStore
from app.services import data_lake_sink as sink_mod
from app.services import event_archive as archive_mod
from app.services.data_lake_sink import DataLakeSinkService
from app.services.event_archive import EventArchiveService


@pytest.fixture
def env(tmp_path, monkeypatch):
    """隔离的 JsonStore + 湖目录 + 归档目录。"""
    store = JsonStore(base_dir=str(tmp_path / "store"))
    monkeypatch.setattr(persistence, "json_store", store)
    monkeypatch.setattr(sink_mod, "json_store", store)
    monkeypatch.setattr(archive_mod, "json_store", store)

    lake_base = str(tmp_path / "lake")
    archive_base = str(tmp_path / "archive")
    sink = DataLakeSinkService(lake_base=lake_base)
    archiver = EventArchiveService(lake_base=lake_base, archive_base=archive_base)
    return {
        "store": store,
        "lake_base": lake_base,
        "archive_base": archive_base,
        "sink": sink,
        "archiver": archiver,
    }


def _seed_three_days(env):
    """落湖三天数据：每天同时包含单条事件文件与批次数组文件。"""
    sink: DataLakeSinkService = env["sink"]
    today = datetime.now(timezone.utc).date()
    days = [today - timedelta(days=2), today - timedelta(days=1), today]

    expected = 0
    for day in days:
        # 批次数组文件（2 条）
        result = sink.write_batch(
            [{"order_id": f"{day}-B1"}, {"order_id": f"{day}-B2"}],
            event_type="order_created",
            source_channel_id=7,
            event_day=day,
        )
        assert result["ok"]
        expected += 2
        # 单条事件文件（1 条）
        result = sink.write_event(
            f"evt-{day.isoformat()}-single",
            "order_created",
            {"order_id": f"{day}-S1"},
            source_channel_id=7,
            event_day=day,
        )
        assert result["ok"]
        expected += 1
    return days, expected


# ──────────────────────────────────────────────────────────
# 1. 混存展开与事件顺序
# ──────────────────────────────────────────────────────────
def test_mixed_files_expanded_with_source_coordinates(env):
    days, expected = _seed_three_days(env)
    page = env["sink"].query_lake_pages(
        event_type="order_created",
        start_day=days[0].isoformat(),
        end_day=days[-1].isoformat(),
        size=50,
    )
    assert page["ok"]
    assert page["total"] == expected

    # 事件顺序：分区日 → 来源文件 → 批次数组内序号
    partitions = [item["_lake"]["partition"] for item in page["items"]]
    assert partitions == sorted(partitions)

    layouts = {item["_lake"]["file_layout"] for item in page["items"]}
    assert layouts == {"single_event", "batch_array"}

    # 每条事件都带分区来源与读取边界
    for item in page["items"]:
        lake = item["_lake"]
        assert lake["partition"].startswith("events/order_created/")
        assert lake["source_file"].endswith(".json")
        assert lake["partition_version"]
        assert lake["read_boundary"]["snapshot_id"] == page["snapshot_id"]
        assert isinstance(lake["sequence"], int)

    # 批次数组内序号从 0 连续编号
    batch_items = [i for i in page["items"] if i["_lake"]["file_layout"] == "batch_array"]
    per_file: dict[str, list[int]] = {}
    for item in batch_items:
        per_file.setdefault(item["_lake"]["source_file"], []).append(item["_lake"]["file_index"])
    for indexes in per_file.values():
        assert indexes == list(range(len(indexes)))


# ──────────────────────────────────────────────────────────
# 2. 跨分区分页，边界跨越单条文件与批次数组
# ──────────────────────────────────────────────────────────
def test_cross_partition_pagination_with_cursor(env):
    days, expected = _seed_three_days(env)
    sink: DataLakeSinkService = env["sink"]

    first = sink.query_lake_pages(
        event_type="order_created",
        start_day=days[0].isoformat(),
        end_day=days[-1].isoformat(),
        size=4,
    )
    assert len(first["items"]) == 4
    assert first["has_more"] is True
    assert first["next_cursor"]

    seen_sequences: list[int] = []
    seen_layouts_in_page: list[set[str]] = []
    cursor = first["next_cursor"]
    pages = [first]
    while cursor:
        page = sink.query_lake_pages(event_type="order_created", cursor=cursor, size=4)
        pages.append(page)
        cursor = page["next_cursor"]

    for page in pages:
        seen_sequences.extend(item["_lake"]["sequence"] for item in page["items"])
        seen_layouts_in_page.append({item["_lake"]["file_layout"] for item in page["items"]})

    # 跨分区序号连续无重复无缺失
    assert seen_sequences == list(range(expected))
    # 至少有一页同时包含单条文件与批次数组记录 → 分页边界跨越两种文件
    assert any(layouts == {"single_event", "batch_array"} for layouts in seen_layouts_in_page)
    # 至少有一页同时包含两个分区的记录 → 分页边界跨越日分区
    partition_sets = [
        {item["_lake"]["partition"] for item in page["items"]} for page in pages
    ]
    assert any(len(parts) > 1 for parts in partition_sets)
    assert pages[-1]["has_more"] is False
    assert pages[-1]["next_cursor"] is None


# ──────────────────────────────────────────────────────────
# 3. 快照隔离并发落湖：后续分页只交付快照内记录
# ──────────────────────────────────────────────────────────
def test_snapshot_isolates_concurrent_writes(env):
    days, expected = _seed_three_days(env)
    sink: DataLakeSinkService = env["sink"]

    first = sink.query_lake_pages(
        event_type="order_created",
        start_day=days[0].isoformat(),
        end_day=days[-1].isoformat(),
        size=4,
    )
    snapshot_id = first["snapshot_id"]

    # 首屏返回后并发落湖新事件（同一天迟到写入前一日分区）
    sink.write_event(
        "evt-late-concurrent",
        "order_created",
        {"order_id": "LATE-1"},
        source_channel_id=7,
        event_day=days[1],
    )

    # 翻页仍读旧快照：总数不变、看不到新事件
    second = sink.query_lake_pages(event_type="order_created", cursor=first["next_cursor"], size=4)
    assert second["snapshot_id"] == snapshot_id
    assert second["total"] == expected
    all_ids = [item["event_id"] for item in first["items"] + second["items"]]
    # 继续翻完剩余页
    cursor = second["next_cursor"]
    while cursor:
        page = sink.query_lake_pages(event_type="order_created", cursor=cursor, size=4)
        all_ids.extend(item["event_id"] for item in page["items"])
        cursor = page["next_cursor"]
    assert "evt-late-concurrent" not in all_ids
    assert len(all_ids) == expected

    # 新查询（新快照）才能看到并发落湖的记录
    fresh = sink.query_lake_pages(
        event_type="order_created",
        start_day=days[0].isoformat(),
        end_day=days[-1].isoformat(),
        size=50,
    )
    assert fresh["snapshot_id"] != snapshot_id
    assert fresh["total"] == expected + 1
    assert "evt-late-concurrent" in [item["event_id"] for item in fresh["items"]]


# ──────────────────────────────────────────────────────────
# 4. 分区版本指纹：稳定、可回查、变化可感知
# ──────────────────────────────────────────────────────────
def test_partition_versions_stable_and_track_changes(env):
    days, _ = _seed_three_days(env)
    sink: DataLakeSinkService = env["sink"]

    versions_first = {v["partition"]: v["version"]
                      for v in sink.get_partition_versions("order_created")}
    assert len(versions_first) == 3

    # 历史分区版本在重新扫描后保持稳定
    versions_second = {v["partition"]: v["version"]
                       for v in sink.get_partition_versions("order_created")}
    assert versions_first == versions_second

    # 分区内容变化后版本更新，未变化的历史分区版本不变
    target_partition = f"events/order_created/{days[0].strftime('%Y/%m/%d')}"
    old_version = versions_first[target_partition]
    sink.write_event("evt-version-probe", "order_created", {"k": "v"},
                     source_channel_id=1, event_day=days[0])
    versions_third = {v["partition"]: v["version"]
                      for v in sink.get_partition_versions("order_created")}
    assert versions_third[target_partition] != old_version
    untouched = f"events/order_created/{days[1].strftime('%Y/%m/%d')}"
    assert versions_third[untouched] == versions_first[untouched]

    # 快照内冻结的分区版本仍可回查当时的读取边界
    page = sink.query_lake_pages(
        event_type="order_created",
        start_day=days[0].isoformat(),
        end_day=days[-1].isoformat(),
        size=50,
    )
    snapshot = sink.get_query_snapshot(page["snapshot_id"])
    assert target_partition in snapshot["partitions"]
    assert snapshot["partitions"][target_partition]["version"] == versions_third[target_partition]


# ──────────────────────────────────────────────────────────
# 5. 迟到事件落入前一日分区，且在同一次查询范围内
# ──────────────────────────────────────────────────────────
def test_late_event_lands_in_previous_partition(env):
    sink: DataLakeSinkService = env["sink"]
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)

    result = sink.write_event(
        "evt-late-yesterday", "order_created", {"order_id": "LATE"},
        source_channel_id=9, event_day=yesterday,
    )
    assert result["partition"] == f"events/order_created/{yesterday.strftime('%Y/%m/%d')}"

    # 物理文件确实在前一日分区目录
    partition_dir = os.path.join(
        env["lake_base"], "events", "order_created", *yesterday.strftime("%Y/%m/%d").split("/")
    )
    assert os.path.isdir(partition_dir)
    assert any(f.endswith(".json") for f in os.listdir(partition_dir))

    # 归档候选也读取这次查询范围：查询范围覆盖前一日时能读到迟到事件
    page = sink.query_lake_pages(
        event_type="order_created",
        start_day=yesterday.isoformat(),
        end_day=today.isoformat(),
        size=20,
    )
    assert page["total"] == 1
    assert page["items"][0]["event_id"] == "evt-late-yesterday"
    assert page["items"][0]["_lake"]["partition"].endswith(yesterday.strftime("%Y/%m/%d"))


# ──────────────────────────────────────────────────────────
# 6. 归档候选与查询共用筛选口径，候选可回查查询记录
# ──────────────────────────────────────────────────────────
def test_archive_candidates_share_filter_and_traceable(env):
    days, expected = _seed_three_days(env)
    sink: DataLakeSinkService = env["sink"]
    archiver: EventArchiveService = env["archiver"]

    policy = {
        "id": 1,
        "事件类型": "order_created",
        "归档条件": "days>2",
        "压缩格式": "zlib",
        "保留天数": "90",
    }
    pack = archiver.collect_archive_candidates(policy)

    # days>2 → 查询范围上界为前天；前天的 3 条命中，其余日期不进候选
    bound_day = datetime.now(timezone.utc).date() - timedelta(days=2)
    expected_partition = f"events/order_created/{bound_day.strftime('%Y/%m/%d')}"
    assert pack["total_records"] == 3
    assert len(pack["candidates"]) == 3
    assert all(c["partition"] == expected_partition for c in pack["candidates"])

    # 用同一筛选口径直接查询，命中数一致
    shared_filter = archiver.build_archive_filter(policy)
    query_page = sink._reader().first_page(shared_filter, size=50, query_scope="test")
    assert query_page["total"] == pack["total_records"]

    # 点开归档候选可以找到其对应的查询记录
    candidate = pack["candidates"][0]
    record = sink.find_query_record(candidate["snapshot_id"], candidate["sequence"])
    assert record is not None
    assert record["event_id"] == candidate["event_id"]
    assert record["_lake"]["source_file"] == candidate["source_file"]
    assert record["_lake"]["read_boundary"] == candidate["read_boundary"]


# ──────────────────────────────────────────────────────────
# 7. 归档执行从数据页读取候选并持久化可回查历史
# ──────────────────────────────────────────────────────────
def test_execute_archive_from_snapshot_pages(env):
    days, _ = _seed_three_days(env)
    store: JsonStore = env["store"]
    archiver: EventArchiveService = env["archiver"]

    policy = store.insert("event_archive", {
        "策略编号": "AR-IT-001",
        "事件类型": "order_created",
        "归档条件": "days>2",
        "归档路径": "/archive/it",
        "压缩格式": "zlib",
        "保留天数": "90",
        "清理方式": "删除源文件",
        "恢复超时": "60",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })
    result = archiver.execute_archive(policy["id"])
    assert result["ok"]
    # 前天分区共 2 个文件（1 个批次 + 1 个单条）
    assert result["archived"] == 2
    assert result["candidate_total"] == 3
    assert result["snapshot_id"]

    # 归档压缩文件落盘
    zlib_files = []
    for root, _dirs, files in os.walk(env["archive_base"]):
        zlib_files.extend(f for f in files if f.endswith(".zlib"))
    assert len(zlib_files) == 2

    # 归档历史持久化且可按 run_id 回查，坐标指向查询快照
    runs = store.get_all(ARCHIVE_RUN_KEY)
    assert len(runs) == 1
    detail = archiver.get_archive_run(result["run_id"])
    assert detail is not None
    assert detail["snapshot_id"] == result["snapshot_id"]
    assert detail["candidate_total"] == 3
    archived_parts = {f["partition"] for f in detail["files"]}
    bound_day = datetime.now(timezone.utc).date() - timedelta(days=2)
    assert archived_parts == {f"events/order_created/{bound_day.strftime('%Y/%m/%d')}"}

    # 归档统计中的 history 来自持久化存储
    stats = archiver.get_archive_stats()
    assert stats["history"][0]["run_id"] == result["run_id"]


def test_archive_candidates_compatible_with_historical_partitions(env):
    """历史分区（无版本元数据的旧文件）仍可被候选扫描兼容读取。"""
    sink: DataLakeSinkService = env["sink"]
    archiver: EventArchiveService = env["archiver"]
    old_day = datetime.now(timezone.utc).date() - timedelta(days=30)

    # 直接按旧布局手写一个历史单条文件，模拟历史分区
    part_dir = os.path.join(
        env["lake_base"], "events", "legacy_type", *old_day.strftime("%Y/%m/%d").split("/")
    )
    os.makedirs(part_dir, exist_ok=True)
    import json
    with open(os.path.join(part_dir, "120000_legacy00.json"), "w", encoding="utf-8") as fh:
        json.dump({
            "event_id": "legacy-event-1",
            "event_type": "legacy_type",
            "source_channel_id": 3,
            "payload": {"v": 1},
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }, fh)

    policy = {"id": 2, "事件类型": "ALL", "归档条件": "days>10",
              "压缩格式": "zlib", "保留天数": "90"}
    pack = archiver.collect_archive_candidates(policy)
    assert pack["total_records"] >= 1
    candidate = next(c for c in pack["candidates"] if c["event_id"] == "legacy-event-1")
    assert candidate["file_layout"] == "single_event"
    assert candidate["partition_version"]


# ──────────────────────────────────────────────────────────
# 8. 游标与旧接口契约
# ──────────────────────────────────────────────────────────
def test_invalid_cursor_rejected(env):
    with pytest.raises(ValueError):
        decode_cursor("not-a-valid-cursor")
    with pytest.raises(ValueError):
        env["sink"].query_lake_pages(event_type="x", cursor="@@bad@@", size=10)


def test_legacy_query_lake_data_contract(env):
    days, _ = _seed_three_days(env)
    sink: DataLakeSinkService = env["sink"]
    legacy = sink.query_lake_data("order_created", date_str=days[0].strftime("%Y/%m/%d"))
    assert legacy["ok"]
    assert legacy["total"] == 3
    assert legacy["partition"].startswith("events/order_created/")
    assert legacy["partition_version"]
    # 旧调用方不传日期时仍默认当天分区
    today_legacy = sink.query_lake_data("order_created")
    assert today_legacy["ok"]
    assert today_legacy["total"] == 3


def test_keyword_and_channel_filters(env):
    _seed_three_days(env)
    sink: DataLakeSinkService = env["sink"]
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=2)).isoformat()

    by_channel = sink.query_lake_pages(
        event_type="order_created", start_day=start, end_day=today.isoformat(),
        source_channel_id=999, size=50,
    )
    assert by_channel["total"] == 0

    by_keyword = sink.query_lake_pages(
        event_type="order_created", start_day=start, end_day=today.isoformat(),
        keyword="single", size=50,
    )
    assert by_keyword["total"] == 3
    assert all("single" in item["event_id"] for item in by_keyword["items"])


def test_snapshot_registry_caps_kept_snapshots(env):
    """快照注册表自动淘汰旧快照，避免无限增长。"""
    sink: DataLakeSinkService = env["sink"]
    today = datetime.now(timezone.utc).date()
    sink.write_event("e1", "t", {}, event_day=today)
    for _ in range(55):
        sink.query_lake_pages(event_type="t", start_day=today.isoformat(),
                              end_day=today.isoformat(), size=10)
    rows = env["store"].get_all(SNAPSHOT_KEY)
    assert len(rows) <= 50
