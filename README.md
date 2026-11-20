# API集成与事件编排平台

面向第三方API接入、Webhook事件接收、消息队列消费与服务编排的全链路API数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| API端点注册 | `api_endpoint` | 端点配置 | 端点编号、端点名称、请求方法 |
| Webhook通道 | `webhook_channel` | 通道配置 | 通道编号、通道名称、接收路径 |
| 事件结构定义 | `event_schema` | 事件结构 | 结构编号、事件类型、字段名称 |
| 消息队列消费 | `mq_consumer` | 消费配置 | 配置编号、队列名称、消费组名 |
| 死信队列管理 | `dlq_handler` | 死信记录 | 记录编号、原始队列、失败原因 |
| 认证凭证管理 | `api_auth_store` | 认证凭证 | 凭证编号、关联端点、凭证类型 |
| 流量控制 | `rate_limit_config` | 限流规则 | 规则编号、关联端点、请求间隔 |
| 数据转换编排 | `data_transform` | 转换规则 | 规则编号、源Schema、目标Schema |
| 事件分发路由 | `dispatch_rule` | 分发规则 | 规则编号、事件类型、目标系统 |
| 事件去重 | `dedup_filter` | 去重规则 | 规则编号、事件类型、去重键列 |
| 批量接入调度 | `batch_ingest` | 接入任务 | 任务编号、关联端点、Cron表达式 |
| 数据落湖 | `data_lake_sink` | 落湖配置 | 配置编号、数据来源、目标路径 |
| 接入质量校验 | `data_quality_rule` | 质量规则 | 规则编号、数据来源、校验字段 |
| 调用链路追踪 | `call_trace_log` | 调用记录 | 记录编号、关联端点、请求参数 |
| 增量水位管理 | `incre_cursor` | 水位标记 | 标记编号、关联端点、增量字段 |
| 历史回补任务 | `backfill_job` | 回补任务 | 任务编号、关联端点、起始日期 |
| 事件归档清理 | `event_archive` | 归档策略 | 策略编号、事件类型、归档条件 |
| 集成监控看板 | `integration_monitor` | 监控指标 | 指标编号、监控对象、成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 落湖跨分区检索与归档候选

落湖目录按 `data_store/lake/events/<事件类型>/<年/月/日>` 分区，单条事件文件
（dict）与批次数组文件（list）混存。`backend/app/lake_scan.py` 把扫描、展开、
排序、分页固化为数据落湖查询与事件归档清理共用的取数口径：

- **跨分区事件顺序分页**：统一展开两种文件，按「分区日 → 来源文件 → 批次内序号」
  排序，分页边界可跨越单条文件、批次数组和日分区，游标为不透明 base64 token。
- **快照隔离并发落湖**：首屏查询冻结一份只读快照（含分区版本与全量记录坐标），
  后续翻页只从快照切片，并发落湖的新记录只出现在新快照，后续分页只交付新记录。
- **分区来源与读取边界**：每条事件带 `_lake`（分区、来源文件、文件布局、批次序号、
  分区版本、快照 ID、跨分区序号），分区版本由文件清单（文件名+大小+mtime）指纹
  得到，历史分区文件不变则版本稳定、可随时回查。
- **迟到事件**：`write_event/write_batch` 支持 `event_day`，同一天迟到的事件可
  显式写入前一日分区，仍落在同一次查询范围内。
- **归档候选共用筛选口径**：`EventArchiveService.build_archive_filter` 与查询共用
  `LakeFilter`；候选从跨分区查询数据页逐页读取的有效记录中筛选，每个候选带快照 ID
  与序号，可回查到对应查询记录；归档执行历史持久化、按 `run_id` 回查。

### 管道端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/pipeline/lake/query` | 跨分区检索，首屏建快照、翻页带 `cursor` |
| GET | `/api/pipeline/lake/snapshots/{snapshot_id}` | 回查快照冻结范围、分区版本与全量记录 |
| GET | `/api/pipeline/lake/snapshots/{snapshot_id}/records/{sequence}` | 按序号定位候选对应的查询记录 |
| GET | `/api/pipeline/lake/partition-versions` | 分区版本清单（兼容历史分区） |
| POST | `/api/pipeline/archive/candidates` | 按策略生成归档候选（共用筛选口径） |
| GET | `/api/pipeline/archive/runs` / `/api/pipeline/archive/runs/{run_id}` | 归档执行历史回查 |

前端在「数据落湖」工作台内嵌跨分区检索面板，在「事件归档清理」工作台内嵌归档
候选面板（`frontend/src/views/lake/`），候选可一键回查其查询记录与读取边界。

领域：数据工程
子方向：数据采集、爬虫与RPA
