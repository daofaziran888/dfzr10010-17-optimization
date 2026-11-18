# 湖仓一体化治理平台

面向数据架构师的湖仓一体化治理平台。覆盖数据湖表格式管理、Iceberg/Hudi表优化、小文件合并、数据压缩策略、元数据服务、表快照管理、数据分支合并与数据回滚恢复的完整湖仓治理链路。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
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
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 湖表格式管理 | `tablefmt` | 表格式配置 | 配置编号、表名称、格式类型 |
| 小文件合并 | `compact` | 合并任务 | 任务编号、目标表名、文件范围 |
| 数据压缩策略 | `compress` | 压缩配置 | 配置编号、目标表名、压缩算法 |
| 元数据服务 | `metadata` | 元数据表 | 表编号、表名称、字段清单 |
| 表快照管理 | `snapshot` | 快照记录 | 快照编号、表名称、快照时间 |
| 数据分支合并 | `branch` | 分支记录 | 分支编号、表名称、分支类型 |
| 数据回滚恢复 | `rollback` | 回滚记录 | 记录编号、表名称、回滚版本 |
| 分区演进管理 | `partition` | 演进计划 | 计划编号、表名称、当前分区 |
| 表优化管理 | `iceberg` | 优化任务 | 任务编号、表名称、优化类型 |
| 数据目录同步 | `catalog` | 同步作业 | 作业编号、源路径、目标目录 |
| 数据过期清理 | `cleanup` | 清理策略 | 策略编号、目标表名、过期条件 |
| 湖存储配额 | `quota` | 配额配置 | 配置编号、存储空间、表数量上限 |
| 湖仓血缘追踪 | `lineage` | 血缘记录 | 记录编号、上游表名、下游表名 |
| 治理任务调度 | `schedule` | 治理任务 | 任务编号、任务类型、调度表达式 |
| 表统计信息 | `stats` | 统计报告 | 报告编号、表名称、行总数 |
| 数据迁移管理 | `migrate` | 迁移任务 | 任务编号、源表名、目标表名 |

## 小文件合并：结果数据集与收益复核

小文件合并除了任务清单，还维护一份**合并任务结果数据集**（`compact_dataset/v1`），
把分片、分组边界和收益记账固定下来，供合并列表与表统计信息两个入口共用。

- 分组计划：读取任务的文件范围与分片统计后，按分区顺序做 next-fit 切分，
  每组记录 `起始分片`/`结束分片` 与分区内序号。例如 `orders` 240 个分片、
  64MB 目标大小 → 58 组，分片大小 P50=9MB、P95=21MB。
- 续跑：分组边界生成后不再改变。失败组重试、中断续跑都从**原分片边界**继续，
  已成功的组跳过，不会重复处理或重复计数。
- 收益记账：每成功一组往台账写一条，同一个分组身份（分区 + 起始分片 + 结束分片）
  只记一次；复核清单与统计报告都只从台账汇总。
- 文件范围扩展：
  - `append`：保留原分组边界，只给新增分片排组，旧完成组收益继续有效；
  - `replan`：按当前范围重排，边界未变的旧完成组继承收益，被覆盖掉的记为
    “过期完成项”并从台账剔除，复核清单最终只保留一次有效收益。
- 空组：分区没有分片、分片容量为 0、或单分片未超目标大小时单独标记，
  写明原因，不参与收益累计。
- 表统计信息 `GET /api/stats/compact-report`：先声明兼容目标
  （`compact_dataset/v1` 必需字段），再读取结果数据集，按成功分组汇总
  文件数量下降与容量下降；读取失败时只重试该任务未完成的分组，
  仍失败则退回上一稳定口径并在 `本次错误` 里说明。

主要接口（`backend/app/routers/compact.py`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/compact/datasets` | 合并任务结果数据集列表 |
| POST | `/api/compact/datasets/generate` | 读取现有任务补生成结果数据集（幂等） |
| GET | `/api/compact/datasets/{task_id}` | 收益详情：已完成组、失败组、剩余组、空组说明、账本 |
| POST | `/api/compact/datasets/{task_id}/run` | 执行未完成分组，可指定 `group_ids` 重试单个组 |
| POST | `/api/compact/datasets/{task_id}/resume` | 从剩余分片续跑 |
| POST | `/api/compact/datasets/{task_id}/interrupt` | 中断当前执行 |
| POST | `/api/compact/datasets/{task_id}/scope` | 文件范围扩展（`append` / `replan`） |
| GET | `/api/compact/reviews` | 收益复核清单（一个任务一条） |
| POST | `/api/compact/reviews/{task_id}/reconcile` | 复核结果数据集并同步复核清单 |
| GET | `/api/stats/compact-report` | 合并收益统计报告 |

后端用例：`cd backend && .venv/bin/python -m unittest discover -s tests -v`。

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
