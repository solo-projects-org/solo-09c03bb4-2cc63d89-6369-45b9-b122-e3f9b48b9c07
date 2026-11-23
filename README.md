# 可信数据网关 (Trusted Data Gateway)

一个供数据工程师使用的网页应用：上传本地 Parquet 批次 → 按固定规则校验 → 失败行隔离并给出原因 → 通过后原子写入按日分区的可信数据集。

技术栈：**Python / FastAPI / DuckDB / PyArrow**，**Docker Compose** 一键启动，浏览器入口为 `/`。

---

## 快速开始

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

首次启动会自动从 `config/rules.yaml` 加载基线数据集 `daily_orders`（规则版本 v1）。

生成演示数据：

```bash
python3 scripts/make_sample_data.py sample_data
```

推荐演示顺序：

1. **参考表** 页签上传 `sample_data/customers.parquet`，名称 `customers`，主键 `customer_id`（版本 v1）。
2. **上传批次** 上传 `orders_batch1_good.parquet` → 2 行写入 `2026-11-20` 分区。
3. 上传 `orders_batch2_mixed.parquet` → 2 行发布、4 行进入隔离区；在 **批次与隔离区** 查看每行原因。
4. **规则版本** 页签修改规则并发布 v2（例如把 `amount.max` 改成 5）；再上传 `orders_batch3_newer.parquet`，新批次按 v2 校验。
5. 在 **批次与隔离区** 选中一个失败批次，上传修复后的文件重跑：只替换该批次自己的结果，旧的可信分区不受影响。
6. 联营渠道接入：在 **规则版本** 页签顶部执行「一次性主键切换」，`daily_orders` 的全表主键变为 `(channel, order_id)`；随后上传 `orders_batch4_partner_same_ids.parquet`（联营同号订单）与 `orders_batch5_channel_mixed.parquet`（非法渠道值被隔离），预览中每行都带渠道。

本地（不使用 Docker）运行：

```bash
python3 -m pip install -r requirements.txt
GSB_DATA_DIR=./data GSB_RULES_FILE=./config/rules.yaml \
  uvicorn app.main:app --host 127.0.0.1 --port 8000
```

运行测试：

```bash
python3 -m pytest tests -q
```

---

## 需求与实现映射

| 需求 | 实现 |
| --- | --- |
| 上传本地 Parquet 批次 | `POST /api/datasets/{dataset}/batches`，支持一次多个 `.parquet` 文件，流式落盘并限制大小 |
| 参考表校验后写入可信数据集 | 参考表以不可变版本发布（`POST /api/references`），规则通过 `references` 声明引用列 |
| 字段检查 | 必填、类型、`max_length`、`min`/`max`、`allowed_values`、正则（RE2） |
| 主键唯一 | 批内按 `primary_key` 去重（`unique_scope: dataset` 或 `partition`），并与已发布可信数据做跨批次唯一性检查 |
| 跨表引用检查 | 与**受理时固定**的参考表版本比对，缺失即隔离 |
| 失败行进入隔离区并显示原因 | `quarantine` 表按 `batch_id + attempt` 保存原始行与结构化原因；页面与 `GET /api/batches/{id}/quarantine` 均可查看 |
| 通过后才更新可信分区 | 先校验、再写 staging、最后在元数据事务中切换；任何一步失败都不会变更可信目录 |
| 按日分区 | `trusted/{dataset}/date=YYYY-MM-DD/*.parquet`，每个批次拥有独立不可变文件，读取时合并 |
| 规则版本在受理时固定 | 受理时把规范化规则快照与参考表版本写入批次记录；后续发布新版本不影响已受理批次 |
| 失败批次修复后可重跑 | 可覆盖源文件（`POST /api/batches/{id}/files`），或直接 `POST /api/batches/{id}/run`；次数累加为 `attempt` |
| 只替换该批次结果 | 提交时先删除该 `batch_id` 自己的 `partition_files` 记录和文件，再放入新文件；其他批次文件逐一保留 |
| 过期批次不得覆盖后来批次 | 每批次有递增 `acceptance_seq`；分区的最新发布序列更大时，旧批次在写入前和提交前两次被拒绝（409 `stale_batch`） |
| 校验或写入失败保留旧数据 | 校验失败不落盘；提交阶段用备份 + 回滚恢复旧文件，元数据事务失败同样恢复 |
| 浏览器入口 | `/`（原生 HTML/CSS/JS，无前端构建步骤），`/docs` 为 OpenAPI |
| 联营渠道一次性切换 | `POST /api/datasets/{dataset}/channel-switch`：主键由字符串 `order_id` 切到 `(channel, order_id)`，存量可信文件补写 `channel=direct`，状态持久化、重启保留、重复调用返回已生效版本 |

### 关键状态机

```
accepted ──run──> running ──ok──> published
                     │
                     └──error──> failed ──修复源文件/直接重跑──> running ...
```

- `accepted`：已受理，规则与参考表版本已固定。
- `failed`：校验或写入失败；可信数据保持原样。`previous_result_preserved=true` 表示该批次此前发布的数据仍在。
- 服务重启时遗留的 `running` 会标记为 `failed`（错误码 `interrupted`）并允许重跑。

### 隔离原因代码

| code | 含义 |
| --- | --- |
| `required_missing` | 必填字段为空 |
| `invalid_type` | 原生类型不匹配或文本无法解析为目标类型（如 `2026-02-30`） |
| `too_long` | 超过 `max_length` |
| `pattern_mismatch` | 不匹配 `regex` |
| `below_min` / `above_max` | 超出数值范围 |
| `not_allowed` | 不在 `allowed_values` 中 |
| `reference_missing` | 在固定的参考表版本中不存在 |
| `duplicate_key` | 批内主键重复 |
| `key_conflict_trusted` | 与已发布可信数据的同一分区主键冲突 |
| `unexpected_column` | 上传文件含规则未声明的列（`allow_extra_columns: false` 时整批失败） |

---

## 规则定义

`config/rules.yaml` 仅用于首次启动时为**不存在**的数据集写入 v1；已有数据集不会被打断或覆盖。之后通过页面或 `POST /api/datasets/{name}/rules` 发布新版本，规则版本不可修改，另一字段 `expected_version` 用于乐观并发（版本不匹配返回 409）。

```json
{
  "partition_field": "order_date",
  "primary_key": ["order_id"],
  "unique_scope": "dataset",
  "allow_extra_columns": false,
  "fields": [
    {"name": "order_date", "type": "date", "required": true},
    {"name": "customer_id", "type": "string", "required": true,
     "references": {"table": "customers", "field": "customer_id"}},
    {"name": "amount", "type": "number", "required": true, "min": 0, "max": 1000000},
    {"name": "currency", "type": "string", "required": true, "regex": "^[A-Z]{3}$"}
  ]
}
```

支持的字段类型：`string`、`integer`、`number`、`date`、`timestamp`、`boolean`。

- 原生 Parquet 类型会先按目标类型归一化（真实的 `DATE` 不会被当成 epoch 天数，`int` 不会被隐式当作字符串）。
- 文本输入允许 ISO 日期/时间、数字字符串与 `true/false`，但拒绝 `2026-02-30`、`NaN`、`Infinity`。
- 正则使用 DuckDB 的 RE2，且在发布规则时预校验。

---

## 联营渠道一次性主键切换

`daily_orders` 历史上以**字符串 `order_id` 作为全表唯一键**，规则中从未出现过 `channel`。接入联营（partner）渠道后，同一订单号可能同时来自直营（direct）与联营两个渠道，因此提供一次性、不可逆的口径切换：

| 维度 | 切换前（旧口径） | 切换后（渠道口径） |
| --- | --- | --- |
| 全表主键 | `order_id`（字符串） | `(channel, order_id)` |
| 受理的 Parquet | 无 `channel` 列 | 每行必须有 `channel`，取值仅 `direct` / `partner`，否则隔离（`not_allowed`）或缺列整批失败（`schema_mismatch`） |
| 新发布规则版本 | — | 自动发布新版本，新增必填 `channel` 字段，主键改为复合键，`unique_scope` 固定 `dataset` |
| 存量可信数据 | 原样保留 | 每个可信 Parquet 文件被原子重写，补写 `channel='direct'`，列顺序与新规则一致；切换前先逐文件备份 |
| 切换前受理的批次 | 按受理时固定的 v1 规则与参考表校验 | **无论何时发布**仍用同一套固定规则与参考表，输出自动归为 `direct`；查重仍按单键 `order_id` |
| 可信预览 | 不显示渠道 | 每行显示 `channel`（含迁移后的存量行） |

隔离与唯一性语义：

- **同渠道同号互相隔离**：批内重复为 `duplicate_key`，与已发布可信数据冲突为 `key_conflict_trusted`；跨日、跨新旧批次同样生效（`unique_scope=dataset`）。
- **跨渠道同号共存**：`(direct, O-1)` 与 `(partner, O-1)` 是两个不同的全表键，即使日期不同也各自发布。

一致性与持久化：

- 切换状态写入 `channel_key_switches` 表，每个批次在 `batches.channel_mode` 固定其受理口径（`legacy` / `channel`）；进程重启后口径原样保留，旧库首次打开时自动补齐该列，存量批次默认 `legacy`。
- 切换是**唯一提交点 + 备份回滚**：文件重写失败或元数据事务失败（错误码 `switch_failed`）时，已重写文件从备份恢复，不写入切换记录、不发布新规则版本，受理、查重、预览全部保持旧口径，之后可重新发起切换。
- 切换**幂等**：重复调用返回 `already_effective=true` 与已生效的规则版本，不会产生第二个版本；页面按钮在生效后隐藏。
- 切换与批次处理共用同一把串行锁，避免切换与校验/提交互相穿插。

操作入口：规则版本页签顶部的「联营渠道接入 · 一次性主键切换」卡片，或 `POST /api/datasets/daily_orders/channel-switch`；`GET` 同路径查询当前切换状态。

---

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 浏览器入口 |
| GET | `/healthz` | 健康检查（Compose healthcheck 使用） |
| GET | `/api/summary` | 全局计数 |
| GET/POST | `/api/datasets` | 列表 / 创建数据集（含 v1 规则） |
| GET | `/api/datasets/{name}` | 数据集详情（当前规则、分区） |
| GET/POST | `/api/datasets/{name}/rules` | 规则版本列表 / 发布新版本 |
| GET/POST | `/api/datasets/{name}/channel-switch` | 查询一次性渠道切换状态 / 执行切换（幂等，重复调用返回已生效版本） |
| GET | `/api/datasets/{name}/partitions` | 已发布分区汇总 |
| GET | `/api/datasets/{name}/preview?date=&limit=` | 可信数据预览 |
| GET/POST | `/api/references` | 参考表版本列表 / 发布新版本 |
| GET/POST | `/api/batches`、`/api/datasets/{name}/batches` | 批次列表 / 上传（multipart，`files`、`process`） |
| GET | `/api/batches/{id}` | 批次详情（含固定规则与参考表快照） |
| GET | `/api/batches/{id}/quarantine` | 隔离行与原因 |
| POST | `/api/batches/{id}/run?replace_published=` | 重跑；已发布批次需显式替换 |
| POST | `/api/batches/{id}/files` | 替换失败批次的源文件并重跑 |

常见状态码：`400` 规则/上传无效、`404` 对象不存在、`409` 版本冲突 / 过期批次 / 重复运行 / 已发布、`202` 已受理但校验未通过（响应体含批次与 `run_error`）。

---

## 数据布局

```
$GSB_DATA_DIR/
  meta.duckdb            # 元数据：数据集、规则版本、参考表版本、批次、隔离区、分区文件
  uploads/{batch_id}/    # 批次受理时固定的源文件
  references/{name}/vNNNNNN/data.parquet
  trusted_staging/       # 提交前暂存与备份（成功后清理）
  trusted/{dataset}/date=YYYY-MM-DD/batch-{batch_id}-a{attempt}-{n}.parquet
```

`meta.duckdb` 中的一个 `partition_files` 行即一个可信文件。读取可信数据时按 `union_by_name` 合并，并关闭 Hive 分区推断（分区列已存在于文件内）。

> **部署约束**：DuckDB 元数据写入在进程内串行化，Compose 固定 `--workers 1`。如需横向扩展，应把元数据与提交协调迁移到 PostgreSQL/对象存储，当前实现刻意不声称支持多实例并发写。

---

## 设计取舍与已知限制

- **单实例**：如上所述，DuckDB 单连接 + 单 worker；不做多副本并发发布。
- **每次发布为快照文件集**：重跑批次会生成新文件名并删除该批次旧文件；不修改历史其他批次文件，因此不做增量 upsert。
- **跨批次主键冲突**按“已发布可信数据”判定，冲突行隔离而非整批失败；`unique_scope=partition` 时仅在同一天内比较。
- **参考表只支持唯一键的单列引用**；引用比较按字符串化后的值进行。
- **没有认证/授权**：适合受信任的内网或单机环境；公网部署前需加反向代理鉴权。
- **上传上限**默认 512 MiB/文件、50 个文件/批次，可通过环境变量调整。
- 数据集/参考表目前只增不删；没有归档与清理接口。

---

## 验证情况

自动化测试共 52 个（`tests/test_gateway.py`、`tests/test_spec.py`、`tests/test_channel_switch.py`），除原 39 个基线用例外，渠道切换专项覆盖：

- 默认无切换状态、切换发布含 `channel` 的新不可变规则版本并迁移存量可信文件；
- 切换幂等（重复调用返回已生效版本，不产生新版本）且重启后状态与口径完整回读；
- 切换后新批次缺 `channel` 整批失败、非法渠道值逐行隔离；
- 跨渠道同号共存（含跨日）、同渠道同号在批内与跨批次（含跨日、跨新旧批次）隔离；
- 切换前受理的旧批次在切换后仍按固定 v1 规则发布并归为 `direct`，且保持单键查重；
- 切换在文件迁移阶段或元数据事务阶段失败时备份回滚，受理、查重、预览全部保持旧口径，之后可重试成功；
- 旧版 DuckDB（无 `channel_mode` 列、无切换表、可信文件无 `channel` 列）首次打开自动迁移，默认旧口径，再执行切换全链路正常；
- 预览每行显示渠道。

此外在本环境用真实 HTTP 服务 + 浏览器逻辑（jsdom 加载页面脚本）验证了：数据集/分区/预览渲染（含 channel 列与两渠道数据）、规则页一次性切换卡片的已生效状态与幂等、批次列表的口径标签、上传页渠道提示，均无前端未捕获异常。

原 39 个基线用例仍全部通过，覆盖：

- 基线数据集加载与页面/健康检查；
- 正常批次按日分区发布与预览；
- 字段、正则、枚举、范围、必填、类型失败行进入隔离区且原因完整；
- 批内主键重复隔离；
- 跨批次主键冲突隔离；
- 参考表缺失引用隔离；
- 全行失败 → `failed`，数据集保持为空；
- 规则版本在受理时固定（先受理后改规则，重跑仍按旧版）；
- 参考表版本在受理时固定（后发新版本不影响已受理批次）；
- 失败批次修复源文件后重跑，只替换自身结果，另一批次数据仍在；
- 已发布批次重跑必须显式 `replace_published`，且只替换自身结果；
- 过期批次被 409 拒绝，新批次数据未被覆盖；
- 写入阶段失败（模拟 `_commit` 抛错）后旧分区仍在，可继续重跑成功；
- 已发布批次重跑失败时旧结果保留且标记 `previous_result_preserved`；
- 服务重启恢复 `running` 批次、并发重跑返回 `batch_busy`；
- 规则/引用定义校验、原生类型不被隐式重解释等规格单元测试。
