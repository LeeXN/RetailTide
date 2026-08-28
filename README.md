# RetailTide

RetailTide 是一个面向投资者社区的多源情绪分析与事件研究系统。系统保留可追溯的原始观测，按 Topic（赛道）解析实体，执行 LLM 分析，并聚合热度、趋势和事件指标。

[English](README.en.md) · [MIT License](LICENSE)

## 功能

- 采集东方财富股吧、淘股吧、知乎、小红书和 Wikimedia Pageviews
- 保留追加式原始数据及规范化内容，支持来源下钻和版本追踪
- 按 `Content × Topic` 分析买卖意图、方向、FOMO、恐慌、推广和垃圾内容
- 计算日度散户热度、历史分位、趋势方向和置信度
- 提供行情关联、事件研究、分位研究、REST API 和 Dashboard
- 支持断点续采、来源级重试、限速和 systemd 定时任务

## 系统流程

```text
.env / config/*.yaml
        │
        ├─ status：检查来源、LLM 和行情配置
        └─ refresh：同步 Topic、Asset、Alias 和 Source
                         │
          ┌──────────────┴──────────────┐
          │                             │
       内容来源                    Wikimedia Pageviews
          │                             │
          └──────────────┬──────────────┘
                         ▼
              RawObservation（追加式）
                         │
          normalize → Content / TrendObservation
                         │
             resolve → Topic / Asset / Author
                         │
              Content × Topic LLM 分析
                         │
        metrics → events → market returns → quality
                         │
                    API / Dashboard
```

## 产品界面

### 市场总览

![RetailTide 市场总览](docs/images/dashboard-overview.png)

### 趋势与价格

![RetailTide 趋势与 Wikimedia 对比](docs/images/dashboard-trends.png)

### 历史帖子

![RetailTide 历史帖子与知乎参考回答](docs/images/dashboard-posts.png)

### 研究与溯源

![RetailTide 研究与溯源](docs/images/dashboard-research.png)

## 数据源

| 来源 | 类型 | 数据范围 | LLM 分析 |
| --- | --- | --- | --- |
| 东方财富股吧 | 内容 | 公开只读帖子与评论 | 是 |
| 淘股吧 | 内容 | 公开只读讨论内容 | 是 |
| 知乎 | 内容 / 发现 | A 股、港股和美股交易日复盘高互动回答 | 是 |
| 小红书 | 内容 / 发现 | Spider bridge 与项目自有 `xiaohongshu-mcp` 的只读搜索和详情 | 是 |
| Wikimedia Pageviews | 趋势 | 页面浏览量与独立关注度 | 否 |

Wikimedia 指标独立展示，不进入散户热度公式。知乎回答使用参考交易日；`EditTime` 仅参与相关性校验。

## 分析与指标

### LLM 分析

每条内容按其关联的 Topic 分别分析：

- `intent`：`buy`、`sell`、`hold`、`wait`、`unknown`
- `direction`：`bullish`、`neutral`、`bearish`、`unknown`
- FOMO：紧迫感、害怕错过、社会认同、追涨、后悔
- 情绪与角色：恐慌、新手信号、投资者角色、投资经验
- `promotion`：广告、课程、付费投放、开户链接和商业导流
- `spam`：刷屏、机器人、批量账号等异常内容

意图证据区分本人动作（`explicit_self_*`）、建议（`advice_or_recommendation`）、走势判断（`market_directional_view`）和风险提醒（`risk_warning`）。`promotion` 与 `spam` 独立标记，并从散户情绪、意图、FOMO 和恐慌指标中排除。

### 散户热度指数

日桶按 `Asia/Shanghai` 自然日统计，并按 `Content × Topic` 去重：

- `A`：全部内容数
- `R`：`actor_type=retail`、`promotion=false`、`spam=false` 的内容数
- `B`、`S`：`R` 中 `intent=buy`、`intent=sell` 的数量
- `F`：`R` 中 FOMO 分数至少为 `0.5` 的数量
- `P`：`R` 中 `emotion.panic=true` 的数量

```text
散户占比       = R / A
散户数量       = R / (R + 20)
意图表达       = min(1, (B + S) / max(R, 1))
情绪激活       = min(1, (F + P) / max(R, 1))
方向一致性     = min(1, abs(B - S) / max(R, 1))

Heat = 100 × clamp(
  0.35 × 散户占比
  + 0.25 × 散户数量
  + 0.20 × 意图表达
  + 0.15 × 情绪激活
  + 0.05 × 方向一致性,
  0, 1
)
```

`A=0` 或没有已分析内容时不生成指数。全市场指数对跨 Topic 内容只计一次。

| 指标 | 规则 |
| --- | --- |
| 高置信度 | 已分析内容至少 30 条，分析覆盖率至少 80% |
| 中置信度 | 已分析内容至少 10 条，分析覆盖率至少 50% |
| 低置信度 | 存在分析结果但未达到以上门槛 |
| 历史分位 | 与此前最多 30 个有效日比较；至少需要 5 个历史有效日 |
| 趋势分数 | 日变化 50%、前后 7 日均值变化 30%、前后 30 日均值变化 20% |
| 趋势标签 | `≥12` 快速升温，`≥4` 温和升温，`≤-12` 快速降温，`≤-4` 温和降温，其余为震荡持平 |

## 快速开始

要求 Python 3.10 或更高版本。以下命令在仓库根目录执行。

### 1. 安装

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

### 2. 配置

```bash
cp .env.example .env
chmod 600 .env
.venv/bin/retail-tide setup --env-file .env
```

已有 `.env` 时直接运行 `setup`，不要复制模板覆盖现有凭据。更新现有配置可使用：

```bash
.venv/bin/retail-tide setup --env-file .env --force
.venv/bin/retail-tide setup --env-file .env --with-market
```

公共来源 User-Agent 必须包含项目名和有效联系邮箱或项目 URL：

```dotenv
RETAIL_TIDE_HTTP_USER_AGENT='RetailTide/0.1 (team@example.com)'
```

API key、token、Cookie 和 secret 不得提交到 Git。

### 3. 检查配置

```bash
.venv/bin/retail-tide status
```

`status` 返回来源、LLM、行情和单实例锁状态，不发起采集请求，也不输出密钥。

### 4. 初始化数据

```bash
# 最近 30 个上海自然日
.venv/bin/retail-tide refresh --days 30

# 从指定日期到当前时刻
.venv/bin/retail-tide refresh --since 2026-07-01
```

`refresh` 自动创建数据库表，并同步 `config/topics.yaml` 和 `config/assets.yaml`。中断后重跑同一命令即可从 checkpoint 继续。

### 5. 启动服务

```bash
.venv/bin/retail-tide serve --host 127.0.0.1 --port 8000
```

| 页面 | 地址 |
| --- | --- |
| Dashboard | <http://127.0.0.1:8000/dashboard> |
| 趋势与价格 | <http://127.0.0.1:8000/trends> |
| 历史帖子 | <http://127.0.0.1:8000/posts> |
| 研究与溯源 | <http://127.0.0.1:8000/research> |
| 健康检查 | <http://127.0.0.1:8000/health> |

Dashboard 默认显示最近一个已结束的上海自然日。可使用日期选择器或 `/dashboard?date=YYYY-MM-DD` 查看指定日期。

## 来源登录会话（可选）

东方财富或淘股吧要求登录时，可导入本人授权的浏览器会话：

1. 在浏览器中正常登录并完成人机验证。
2. 从开发者工具导出对应请求的 **Copy as cURL**，或导出 storage-state JSON。
3. 将文件权限设为 `600` 后导入：

```bash
chmod 600 /secure/path/taoguba-auth.curl
.venv/bin/retail-tide source auth login taoguba \
  --from-file /secure/path/taoguba-auth.curl
.venv/bin/retail-tide source auth status taoguba

chmod 600 /secure/path/guba-auth.curl
.venv/bin/retail-tide source auth login guba \
  --from-file /secure/path/guba-auth.curl
.venv/bin/retail-tide source auth status guba
```

导入成功后删除临时文件。不要在聊天、Issue 或日志中粘贴 Cookie、cURL 或会话 JSON。退出本地会话：

```bash
.venv/bin/retail-tide source auth logout taoguba
.venv/bin/retail-tide source auth logout guba
```

会话默认保存在 `var/auth/guba.session.json` 和 `var/auth/taoguba.session.json`，不写入数据库，也不进入 Git。可通过 `RETAIL_TIDE_GUBA_SESSION_FILE` 和 `RETAIL_TIDE_TAOGUBA_SESSION_FILE` 修改路径。

## 采集

`refresh` 是统一采集入口：

```bash
# 指定自然日
.venv/bin/retail-tide refresh --date 2026-08-24

# 指定 Topic；--topic 可重复
.venv/bin/retail-tide refresh --date 2026-08-24 --topic gold
```

执行顺序：

1. 并发运行已启用来源；各来源独立限速并串行处理自身任务。
2. 规范化、去重并解析已入库内容。
3. 执行 `Content × Topic` LLM 分析。
4. 计算趋势、指标、事件、行情收益和来源质量。
5. 输出完成状态、告警及待重试任务。

### 日期参数

| 参数 | 范围 |
| --- | --- |
| `--date YYYY-MM-DD` | 指定上海自然日 `[00:00, 次日 00:00)` |
| `--days N` | 包含今天的最近 N 个上海自然日 |
| `--since YYYY-MM-DD` | 指定日期 00:00 至当前时刻 |

运行中的当日窗口在首次执行时冻结。固定窗口按来源和 Topic 保存 cursor、页数和重试时间。

### 来源边界

- 东方财富和淘股吧按新到旧分页，并在本地执行日期过滤。
- 小红书最近单日每个查询最多 1 页；历史窗口最多 20 页，每轮推进 1 页。
- 知乎按 A 股、港股和美股交易日复盘问题采集高互动回答。
- Wikimedia 使用 UTC 日桶，并独立确认上游数据可用性。
- `partial_budget_exhausted` 表示达到分页上限，不代表全量采集。

跨来源并发由 `RETAIL_TIDE_SOURCE_CONCURRENCY` 控制，默认 `5`，范围 `1–8`。同一来源保持单通道，并遵守各自的 `*_MIN_INTERVAL`。

### 完整性与重试

- 原始版本唯一键：`source_id + source_item_id + payload_hash`
- 规范化内容唯一键：`source_id + source_item_id`
- 跨 Topic 命中的同一来源内容仅保存一份，并建立多个 Topic 关联
- 来源任务记录在 `collection_task`；分析任务记录在 `analysis_task`
- 来源失败标记为 `degraded`，并保留 checkpoint
- 定时任务按原日期每小时重试，单个来源最多尝试 6 次
- LLM 失败不影响原始内容入库

### 日志

CLI 默认以 `INFO` 级别向 stderr 写日志，最终 JSON 写入 stdout：

```bash
.venv/bin/retail-tide refresh --date 2026-08-24 --topic gold
.venv/bin/retail-tide --log-level DEBUG refresh --date 2026-08-24 --topic gold
```

日志不包含帖子正文、API key 或认证请求头。

## systemd 部署

标准部署目录为 `/opt/retail-tide`。目录内需要 `.venv` 和权限为 `600` 的 `.env`。

### API 与 Dashboard

```bash
sudo install -m 0644 deploy/retail-tide.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now retail-tide.service
```

```bash
systemctl status retail-tide.service
journalctl -u retail-tide.service -f
sudo systemctl restart retail-tide.service
curl -fsS http://127.0.0.1:8000/health
```

服务默认监听 `127.0.0.1:8000`。远程访问应使用带认证的反向代理或 SSH 端口转发。

### 定时任务

| Timer | 时间 | 任务 |
| --- | --- | --- |
| `retail-tide-posts-yesterday.timer` | 每天 03:00 `Asia/Shanghai` | 采集并分析前一上海自然日，同步行情 |
| `retail-tide-wikimedia-yesterday.timer` | 每天 04:00 UTC | 采集前一 UTC 日的 Wikimedia 数据 |

```bash
sudo install -m 0644 deploy/retail-tide-posts.service /etc/systemd/system/
sudo install -m 0644 deploy/retail-tide-posts-yesterday.timer /etc/systemd/system/
sudo install -m 0644 deploy/retail-tide-wikimedia.service /etc/systemd/system/
sudo install -m 0644 deploy/retail-tide-wikimedia-yesterday.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now retail-tide-posts-yesterday.timer
sudo systemctl enable --now retail-tide-wikimedia-yesterday.timer
systemctl list-timers --all 'retail-tide-posts-*'
systemctl list-timers --all 'retail-tide-wikimedia-*'
```

Timer 使用 `Persistent=true`。停机跨越多个自然日时，按遗漏日期分别运行 `refresh --date`。旧版部署需停用并移除 `retail-tide-posts-today.timer`。

### 小红书部署参考

- [`xiaohongshu-mcp` Linux ARM64 适配记录](docs/xiaohongshu-mcp-arm64.md)
- [noVNC 人工短信验证方案](docs/xiaohongshu-novnc-login.md)

两份文档只说明兼容改动、安全边界和代理必须满足的通用能力，不绑定具体云主机、Compose、systemd 或反向代理产品。

## API

| 接口 | 作用 |
| --- | --- |
| `/health` | 服务健康检查 |
| `/config/status` | 来源、LLM 和行情配置状态 |
| `/sources/status` | 来源健康和质量指标 |
| `/topics` | Active Topic 列表 |
| `/topics/overview` | 市场与 Topic 总览 |
| `/topics/{topic_id}/series` | Topic 内容热度序列 |
| `/topics/{topic_id}/attention` | Topic Wikimedia 关注度序列 |
| `/trends/attention` | 全部 Topic Wikimedia 关注度序列 |
| `/contents` | 全部 Topic 的去重内容及分析结果 |
| `/topics/{topic_id}/contents` | 指定 Topic 的内容及分析结果 |
| `/metrics` | 聚合指标和 baseline signals |
| `/events` | 事件列表 |
| `/research/event-study` | 事件研究 |
| `/research/quantile-study` | 分位研究 |

## LLM 数据分析 Skill

仓库提供只读的 [`retail-tide-analysis`](skills/retail-tide-analysis/SKILL.md) Skill，可按日期、Topic、来源和信号查询平台。

在 Codex 中安装：

```bash
CODEX_SKILLS_DIR="${CODEX_HOME:-$HOME/.codex}/skills"
mkdir -p "$CODEX_SKILLS_DIR"
cp -R skills/retail-tide-analysis "$CODEX_SKILLS_DIR/"
test -f "$CODEX_SKILLS_DIR/retail-tide-analysis/SKILL.md"
```

重启 Codex 或开启新会话后，通过 `$retail-tide-analysis` 调用。生成分析证据包：

```bash
python skills/retail-tide-analysis/scripts/retail_tide_query.py bundle \
  --from-date 2026-08-01 --to-date 2026-08-30 \
  --topic semiconductor --post-limit 100
```

Skill 默认仅调用 HTTP GET API，不读取 SQLite，也不触发采集或分析任务。

## 配置

| 路径 | 内容 |
| --- | --- |
| `.env` | 运行时配置、来源凭据、LLM 和行情认证 |
| `.env.example` | 环境变量模板及说明 |
| `config/topics.yaml` | Topic、别名、来源查询和页数上限 |
| `config/assets.yaml` | 代表资产、市场、Topic 关联和 benchmark |
| `prompts/` | LLM schema 和提示词版本 |
| `migrations/` | 数据库迁移脚本 |

备用 LLM 使用 `RETAIL_TIDE_LLM_FALLBACK_*` 配置。网络、超时、限流、上游 HTTP 或 schema 校验错误会触发切换；合法的 `unknown` 结果不会触发切换。主备模型分别限速，结果按实际模型入库。

模型变更后，可重分析旧模型返回 `unknown` 的内容：

```bash
.venv/bin/retail-tide llm review-compatible \
  --candidate-model OLD_MODEL-via-openai-compatible \
  --candidate-intent unknown
```

## 演示模式

```bash
RETAIL_TIDE_DATA_MODE=demo \
RETAIL_TIDE_DATABASE_URL=sqlite:////tmp/retail-tide-demo.db \
.venv/bin/retail-tide demo run
```

演示数据库应使用 `/tmp` 或其他临时目录，与生产数据库隔离。
