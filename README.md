# 固定内网区域主备 DNS（FastAPI + SQLite + dnslib）

为固定内网区域 `internal.local`（可用 `DNS_ZONE` 修改）提供主备两台权威 DNS：

- **主机 (primary)**：通过 REST API 接收一次提交、最多 8 条 A 记录的**完整区域**；
  仅在整份区域校验并落库成功后才把序号 (serial) 加 1。发布不依赖备用机。
- **备用机 (backup)**：从主机 HTTP 拉取**整份版本**，支持手动同步、后台自动同步和
  版本查询；两机各自使用独立 SQLite 数据库（不同文件/卷）。
- 两机都通过 **UDP 53 风格端口（默认 5053）** 回答区域内 A 查询，每个回答只取自
  **一份不可变的完整版本**快照，绝不混版。

## 设计要点与需求对应

| 需求 | 实现方式 |
| --- | --- |
| 单次提交 ≤8 条 A 记录的完整区域 | `app/models.py`：非空、≤8、仅 IPv4、名称必须在固定区域内、禁止重名 |
| 发布成功才递增序号 | `Storage.next_publish()`：单事务 `BEGIN IMMEDIATE` 内插入新版本并切换 active 指针；校验失败在进事务前返回 422，序号不变 |
| 两机独立数据库 | 各自 `DB_PATH`（compose 中 `primary-data` / `backup-data` 两个卷） |
| 单次回答只用一份完整版本 | `zone_versions` 存不可变整份快照；DNS 解析前一次性读取 active 版本，再在内存中查表回答 |
| 手动同步 + 版本查询 | `POST /api/sync`、`GET /api/version`、`GET /api/zone` |
| 慢传/断传期间继续回答旧版 | 传输失败（连接拒绝、超时、HTTP 错误、坏报文）不触碰 active 版本，旧版本继续服务；同步状态写入 `state` 表 |
| 断开/重启后自动追到最新版 | 备用机启动即拉取一次，之后按 `SYNC_INTERVAL` 后台循环，直到与上游同序号 |
| 迟到旧传输不得回退 | `commit_version()` 拒绝任何 `serial <= active_serial` 的版本（返回 `ignored_stale`/False），事务保护 |
| 任一方失败仍供原版 | 发布失败保留旧 active；同步失败保留旧 active；DNS 与 HTTP 在同一进程内独立运行 |
| 主机发布无需等待备用 | 主机 API 不调用备用机；e2e 中备用停机时发布仍在毫秒级成功 |
| 旧版本回收（主备均可调用） | `POST /api/reclaim`：按**本机实际存储**的版本只保留当前生效版与最近一份旧版，删除其余版本并返回被删序号；备用跳号同步时以已存序号为准（缺号不影响） |
| 回收与发布/同步/查询交错安全 | 回收在单事务内删除且绝不删 active 行；active 快照改为单条 JOIN 原子读取，已选中旧版的查询仍拿到那份完整记录，不会误报 NXDOMAIN 或混入新地址；回收失败回滚、可重试，发布与同步不依赖回收 |
| Docker Compose 双服务 + Swagger | `docker-compose.yml` 起 primary/backup；`http://<host>:8080/docs`、`http://<host>:8081/docs` |

### 数据模型（SQLite，WAL 模式）

```
zone_versions(serial INTEGER PRIMARY KEY, records_json TEXT,
              record_count INTEGER, published_at TEXT)   -- 不可变整份版本
state(key TEXT PRIMARY KEY, value TEXT)
        active_serial      当前生效版本指针
        last_sync_*        备用机最近一次同步状态（at/status/error）
        upstream_serial    最近看到的上游序号
```

版本切换 = 插入新版本行 + 更新 `active_serial` 指针，在同一个事务里提交，
因此外部观察到的永远是“某一份完整版本”，不存在中间态。

版本回收 = 一个事务内删除除 active 与“本机已存的最大旧序号”之外的所有行；
active 行永不删除，active 快照通过 `zone_versions JOIN state` 单语句读取，
回收删除对并发查询表现为原子可见（整行在或不在，不会读到指针指向已删行）。

## 端口与 API

| 角色 | API/Swagger | UDP DNS |
| --- | --- | --- |
| primary | 主机 8080 → 容器 8000 | 主机 5053 → 容器 5053/udp |
| backup  | 主机 8081 → 容器 8000 | 主机 5054 → 容器 5053/udp |

- `GET  /health` 健康检查
- `GET  /api/version` 角色、当前序号、记录数、备用机同步状态
- `GET  /api/zone` 当前完整区域（含 serial、records、published_at）
- `GET  /api/versions` **主备均可**，列出本机数据库实际存储的全部版本序号与当前 active
- `POST /api/zone` **仅主机**，请求体 `{"records":[{"name":"db.internal.local","ip":"10.0.0.11"}]}`，成功返回 201 与新 serial
- `POST /api/sync` **仅备用机**，立即拉取一次，返回 `synced` / `ignored_stale` / `failed`
- `POST /api/reclaim` **主备均可**，版本回收：保留当前区域和最近一份旧区域，删除其余旧版本，返回
  `{"ok":true,"active_serial":N,"retained_serials":[...],"deleted_serials":[...],"deleted_count":K}`；
  无版本可删时 `deleted_serials` 为空数组，可安全重复调用；失败返回 500 且不删任何数据、可重试

DNS 行为：区域内 A 查询命中返回 NOERROR+A；区域内不存在的名称 NXDOMAIN；
区域内非 A 查询（如 AAAA）返回 NOERROR NODATA；区域外查询 REFUSED。

## 运行方式

### Docker Compose（推荐）

```bash
docker compose up --build
# 主机 Swagger:  http://localhost:8080/docs
# 备用 Swagger:  http://localhost:8081/docs

curl -sX POST http://localhost:8080/api/zone -H 'content-type: application/json' \
  -d '{"records":[{"name":"db.internal.local","ip":"10.0.0.11"},
                  {"name":"web.internal.local","ip":"10.0.0.21"}]}'

# 手动同步（后台也会每 3 秒自动同步）
curl -sX POST http://localhost:8081/api/sync

# 版本回收（主备均可；保留当前版 + 最近一份旧版，返回被删序号）
curl -sX POST http://localhost:8080/api/reclaim
curl -sX POST http://localhost:8081/api/reclaim
# 查看本机实际留存的版本序号
curl -s http://localhost:8080/api/versions

# UDP 查询
dig @127.0.0.1 -p 5053 db.internal.local A
dig @127.0.0.1 -p 5054 db.internal.local A   # 备用机
```

### 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv --without-pip   # 或普通 venv
.venv/bin/pip install -r requirements.txt
DNS_ROLE=primary DB_PATH=./data/primary.db DNS_PORT=15053 API_PORT=18000 \
  .venv/bin/python -m uvicorn app.main:app --port 18000
DNS_ROLE=backup  DB_PATH=./data/backup.db  DNS_PORT=15054 API_PORT=18001 \
  PRIMARY_API_URL=http://127.0.0.1:18000 \
  .venv/bin/python -m uvicorn app.main:app --port 18001
```

## 测试

```bash
.venv/bin/python -m pytest tests/test_unit.py tests/test_reclaim_unit.py -q  # 校验/事务/防回退/回收单元测试
.venv/bin/python -I tests/e2e_test.py                # 双进程端到端 30 项
.venv/bin/python -I tests/auto_sync_test.py          # 自动同步/幂等/Swagger 8 项
.venv/bin/python -I tests/slow_transfer_test.py      # 慢传期间旧版持续服务
.venv/bin/python -I tests/reclaim_e2e_test.py        # 回收/跳号/交错压测 18 项
```

端到端覆盖：启动空版本 NXDOMAIN、非法发布不升序号、发布后 UDP 应答、
手动与自动同步、黑洞上游期间旧版持续服务、恢复后自动追平、
迟到旧版本注入被拒且不回退、进程重启持久化与追赶、备用停机时主机照常发布。

回收测试覆盖：保留规则（active + 最近一份旧版）、备用清空库后跳号同步只按已存
版本留存、回收幂等与无版本可删、注入存储故障后事务回滚且重试成功、
发布/回收/UDP 查询多线程交错下 600+ 次应答全部是单版本完整记录（无 NXDOMAIN
误报、无新旧地址混杂）、回收后发布与同步照常工作、回收失败返回 500 且区域保留。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DNS_ROLE` | `primary` | `primary` 或 `backup` |
| `DNS_ZONE` | `internal.local` | 固定内网区域名 |
| `DB_PATH` | `/data/dns.db` | 本机独立 SQLite 文件 |
| `DNS_PORT` / `API_PORT` | `5053` / `8000` | UDP DNS / HTTP 端口 |
| `PRIMARY_API_URL` | `http://primary:8000` | 备用机指向上机 API |
| `SYNC_INTERVAL` | `5` | 备用机后台同步间隔秒 |
| `HTTP_TIMEOUT` | `10` | 拉取区域 HTTP 超时秒 |
| `MAX_RECORDS` | `8` | 单次区域最大 A 记录数 |

## 已知边界

- 仅支持 A 记录与 IPv4（题目范围）；区域固定为一个，区域名通过配置设定。
- 传输为整份区域拉取（无增量/AXFR 分帧）；区域上限 8 条，单份报文很小。
- 回收固定保留两份版本（当前版 + 最近一份旧版），保留份数不可配置；回收是
  按需手动触发的 API，不会在发布或同步时自动执行（发布/同步路径完全不依赖它）。
- 单机内嵌一个 UDP 解析线程与一个 uvicorn 进程；未做水平扩展与 TSIG 鉴权，
  内网部署时应限制 `/api/zone`、`/api/reclaim` 的调用来源。
