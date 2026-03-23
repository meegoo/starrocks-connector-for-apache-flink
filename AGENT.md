# AGENT.md — AI Agent 工作说明（Flink Connector for StarRocks）

本文件为 Cloud / Coding Agent 的**持久化技能入口**：执行集成测试与构建时**优先遵循本文**，细节与完整说明以 `docs/` 为准并保持同步更新。

---

## 1. 文档地图（与 `docs/` 协调）

| 文档 | 用途 |
|------|------|
| [docs/CLOUD_AGENTS_SKILL.md](docs/CLOUD_AGENTS_SKILL.md) | 构建、单测、集成测试、TSP / SSH / Docker、常见问题（**权威长文档**） |
| [docs/multi-table-transaction-stream-load-en.md](docs/multi-table-transaction-stream-load-en.md) | 多表事务 Stream Load 设计、配置项、约束（英文） |
| [docs/content/connector-sink.md](docs/content/connector-sink.md) / [connector-source.md](docs/content/connector-source.md) | Sink / Source 能力说明 |

**约定**：集成测试流程、环境变量或 `-Dtest` 清单变更时，须**同时**更新本文件与 `docs/CLOUD_AGENTS_SKILL.md` 中对应小节（见该文档 §8）。

---

## 2. 集成测试：固定前置步骤

### 2.1 必须先安装 Stream Load SDK

根目录测试依赖本地 `starrocks-stream-load-sdk` 的 `jar-with-dependencies`，**任何** `mvn test`（在仓库根目录）前执行：

```bash
cd starrocks-stream-load-sdk && mvn -B install -Dmaven.javadoc.skip=true -DskipTests && cd ..
```

### 2.2 JDK 版本

集成测试与 Arrow 等依赖在 **JDK 8** 下验证；远程节点仅有 JDK 11/17 时，使用 `docs/CLOUD_AGENTS_SKILL.md` §7.5 中的 `maven:3.8-eclipse-temurin-8` 容器运行 Maven。

### 2.3 外部 StarRocks 集群（跳过 Testcontainers）

向测试 JVM 提供 **HTTP + JDBC** 后，`com.starrocks.connector.flink.it.StarRocksITTestBase` 将**跳过** StarRocks Testcontainers，直连现网集群：

| 环境变量 | 系统属性（等价） | 示例 |
|----------|------------------|------|
| `SR_HTTP_URLS` | `it.starrocks.http-urls` | `172.26.95.50:8030` |
| `SR_JDBC_URLS` | `it.starrocks.jdbc-urls` | `jdbc:mysql://172.26.95.50:9030` |
| `SR_USERNAME` | `it.starrocks.username` | 默认 `root` |
| `SR_PASSWORD` | `it.starrocks.password` | 可为空 |

完整表格与 TSP 流程见 [docs/CLOUD_AGENTS_SKILL.md](docs/CLOUD_AGENTS_SKILL.md) §3.2、§7。

---

## 3. 集成测试：按场景选择 `-Dtest`

### 3.1 多表事务 IT

- 类名：`MultiTableTransactionITTest`
- 前置：**StarRocks ≥ 4.0**（多表事务 Stream Load）
- 产品语义与配置见 [docs/multi-table-transaction-stream-load-en.md](docs/multi-table-transaction-stream-load-en.md)

```bash
mvn -B test -DskipTests=false -Dtest=MultiTableTransactionITTest
```

**逐个方法运行**（定位卡住用例）：Surefire 支持方法级过滤。

```bash
mvn -B test -DskipTests=false -Dtest=MultiTableTransactionITTest#testEndToEndMultiPartition
```

**60 秒超时**（避免单测卡死久等）：启用 profile `it-fork-timeout-60s`，由 Surefire 在超时后终止 fork 进程（正常用例应远小于 60s；超时即视为卡住或环境异常）。

```bash
mvn -B test -Pit-fork-timeout-60s -DskipTests=false -Dtest=MultiTableTransactionITTest#testNoFlushBeforeTxnEnd
```

个别方法可能带 `@Ignore`（如 `testCheckpointTriggeredFlushDataIntegrity`），以当前源码为准。

### 3.2 非 multi-table IT（**不含 Kafka**）

下列类**不依赖** `MultiTableTransactionITTest`，且**不**包含 `KafkaToStarRocksITTest`。适用于 **`docker run maven` 且未挂载 Docker socket** 的场景（见 §4）。

```bash
mvn -B test -DskipTests=false \
  -Dtest=StarRocksSinkITTest,StarRocksDynamicTableSinkITTest,StarRocksGenericSinkITTest,StarRocksSourceITTest,StarRocksDynamicTableSourceITTest,StarRocksCatalogTest,FlinkCatalogTest,com.starrocks.connector.flink.it.container.StarRocksITTest
```

### 3.3 含 Kafka 的 IT

- 类：`com.starrocks.connector.flink.it.sink.kafka.KafkaToStarRocksITTest`
- 依赖 **Testcontainers** 启动 Kafka 等容器，需要 **宿主机 Docker API** 对 Maven 进程可用。
- 在「仅 `docker run maven`、不挂载 `/var/run/docker.sock`」时会出现 `Could not find a valid Docker environment`，属预期；须在宿主机跑 Maven，或对 `docker run` **增加** `-v /var/run/docker.sock:/var/run/docker.sock`（及权限策略）后再把该类加入 `-Dtest` 列表。

---

## 4. 远程节点（SSH）+ Docker 内跑 IT（摘要）

与 [docs/CLOUD_AGENTS_SKILL.md](docs/CLOUD_AGENTS_SKILL.md) **§7** 一致。典型环境变量：`SSH_HOST`、`SSH_USERNAME`、`SSH_PASSWORD`；远程仓库路径以实际为准（文档示例：`/home/disk4/hujie/src/starrocks-connector-for-apache-flink`）。

1. `ssh` 到远程后：`git fetch && git checkout <branch> && git pull`。
2. `FE_HOST` 为 FE IP（无协议、无 JDBC 前缀）；HTTP 端口 **8030**，MySQL 协议端口 **9030**。
3. 使用 `sudo docker run`（若远程 Docker 需 root）挂载仓库与 `~/.m2`，传入 `SR_HTTP_URLS` / `SR_JDBC_URLS`，在容器内执行 §2.1 的 SDK 安装 + §3 的 `mvn test`。

可复制模板（将 `FE_HOST`、远程路径、`-Dtest` 替换为实际值）：

```bash
FE_HOST="<fe_ip>"
REMOTE_SSH='sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=60 $SSH_USERNAME@$SSH_HOST'

$REMOTE_SSH "sudo docker run --rm \
  -v /home/disk4/hujie/src/starrocks-connector-for-apache-flink:/workspace \
  -v /home/disk4/hujie/.m2:/root/.m2 \
  -e SR_HTTP_URLS=${FE_HOST}:8030 \
  -e SR_JDBC_URLS=jdbc:mysql://${FE_HOST}:9030 \
  -e SR_USERNAME=root \
  -e SR_PASSWORD= \
  -w /workspace \
  maven:3.8-eclipse-temurin-8 \
  bash -c 'cd starrocks-stream-load-sdk && mvn -B install -Dmaven.javadoc.skip=true -DskipTests && cd .. && mvn -B test -DskipTests=false -Dtest=MultiTableTransactionITTest'"
```

将最后一行 `-Dtest=...` 换成 §3.2 或 §3.3 所列即可切换套件。

---

## 5. Agent 行为检查清单

- [ ] 已读或与任务相关的 `docs/CLOUD_AGENTS_SKILL.md` 小节。
- [ ] 运行根目录 IT 前已 `install` **starrocks-stream-load-sdk**（§2.1）。
- [ ] 使用外部集群时已设置 `SR_HTTP_URLS` + `SR_JDBC_URLS`（或等价系统属性）。
- [ ] 在嵌套 Docker 中未挂载 `docker.sock` 时，**勿**将 `KafkaToStarRocksITTest` 加入 `-Dtest`（§3.3、§4）。
- [ ] 多表事务相关断言失败时，对照 StarRocks 版本与 [docs/multi-table-transaction-stream-load-en.md](docs/multi-table-transaction-stream-load-en.md)。

---

*与 `docs/CLOUD_AGENTS_SKILL.md` 配套使用；二者冲突时以仓库内最新提交为准，并应合并修正。*
