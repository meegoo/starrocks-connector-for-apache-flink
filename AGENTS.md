# Agent 说明：Flink Connector 集成测试与 TSP 集群

本文档补充仓库内自动化/Agent 在**远程节点 + TSP StarRocks** 上跑集成测试时的要点。更完整的构建与模块说明见 [`docs/CLOUD_AGENTS_SKILL.md`](docs/CLOUD_AGENTS_SKILL.md)。

---

## 1. 外部集群配置（必读）

部分分支（例如 `meegoo/-bc-a74d6be4-03ad-4c55-a118-f6f57ff1d7aa-4924` 及之后合并了同类改动的代码）中，`StarRocksITTestBase` **仅通过 JVM 系统属性**识别外部 FE，**不再读取**环境变量 `SR_HTTP_URLS` / `SR_JDBC_URLS`。

| 用途 | JVM 系统属性（Maven 用 `-D` 传入） | 示例值 |
|------|-----------------------------------|--------|
| FE HTTP（Stream Load 等） | `it.starrocks.fe.http` | `172.26.95.50:8030` |
| FE MySQL/JDBC | `it.starrocks.fe.jdbc` | `jdbc:mysql://172.26.95.50:9030` |
| 可选：用户名 | `it.starrocks.username` | `root` |
| 可选：密码 | `it.starrocks.password` | （空或实际密码） |

在 `mvn` 中示例：

```bash
mvn test -DskipTests=false \
  -Dit.starrocks.fe.http=172.26.95.50:8030 \
  -Dit.starrocks.fe.jdbc=jdbc:mysql://172.26.95.50:9030
```

若未设置上述属性，基类会尝试通过 **Testcontainers** 启动本机 StarRocks 镜像；启动超时可达约 **8 分钟**，日志较少时易被误判为「执行卡住」。

**兼容性提示**：旧文档或脚本里仅用 `export SR_HTTP_URLS=...`、`SR_JDBC_URLS=...` 时，在上述分支上对 `StarRocksITTestBase` 子类**无效**，需改为 `-Dit.starrocks.fe.http` / `-Dit.starrocks.fe.jdbc`，或写入 `MAVEN_OPTS`。

---

## 2. 「卡住」时的最小化定位步骤

按顺序执行，便于区分网络、配置与用例问题：

1. **网络探测（带超时，避免真挂死）**  
   在将要运行 Maven 的同一环境执行（宿主机或 Docker 内各测一次更稳妥）：

   ```bash
   timeout 3 bash -c '</dev/tcp/<FE_HOST>/9030' && echo ok_9030 || echo fail_9030
   timeout 3 bash -c '</dev/tcp/<FE_HOST>/8030' && echo ok_8030 || echo fail_8030
   ```

2. **最小 Maven 验证（单类或单方法 + 正确 `-D`）**  
   先安装 SDK，再只跑一个 IT 方法，确认连的是 TSP 而非 Testcontainers：

   ```bash
   cd starrocks-stream-load-sdk && mvn -B -ntp install -Dmaven.javadoc.skip=true -DskipTests && cd ..
   mvn -B -ntp test -DskipTests=false \
     -Dit.starrocks.fe.http=<FE_HOST>:8030 \
     -Dit.starrocks.fe.jdbc=jdbc:mysql://<FE_HOST>:9030 \
     -Dtest=StarRocksSinkITTest#testDupKeyWriteFullColumnsInOrder
   ```

3. **若未传 `it.starrocks.fe.*`**  
   优先怀疑正在拉镜像/起 Testcontainers，等待时间过长属预期；应回到步骤 2 用属性固定外部集群后再判失败原因。

---

## 3. 远程 Docker（JDK 8）+ TSP 示例

与 `docs/CLOUD_AGENTS_SKILL.md` 中一致：在远程节点用 `maven:3.8-eclipse-temurin-8`，挂载仓库与 `~/.m2`，并把 **`-Dit.starrocks.fe.http` / `-Dit.starrocks.fe.jdbc`** 传给 `mvn`（不要仅依赖容器内的 `SR_*` 环境变量，除非代码分支仍支持）。

示例（将 `<FE_HOST>` 换为 TSP 返回的 FE IP）：

```bash
sudo docker run --rm \
  -v /path/to/starrocks-connector-for-apache-flink:/workspace \
  -v /path/to/.m2:/root/.m2 \
  -w /workspace \
  maven:3.8-eclipse-temurin-8 \
  bash -c 'set -e; cd starrocks-stream-load-sdk && mvn -B -ntp install -Dmaven.javadoc.skip=true -DskipTests && cd .. && mvn -B -ntp test -DskipTests=false \
    -Dit.starrocks.fe.http=<FE_HOST>:8030 \
    -Dit.starrocks.fe.jdbc=jdbc:mysql://<FE_HOST>:9030'
```

Kafka 相关 IT（`KafkaTableTestBase`）仍依赖 Testcontainers 起 Kafka，且 StarRocks 侧行为以对应基类实现为准；全量 `mvn test` 前建议确认 Docker 与网络是否满足该类用例。

---

## 4. TSP 与地址约定（7011 模板）

通过主 StarRocks 仓库中的 `tsp_quick_apply.sh`（如 `--apply-from 7011`）申请集群后，用 `--get-address <cluster_name>` 取得 `SR_FE=host:9030`：

- JDBC：`jdbc:mysql://<host>:9030` → 对应属性 `it.starrocks.fe.jdbc`
- HTTP：`http://<host>:8030` → 地址部分 `<host>:8030` → 对应属性 `it.starrocks.fe.http`

集群名与 FE 以 TSP 脚本实际输出为准。

---

*若基类再次支持 `SR_HTTP_URLS` / `SR_JDBC_URLS`，以 `StarRocksITTestBase` 源码为准，并建议同步更新本节。*
