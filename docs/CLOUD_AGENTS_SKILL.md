# Cloud Agents Starter Skill: StarRocks Flink Connector

面向 Cloud Agents 的最小运行与测试指南。按代码库分区组织，提供可执行的测试流程与常用环境/工作流说明。

---

## 1. 环境与前置条件

| 要求 | 说明 |
|------|------|
| **JDK** | Java 8 |
| **Maven** | 3.x，支持 `${CUSTOM_MVN}` 覆盖默认命令 |
| **Docker** | 集成测试依赖 Testcontainers，需可用 Docker daemon |
| **Flink** | 支持 1.15–1.20，通过 `build.sh` 指定版本 |

---

## 2. 构建与打包

### 2.1 构建 SDK（必须）

先构建 `starrocks-stream-load-sdk`，主项目依赖其本地 `jar-with-dependencies`：

```bash
cd starrocks-stream-load-sdk
mvn -B -ntp -Dsurefire.forkCount=2 clean install
cd ..
```

### 2.2 构建主项目（不跑测试）

```bash
chmod +x build.sh
./build.sh 1.20
```

输出 JAR：`target/flink-connector-starrocks-*.jar`

### 2.3 构建并运行测试

```bash
./build.sh 1.20 --run-tests
```

CI 中通常使用：`./build.sh ${{ matrix.flink }} --run-tests`（如 1.15–1.20）。

---

## 3. 按模块的测试流程

### 3.1 单元测试（不依赖外部服务）

单元测试使用 JMockit、mock HTTP 服务器，无需 StarRocks 实例。

| 模块 | 路径 | 命令示例 |
|------|------|----------|
| Sink options | `src/test/java/.../table/sink/` | `mvn test -Dtest=StarRocksSinkOptionsTest` |
| Source options | `src/test/java/.../table/source/` | `mvn test -Dtest=StarRocksSourceOptionsTest` |
| Serializers | `src/test/java/.../row/sink/` | `mvn test -Dtest=StarRocksJsonSerializerTest` |
| Merge commit | `src/test/java/.../table/sink/` | `mvn test -Dtest=MergeCommitOptionsTest` |

快速验证单测是否通过：

```bash
mvn test -Dtest=StarRocksSinkOptionsTest,MergeCommitOptionsTest,StarRocksSourceOptionsTest
```

注意：部分用例有 `@Ignore`，如 `StarRocksSinkManagerTest`、`StarRocksStreamLoadVisitorTest`，会被跳过。

### 3.2 集成测试（依赖 StarRocks）

集成测试基类 `StarRocksITTestBase` 支持两种模式：

1. **Testcontainers 模式**：自动启动 StarRocks 容器（需 Docker）
2. **外部集群模式**：通过环境变量或系统属性指定 TSP 等外部集群，避免连接失败

#### 外部集群配置（TSP / 手动部署）

当 `SR_HTTP_URLS` 和 `SR_JDBC_URLS` 已设置时，**跳过 Testcontainers**，直接连接外部 StarRocks：

| 环境变量 | 系统属性（备选） | 说明 |
|----------|------------------|------|
| `SR_HTTP_URLS` | `it.starrocks.http-urls` | FE HTTP 地址，如 `fe_host:8030` |
| `SR_JDBC_URLS` | `it.starrocks.jdbc-urls` | JDBC URL，如 `jdbc:mysql://fe_host:9030` |
| `SR_USERNAME` | `it.starrocks.username` | 用户名（默认 `root`） |
| `SR_PASSWORD` | `it.starrocks.password` | 密码（默认空） |

**TSP 集群示例**（TSP 脚本位于主 StarRocks 仓库 `AGENTS.md`，或从 TSP 控制台获取集群地址）：

```bash
# 1. 申请并等待集群（在主 StarRocks 仓库中执行）
# ./tools/tsp_quick_apply.sh --apply-from 7011
# ./tools/tsp_quick_apply.sh --wait-ready <cluster_name> 900
# ./tools/tsp_quick_apply.sh --get-address <cluster_name>
# 输出: SR_FE=fe_host:9030  → HTTP 用 fe_host:8030，JDBC 用 jdbc:mysql://fe_host:9030

# 2. 在 Flink Connector 仓库中导出环境变量并运行 IT
export SR_HTTP_URLS="<fe_host>:8030"
export SR_JDBC_URLS="jdbc:mysql://<fe_host>:9030"
export SR_USERNAME="root"
export SR_PASSWORD=""
mvn test -Dtest=StarRocksSinkITTest -DskipTests=false
```

#### Testcontainers 模式（需 Docker）

| 系统属性 | 说明 |
|----------|------|
| `it.starrocks.image` | StarRocks 镜像，默认 `starrocks/allin1-ubuntu:3.5.5` |
| `it.starrocks.platform` | 例如 Apple Silicon：`linux/arm64` |

**当 Docker 需 sudo 时**：以 root 运行 Maven 以便访问 Docker socket：

```bash
sudo -E mvn test -Dtest=StarRocksSinkITTest -DskipTests=false
```

`-E` 保留当前环境变量。或用 `sudo env "PATH=$PATH" mvn test ...`。

运行 IT 示例：

```bash
# 使用默认镜像
mvn test -Dtest=StarRocksSinkITTest -DskipTests=false

# Apple Silicon 示例
mvn test -Dtest=StarRocksSinkITTest -Dit.starrocks.platform=linux/arm64 -DskipTests=false
```

IT 需要 Docker 和足够内存（约 8 分钟启动超时）。无容器且未配置外部集群时，IT 会因连接失败而跳过。

### 3.3 Stream Load SDK 测试

```bash
cd starrocks-stream-load-sdk
mvn test
cd ..
```

### 3.4 Checkstyle 与代码质量

Checkstyle 在 `validate` 阶段执行，配置在 `checkstyle.xml`：

```bash
mvn validate
```

或随完整构建自动执行。

---

## 4. 环境与“开关”配置

### 4.1 登录 / 连接（本地开发）

本地 StarRocks 开发环境常用配置：

- HTTP（Stream Load）：`127.0.0.1:8030`
- MySQL JDBC：`jdbc:mysql://127.0.0.1:9030`
- 用户名：`root`，密码：`""`

IT 基类中对应：

```java
HTTP_URLS = "127.0.0.1:8030";
JDBC_URLS = "jdbc:mysql://127.0.0.1:9030";
USERNAME = "root";
PASSWORD = "";
```

### 4.2 常用 Sink 配置项（近似“特性开关”）

| 选项 | 默认 | 用途 |
|------|------|------|
| `sink.semantic` | `at-least-once` | `exactly-once` / `at-least-once` |
| `sink.blackhole` | `false` | 测试用，丢弃写入 |
| `sink.use.new-sink-api` | 视版本 | 启用 FLIP-191 新 Sink API |
| `sink.exactly-once.enable-abort-lingering-txn` | `false` | 自动中止遗留事务 |

测试时可启用 `sink.blackhole=true` 以验证逻辑而不写真实库。

### 4.3 Source 配置

`scan-url`、`jdbc-url`、`database-name`、`table-name`、`username`、`password` 为必填，其余为可选。

---

## 5. 示例与运行

`examples/` 下有 DataStream 示例（如 `LoadJsonRecords`、`LoadCsvRecords`），需先构建主项目并安装到本地仓库。

注意：`examples/pom.xml` 中 connector 版本可能落后于当前开发版本，运行前需确认版本一致。

---

## 6. 常见问题与快速排查

| 现象 | 处理建议 |
|------|----------|
| IT 启动超时 | 检查 Docker 是否运行、镜像拉取是否正常；可尝试 `-Dit.starrocks.image=starrocks/allin1-ubuntu:3.5.5` |
| Docker 需 sudo | 使用 `sudo -E mvn test ...` 以 root 访问 Docker socket；或配置 `SR_HTTP_URLS`/`SR_JDBC_URLS` 使用 TSP 外部集群 |
| IT 连接失败 | 设置 `SR_HTTP_URLS` 和 `SR_JDBC_URLS` 指向 TSP 申请集群，修改默认配置避免连接 127.0.0.1 |
| JMockit 报错 | 确认 `maven-surefire-plugin` 的 `argLine` 中包含 `-javaagent:.../jmockit-*.jar` |
| SDK 依赖缺失 | 先执行 `cd starrocks-stream-load-sdk && mvn clean install` |
| 端口冲突 | Sink 相关单测会绑定动态端口；IT 使用固定端口 8030、9030、8040 等，避免本机其他服务占用 |

---

## 7. 远程机器 + TSP 集群 + Docker 运行集成测试（完整流程）

当本地 Cloud VM 无 Docker 或 JDK 8 时，可在**远程编译节点**上通过 **TSP 申请集群** + **Docker 容器（JDK 8）** 运行集成测试，确保流程可复现。

### 7.1 环境变量（必须提前设置）

| 变量 | 说明 |
|------|------|
| `SSH_HOST` | 远程机器 IP |
| `SSH_USERNAME` | 远程 SSH 用户名 |
| `SSH_PASSWORD` | 远程 SSH 密码 |
| `TSP_HOST` | TSP 服务地址（申请集群用） |
| `TSP_USERNAME` | TSP 登录账号 |
| `TSP_PASSWORD` | TSP 登录密码 |

依赖：`sshpass`（用于非交互式 SSH 传密）。

### 7.2 远程目录与仓库约定

| 路径 | 说明 |
|------|------|
| `/home/disk4/hujie/src/starrocks` | 主 StarRocks 仓库（含 TSP 脚本） |
| `/home/disk4/hujie/src/starrocks-connector-for-apache-flink` | Flink Connector 仓库 |
| `/home/disk4/hujie/.m2` | Maven 本地仓库（可跨构建复用） |

### 7.3 步骤 1：申请 TSP 集群

TSP 脚本位于主 StarRocks 仓库的 `meegoo/starrocks-cluster-quick-apply-b107` 分支：

```bash
REMOTE_SSH="sshpass -p \"\$SSH_PASSWORD\" ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 \$SSH_USERNAME@\$SSH_HOST"

$REMOTE_SSH "cd /home/disk4/hujie/src/starrocks && git fetch origin && git checkout meegoo/starrocks-cluster-quick-apply-b107 2>/dev/null || true"

# 申请集群（示例 7011 对应 hujietest1-4u-benchmark）
$REMOTE_SSH "export TSP_HOST='$TSP_HOST'; export TSP_USERNAME='$TSP_USERNAME'; export TSP_PASSWORD='$TSP_PASSWORD'; cd /home/disk4/hujie/src/starrocks && ./tools/tsp_quick_apply.sh --apply-from 7011"
# 输出示例: 新集群名称: hujietest1-4u-benchmark-03141430

# 获取 FE 地址
$REMOTE_SSH "export TSP_HOST='$TSP_HOST'; export TSP_USERNAME='$TSP_USERNAME'; export TSP_PASSWORD='$TSP_PASSWORD'; cd /home/disk4/hujie/src/starrocks && ./tools/tsp_quick_apply.sh --get-address <cluster_name>"
# 输出示例: SR_FE=172.26.95.231:9030
# 则 HTTP 地址为 172.26.95.231:8030，JDBC 为 jdbc:mysql://172.26.95.231:9030
```

集群申请后需等待状态为 Running；若脚本支持 `--wait-ready`，可先等待再获取地址。

### 7.4 步骤 2：拉取 Connector 最新代码

```bash
$REMOTE_SSH "cd /home/disk4/hujie/src/starrocks-connector-for-apache-flink && git fetch origin && git checkout <branch> && git pull origin <branch>"
```

### 7.5 步骤 3：在 Docker（JDK 8）中运行集成测试

远程节点可能只有 JDK 11/17，无 JDK 8。使用 `maven:3.8-eclipse-temurin-8` 镜像提供 JDK 8 环境，并将 `SR_HTTP_URLS`、`SR_JDBC_URLS` 传入容器：

```bash
FE_HOST="172.26.95.231"   # 从步骤 1 获取的实际 FE 地址

$REMOTE_SSH "sudo docker run --rm \
  -v /home/disk4/hujie/src/starrocks-connector-for-apache-flink:/workspace \
  -v /home/disk4/hujie/.m2:/root/.m2 \
  -e SR_HTTP_URLS=\"${FE_HOST}:8030\" \
  -e SR_JDBC_URLS=\"jdbc:mysql://${FE_HOST}:9030\" \
  -w /workspace \
  maven:3.8-eclipse-temurin-8 \
  bash -c 'cd starrocks-stream-load-sdk && mvn -q -B install -Dmaven.javadoc.skip=true && cd .. && mvn -q -B test -DskipTests=false'"
```

若 FE 地址需动态获取，可先执行 `--get-address` 解析输出，再代入上述命令。

### 7.6 一键脚本示例（伪代码，需替换变量）

```bash
# 1. 申请集群并获取地址
CLUSTER_NAME=$($REMOTE_SSH "export TSP_HOST=... TSP_USERNAME=... TSP_PASSWORD=...; cd /home/disk4/hujie/src/starrocks && ./tools/tsp_quick_apply.sh --apply-from 7011" | grep "新集群名称" | awk '{print $NF}')
SR_FE=$($REMOTE_SSH "export TSP_HOST=...; cd /home/disk4/hujie/src/starrocks && ./tools/tsp_quick_apply.sh --get-address $CLUSTER_NAME" | grep SR_FE | cut -d= -f2)
FE_HOST="${SR_FE%:*}"

# 2. 拉取 Connector 代码
$REMOTE_SSH "cd /home/disk4/hujie/src/starrocks-connector-for-apache-flink && git pull origin \$(git branch --show-current)"

# 3. Docker 运行集成测试
$REMOTE_SSH "sudo docker run --rm \
  -v /home/disk4/hujie/src/starrocks-connector-for-apache-flink:/workspace \
  -v /home/disk4/hujie/.m2:/root/.m2 \
  -e SR_HTTP_URLS=\"${FE_HOST}:8030\" \
  -e SR_JDBC_URLS=\"jdbc:mysql://${FE_HOST}:9030\" \
  -w /workspace \
  maven:3.8-eclipse-temurin-8 \
  bash -c 'cd starrocks-stream-load-sdk && mvn -q -B install -Dmaven.javadoc.skip=true && cd .. && mvn -q -B test -DskipTests=false'"
```

### 7.7 关键要点

- **JDK 8**：Connector 与 Arrow 在 JDK 17+ 上存在反射兼容问题，必须用 JDK 8 运行集成测试。
- **Docker 镜像**：`maven:3.8-eclipse-temurin-8` 内置 JDK 8 与 Maven，首次使用需 `docker pull`。
- **外部集群**：设置 `SR_HTTP_URLS`、`SR_JDBC_URLS` 后，IT 跳过 Testcontainers，直接连接 TSP 集群。
- **sudo**：远程 Docker 若需 root，使用 `sudo docker run`。

---

## 8. 运行主 StarRocks 仓库的 SQL 测试（从 Flink Connector 工作区）

当任务涉及主 StarRocks 仓库中某分支新增的 SQL 测试时，无法直接使用主 StarRocks 仓库的 `run_sql_test_remote.sh`（该脚本依赖 `git branch --show-current` 返回 StarRocks 分支名）。需要手动在远程机器上创建 worktree 并运行测试。

### 8.1 远程目录约定

| 路径 | 说明 |
|------|------|
| `/home/disk4/hujie/cursor/src/starrocks` | cursor 专用 StarRocks 仓库（用于 worktree）|
| `/home/disk4/hujie/cursor/agents/<agent-id>/starrocks` | agent worktree 目录 |

`<agent-id>` 基于当前 Flink Connector 分支名生成，规则：分支名非字母数字字符替换为 `-`，截取前 40 位。  
例：`meegoo/sql-86be` → `meegoo-sql-86be`

### 8.2 完整流程

```bash
# 0. 前置：已申请 TSP 集群（参考第 7 节），FE 地址如 172.26.95.155:9030
FE_HOST="172.26.95.155"
SR_BRANCH="claude/chunk-random-distribution-RfcL7"  # 目标 StarRocks 分支
AGENT_ID="meegoo-sql-86be"  # 当前 Flink Connector 分支名转换
AGENT_DIR="/home/disk4/hujie/cursor/agents/${AGENT_ID}/starrocks"
BASE_REPO="/home/disk4/hujie/cursor/src/starrocks"

# 1. 在远程机器创建 worktree
sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no "$SSH_USERNAME@$SSH_HOST" "
  cd $BASE_REPO && git fetch origin $SR_BRANCH
  if [ ! -d $AGENT_DIR ]; then
    mkdir -p /home/disk4/hujie/cursor/agents/$AGENT_ID
    git worktree add $AGENT_DIR $SR_BRANCH
  else
    cd $AGENT_DIR && git checkout $SR_BRANCH && git pull origin $SR_BRANCH
  fi
"

# 2. 安装 Python 依赖（首次）
sshpass -p "$SSH_PASSWORD" ssh ... "cd $AGENT_DIR/test && pip3 install -r requirements.txt -q"

# 3. 配置测试连接 TSP 集群
sshpass -p "$SSH_PASSWORD" ssh ... "
  cd $AGENT_DIR/test
  cp conf/sr.conf conf/sr_tsp.conf
  sed -i 's/^  host = .*/  host = $FE_HOST/' conf/sr_tsp.conf
  sed -i 's/^  port = .*/  port = 9030/' conf/sr_tsp.conf
  sed -i 's/^  http_port = .*/  http_port = 8030/' conf/sr_tsp.conf
"

# 4. 记录模式（首次运行，生成 R 文件）
sshpass -p "$SSH_PASSWORD" ssh ... "
  cd $AGENT_DIR/test
  python3 run.py --config conf/sr_tsp.conf \
    -d sql/test_random_distribution/T/test_chunk_random_distribution \
    -a sequential -c 1 -r -t 600
"

# 5. 验证模式（对比 R 文件运行）
sshpass -p "$SSH_PASSWORD" ssh ... "
  cd $AGENT_DIR/test
  python3 run.py --config conf/sr_tsp.conf \
    -d sql/test_random_distribution/R/test_chunk_random_distribution \
    -a sequential -c 1 -v -t 600
"
```

### 8.3 注意事项

- **R 文件**：分支中若只有 T 文件（无对应 R 文件），需先用 `-r` 记录模式生成 R 文件，再用 `-v` 验证。
- **BE 代码未部署**：TSP 集群运行的是 `main` 分支代码。若测试依赖 BE 代码修改（如 `tablet_info.cpp`），测试结果反映的是修改前的行为，`compression_ratio_acceptable` 等特性断言可能返回 `0`（未通过）。
- **validate 模式通过条件**：validate 模式比对 R 文件与实际结果，若 R 文件记录的是未修复行为（如 `0`），validate 依然会通过。需确认 R 文件记录的是正确预期值（`1`）才能真正验证特性。
- **worktree 复用**：同一 agent-id 的 worktree 会复用，再次运行时执行 `git pull` 即可。

---

## 9. 如何更新本技能文档

当发现新的运行或测试技巧时，按以下方式更新本文件：

1. **按模块归类**：将内容放入对应分区（构建、单元测试、集成测试、环境配置等）。
2. **补充可执行命令**：给出可直接复制的 `mvn` / `build.sh` 命令。
3. **记录系统属性 / 环境变量**：新增的 `-D` 或 `env` 放在「环境与“开关”配置」相关小节。
4. **维护常见问题表**：在「常见问题与快速排查」中增加新情况与解决办法。
5. **简短提交信息**：提交时使用类似 `docs: update Cloud Agents skill with [具体内容]` 的说明。

---

*最后更新：基于当前代码库结构整理*
