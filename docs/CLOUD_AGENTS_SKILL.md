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

集成测试基类 `StarRocksITTestBase` 会尝试启动 `StarRocksTestEnvironment` 容器；若失败，IT 可跳过。

| 系统属性 | 说明 |
|----------|------|
| `it.starrocks.image` | StarRocks 镜像，默认 `starrocks/allin1-ubuntu:3.5.5` |
| `it.starrocks.platform` | 例如 Apple Silicon：`linux/arm64` |

运行 IT 示例：

```bash
# 使用默认镜像
mvn test -Dtest=StarRocksSinkITTest -DskipTests=false

# Apple Silicon 示例
mvn test -Dtest=StarRocksSinkITTest -Dit.starrocks.platform=linux/arm64 -DskipTests=false
```

IT 需要 Docker 和足够内存（约 8 分钟启动超时）。无容器环境时，IT 会因启动失败而跳过。

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
| JMockit 报错 | 确认 `maven-surefire-plugin` 的 `argLine` 中包含 `-javaagent:.../jmockit-*.jar` |
| SDK 依赖缺失 | 先执行 `cd starrocks-stream-load-sdk && mvn clean install` |
| 端口冲突 | Sink 相关单测会绑定动态端口；IT 使用固定端口 8030、9030、8040 等，避免本机其他服务占用 |

---

## 7. 如何更新本技能文档

当发现新的运行或测试技巧时，按以下方式更新本文件：

1. **按模块归类**：将内容放入对应分区（构建、单元测试、集成测试、环境配置等）。
2. **补充可执行命令**：给出可直接复制的 `mvn` / `build.sh` 命令。
3. **记录系统属性 / 环境变量**：新增的 `-D` 或 `env` 放在「环境与“开关”配置」相关小节。
4. **维护常见问题表**：在「常见问题与快速排查」中增加新情况与解决办法。
5. **简短提交信息**：提交时使用类似 `docs: update Cloud Agents skill with [具体内容]` 的说明。

---

*最后更新：基于当前代码库结构整理*
