# Metadata Vacuum Data Loss 根因分析评估

## 一、原始根因分析的评估

### 1.1 分析中正确的部分

#### (a) FE 切主导致 edit log 丢失 — **已被 Issue 1 确认**

Issue 1（重复版本问题）已经证实：BDB JE quorum rollback 在 02-24 03:28:09 的 FE 切主期间丢失了 COMMITTED edit log。这一事实确立了整条因果链的起点。

#### (b) S3 上 metadata 链可以跨时间线 — **代码层面可证实**

在 `transactions.cpp` 的 `publish_version` 中：

```cpp
// line 367-371
if (base_metadata->compaction_inputs_size() > 0 || base_metadata->orphan_files_size() > 0 ||
    base_metadata->version() - base_metadata->prev_garbage_version() >=
            config::lake_max_garbage_version_distance) {
    new_metadata->set_prev_garbage_version(base_metadata->version());
}
```

`prev_garbage_version` 指向 `base_metadata` 的 version。如果 FE 切主导致 visibleVersion 回退，新 leader 从旧的 base_version 开始 publish，就会在 S3 上产生不同 leader 写入的 metadata。`prev_garbage_version` 链会跨越这些不同时间线的 metadata。

#### (c) vacuum 的 `collect_garbage_files` 盲信 `compaction_inputs` — **代码层面确认**

`vacuum.cpp` 第 228-264 行 `collect_garbage_files`：

```cpp
for (const auto& rowset : metadata.compaction_inputs()) {
    // ... 直接从 compaction_inputs 收集文件删除，不检查其他版本是否仍引用
    for (int i = 0; i < rowset.segments_size(); ++i) {
        RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, rowset.segments(i))));
    }
}
```

vacuum 在遍历 `prev_garbage_version` 链时，对每个版本的 `compaction_inputs` 无条件收集为垃圾文件。**不会交叉验证这些文件是否仍被其他版本的 `rowsets[]` 引用**。

#### (d) `force_publish` 跳过缺失 txn_log 的 compaction — **代码层面确认**

`transactions.cpp` 第 338-339 行：

```cpp
} else if (txns[i].force_publish()) {
    ignore_txn_log = true;
}
```

compaction 事务的 `force_publish` 为 true。当 txn_log 被 vacuum 删除后，如果 publish 重试，compaction 会被静默跳过。第 387-391 行：

```cpp
if (ignore_txn_log) {
    LOG(INFO) << "txn_log of txn: " << txns[i].txn_id() << " for tablet: " << tablet_info
              << " not found, force publish is on, ignore txn log";
    log_applier->observe_empty_compaction();
    continue;  // 跳过这个 compaction，不修改 metadata
}
```

### 1.2 分析中需要修正的部分

#### (a) 丢失的 segment 是 compaction 的 **输出** 而非输入

原始分析的 "Scenario 1: metadata 链跨时间线" 过于简化。Claude Code 描述的是 "old leader 的 compaction 标记文件为垃圾，但 new leader 的 metadata 仍引用"。但实际情况更复杂：

丢失的 segment `0000000003cfb698_87198a2e-9807-4430-b59a-0bfe24897398.dat` 是 Compaction A (txn 63944344) 的 **输出产物**（被添加到 `rowsets[]`），而不是输入（不在 `compaction_inputs` 中）。

这意味着：这个 segment 不可能直接被 Compaction A 所在版本的 `compaction_inputs` 删除。**必须有一次后续的 Compaction B 将此 segment 作为输入**，才能将其移入 `compaction_inputs`。

#### (b) "二次 Compaction + txn_log 丢失 + Publish 重试" 才是精确描述

Claude Code 后续的分析（"Step 1-5" 场景）实际上已经正确描述了这一机制，但与 "Scenario 1" 的表述不一致。精确的场景链应该是：

1. Compaction A 创建 segment → `rowsets[]` 中有 segment
2. **Compaction B 将 segment 作为输入 → `compaction_inputs` 中有 segment**
3. Vacuum 删除 segment 文件 + Compaction B 的 txn_log
4. Publish 重试跳过 Compaction B → segment 重新出现在 `rowsets[]`
5. 查询读取 segment → 404

这个链条中，第 4 步是关键。以下分析集中在第 4 步如何发生。

## 二、Publish 重试的触发机制分析

### 2.1 可能触发 publish 重试的路径

#### 路径 1：FE 切主导致 visibleVersion 回退（最可能）

FE 在 02-24 发生了多次切主（03:28, 03:29, 06:09, 06:10, 17:00, 17:01），在 02-25 还有一次切主（02:27）。每次切主都可能丢失部分 edit log。

如果某次切主导致此 partition 的 `visibleVersion` 回退到 Compaction B 之前的版本（假设回退到 V_old），后续 publish 会以 V_old 为 base_version 重新构建 metadata。

FE 代码中（`PublishVersionDaemon.java` 第 921-924 行）：

```java
if (txnState.getSourceType() != TransactionState.LoadJobSourceType.REPLICATION &&
        partition.getVisibleVersion() + 1 != txnVersion) {
    return false;
}
baseVersion = partition.getVisibleVersion();
```

如果 `partition.getVisibleVersion()` 低于实际 S3 上的最新版本，FE 会用这个旧的 baseVersion 发送 publish 请求。

#### 路径 2：batch → single publish 模式切换

代码注释（`transactions.cpp` 第 273-283 行）明确提到这个风险：

```cpp
// Do not delete txn logs if txns_size != 1, let vacuum do the work
// If the txn logs are deleted, it will be tricky to handle the situation of batch publish switching to single.
// for example:
// 1. the mode of publish is batch,
// 2. txn2 and txn3 have been published successfully and visible version in FE is updated to 3,
// 3. then txn4 and txn5 are published successfully in BE and the txn_log of txn4 and txn5 have been deleted,
// but FE do not get the response for some reason,
// 4. turn the mode of publish to single,
// 5. txn4 will be published in later publish task, but we can't judge what's the latest_version in BE
//    and we can not reapply txn_log if txn logs have been deleted.
```

如果 batch publish 成功但 FE 未收到响应，然后切换到 single publish 模式，已被 vacuum 删除的 txn_log 会导致 compaction 被跳过。

#### 路径 3：CN 节点重启

CN 重启后丢失内存缓存。如果 FE 的 visibleVersion 因之前的响应丢失而停留在 Compaction B 之前，batch publish 会从旧 base_version 开始，触发上述场景。

### 2.2 时间线关联

```
02-24 03:28:09   FE 切主 #1（BDB JE quorum rollback, edit log 丢失）
02-24 03:29:36   FE 切主 #2
02-24 06:09-06:10 FE 切主 #3, #4
02-24 17:00-17:01 FE 切主 #5, #6
02-25 02:27:23   FE 切主 #7（10.68.171.246 成为 LEADER）
02-25 03:07:23   Compaction A 开始（txn 63944344）
02-25 03:15:26   Compaction A COMMITTED (version=787637)
02-25 03:16:13   Compaction A VISIBLE
...             （可能有 Compaction B）
02-25 ~12:53     Vacuum 运行：hasError=true, vacuumVersion=791754
                 → 可能在此之前已经删除了 segment 文件
02-25 14:48:02   查询失败：segment 404 Not Found
```

版本 787637 到 791754 之间有 4117 个版本，在几小时内完成。考虑到该 partition 的版本增长速度（从 01-10 到 02-26 共 7502 个 metadata 版本），Compaction B 完全可能在 03:16 到 12:53 之间发生。

## 三、其他可能性分析

### 3.1 datafile_gc（孤儿文件检测）误删 — **不太可能但有条件**

`vacuum.cpp` 的 `find_orphan_data_files`（第 1113-1211 行）扫描所有存活的 metadata 文件，从 `rowsets()` 构建引用集合。不在集合中的 segment 被识别为孤儿。

关键代码：

```cpp
auto check_reference_files = [&](const TabletMetadataPtr& check_meta) {
    for (const auto& rowset : check_meta->rowsets()) {
        for (const auto& segment : rowset.segments()) {
            data_files.erase(segment);  // 从候选列表中移除
        }
    }
};
```

**如果在 datafile_gc 扫描 metadata 时，所有引用此 segment 的 metadata 恰好都被 vacuum 删除了**，而 publish 重试尚未创建新的引用，那 datafile_gc 会将其识别为孤儿并删除。

但 datafile_gc 有 `expired_seconds` 保护：只有 mtime 超过阈值的文件才会被列为候选。由于该 segment 在 02-25 03:16 创建，如果 `expired_seconds` 设置合理（如 > 12 小时），02-25 12:53 的 vacuum 运行时该文件不应该满足过期条件。

**结论：如果 expired_seconds 设置较短，datafile_gc 可能参与文件删除，但不是主要路径。**

### 3.2 `vacuum_txn_log` 过早清理 txn_log — **是级联因素，非直接原因**

`vacuum.cpp` 第 545-591 行的 `vacuum_txn_log` 删除 `txn_id < min_active_txn_id` 的所有 txn_log。

从 vacuum 日志看：`minActiveTxnId=64251774`，而 Compaction A 的 txn_id=63944344 < 64251774。如果 Compaction B 的 txn_id 也小于 64251774，其 txn_log 会被删除。这是 publish 重试时 compaction 被跳过的前提条件。

**结论：txn_log 清理是必要条件但非充分条件。没有 txn_log 清理，publish 重试仍能正确应用 compaction。**

### 3.3 S3 数据一致性问题 — **极不可能**

AWS S3 提供强一致性（2020 年后）。segment 文件一旦写入成功，不会自发消失。排除此可能性。

### 3.4 `delete_tablets` 路径 — **不适用**

`vacuum.cpp` 的 `delete_tablets_impl`（第 750-995 行）会在删除 tablet 时，读取 txn_log 并删除 compaction 输出的 segment：

```cpp
if (log.has_op_compaction()) {
    const auto& op = log.op_compaction();
    for (int i = 0; i < op.output_rowset().segments_size(); ++i) {
        RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, op.output_rowset().segments(i))));
    }
}
```

但该 tablet 仍然存在且活跃，不适用此路径。

### 3.5 vacuum `collect_files_to_vacuum` 中间 metadata 删除 + 竞态 — **辅助路径**

`vacuum.cpp` 第 432-443 行：

```cpp
for (auto v = version + 1; v < final_retain_version; v++) {
    if (retain_info.contains_version(v)) {
        continue;
    }
    RETURN_IF_ERROR(metafile_deleter->delete_file(
        join_path(meta_dir, tablet_metadata_filename(tablet_id, v))));
}
```

vacuum 在遍历完 `prev_garbage_version` 链后，删除链跳跃之间的所有中间 metadata。如果这些被删除的 metadata 是引用目标 segment 的版本，它们消失后：
- 后续 datafile_gc 扫描不到它们 → segment 可能被识别为孤儿
- 但这要求 datafile_gc 与 vacuum 有时间窗口重叠

## 四、综合评估

### 4.1 最可能的根因（置信度：高）

**"二次 Compaction + vacuum 删除 txn_log + FE 切主导致 publish 重试跳过 compaction"**

完整因果链：

```
FE 多次切主（02-24 ~ 02-25）
  → BDB JE quorum rollback 丢失部分 edit log
    → 某些 partition 的 visibleVersion / nextVersion 不一致
      → Compaction A 创建 segment（v787637）
        → Compaction B 将 segment 作为输入（v787637 ~ v791754 之间）
          → vacuum 运行：
            1. 沿 prev_garbage_version 链收集 Compaction B 的 compaction_inputs → 删除 segment 文件
            2. 删除 Compaction B 的 txn_log（txn_id < min_active_txn_id）
            3. 删除中间 metadata 版本
              → FE 因某种原因重新 publish（visibleVersion 回退 / batch→single 切换 / CN 重启）
                → 从 Compaction B 之前的 base_version 开始 publish
                  → Compaction B 的 txn_log 缺失 + force_publish=true → 跳过
                    → 新 metadata 的 rowsets[] 仍引用已删除的 segment
                      → 查询 404 Not Found
```

### 4.2 辅助验证建议

1. **检查 v791756 (C14CC) metadata 的 rowsets[]**：
   - 如果 **包含** 丢失的 segment → 说明在 vacuum 删除 segment 之后，publish 重试重新引入了引用
   - 如果 **不包含** → 说明 segment 是在更晚的 publish 中重新引入的

2. **在 CN 日志搜索 force_publish 跳过记录**：
   ```
   grep "not found, force publish is on, ignore" cn.log | grep 26B5649
   ```

3. **检查是否存在从旧 base_version 的 publish**：
   ```
   grep "Base version has been adjusted" cn.log | grep 26B5649
   ```

4. **检查 Compaction B 是否存在**：
   在 v787637 到 v791756 之间的某个版本（如果有幸存的 metadata），查看其 `compaction_inputs` 是否包含丢失的 segment。

5. **检查 FE 切主后 visibleVersion 变化**：
   在 FE 日志中搜索 partition 40588770 的 visibleVersion 记录，确认是否有回退。

### 4.3 原始分析的准确度评分

| 维度 | 评分 | 说明 |
|------|------|------|
| FE 切主作为根因触发器 | ✅ 正确 | 已被 Issue 1 确认 |
| metadata 链跨时间线 | ⚠️ 部分正确 | 机制描述正确，但具体场景过于简化 |
| vacuum 错误删除文件 | ✅ 正确 | 代码层面可证实 |
| 二次 Compaction + publish 重试 | ✅ 正确 | 这是最可能的精确路径 |
| txn_log 过早清理 | ✅ 正确 | 是必要的级联因素 |
| datafile_gc 误删 | ⚠️ 有条件 | 取决于 expired_seconds 配置和时间窗口 |
| Issue 1 和 Issue 2 的关联 | ✅ 正确 | 同一次 FE 切主的不同表现形式 |

**总体评价：原始分析的方向正确，核心结论可信，但 "Scenario 1" 的描述过于简化，实际路径更可能是 "二次 Compaction + vacuum 删除 + publish 重试" 的组合。两个 Issue 确实由同一个根因（FE 切主丢 edit log）触发，但通过不同路径表现。**
