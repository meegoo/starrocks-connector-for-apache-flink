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

---

## 六、Issue 3：SST 文件丢失（合并分析）

### 6.1 问题概述

| 项目 | 值 |
|------|------|
| 丢失文件 | `40ca5914-dcd3-4023-912c-a32306b1cc2e.sst`（persistent index SST, ~1.2GB） |
| Tablet | 40569331 (hex: 26B09F3) |
| Table | contact_merge_fields (40451012) |
| Partition | 40569261 (p4) |
| DB | cdp (40429389) — 与 Issue 2 同一 DB |
| 最后正常版本 | v724963, publish 于 02-25 14:33:06 |
| 报错时间 | 02-25 15:37:29 |
| 恢复时间 | 02-25 19:23:09（通过 DROP PERSISTENT INDEX 重建） |

### 6.2 SST 文件的生命周期（代码分析）

SST 文件是 Primary Key 表的持久化索引文件，与 segment 数据文件有不同的管理路径：

**1. SST 在 metadata 中的位置**

SST 文件存储在 `sstable_meta.sstables[]` 中（非 `rowsets[]`）：

```
"sstable_meta": {
    "sstables": [
        {"filename": "40ca5914-...-b1cc2e.sst", "filesize": 1278320278},
        {"filename": "4a9f5efe-...-b39ca7.sst", "filesize": 48220900}
    ]
}
```

**2. SST 何时变为垃圾**

SST 文件通过以下路径移入 `orphan_files`，等待 vacuum 清理：

| 路径 | 代码位置 | 触发条件 |
|------|----------|----------|
| SST compaction | `meta_file.cpp:322-331` `remove_compacted_sst()` | compaction 的 `input_sstables` 被新 SST 替换 |
| PK index 重建 | `txn_log_applier.cpp:201-210` `check_rebuild_index()` | `rebuild_pindex=true` 时，旧 SST 移入 `orphan_files`，`sstable_meta` 清空 |
| 持久化索引类型变更 | `meta_file.cpp:622-640` `_sstable_meta_clean_after_alter_type()` | 从 CLOUD_NATIVE 切到 LOCAL 类型 |
| Full replication | `txn_log_applier.cpp:670-678` | 全量复制时旧 SST 成为孤儿 |
| PK recover | `lake_primary_key_recover.cpp:35` | PK 恢复时直接 `clear_sstable_meta()` |

**3. vacuum 如何删除 SST**

- **路径 A：`collect_garbage_files`** — 从 `orphan_files` 收集（`vacuum.cpp:252-263`）。如果某版本的 metadata 有 SST 在 `orphan_files` 中，vacuum 沿 `prev_garbage_version` 链遍历到该版本时会删除它。
- **路径 B：`datafile_gc`** — `list_data_files` 明确包含 SST（`vacuum.cpp:1093` `is_sst(entry.name)`），而 `check_reference_files` 检查 `sstable_meta`（`vacuum.cpp:1146-1150`）。如果没有存活 metadata 的 `sstable_meta` 引用该 SST，它会被识别为孤儿。

### 6.3 根因分析：与 Issue 2 相同的模式

**SST 文件丢失的机制与 segment 丢失完全一致，只是涉及不同的 metadata 字段。**

#### 场景复现

```
Step 1: 版本 V_old (≤724963) 的 sstable_meta 引用 SST_A

Step 2: SST Compaction 发生（版本 V_comp, 724963 < V_comp < 724966）
  - SST_A 从 sstable_meta 移出，加入 orphan_files
  - 新的 SST_B 加入 sstable_meta
  - prev_garbage_version 指向 V_old（因为 orphan_files 非空）

Step 3: vacuum 运行
  - 沿 prev_garbage_version 链遍历到 V_comp
  - 从 V_comp 的 orphan_files 收集 SST_A → 删除 SST_A 文件
  - 删除 V_comp 的 txn_log
  - 删除中间 metadata 版本

Step 4: Publish 重试（FE 切主导致 visibleVersion 回退）
  - 从 V_old（≤724963）重新 publish
  - SST Compaction 的 txn_log 已被删除
  - force_publish=true → 跳过 SST Compaction
  - 新 metadata 的 sstable_meta 仍引用 SST_A（因为 compaction 被跳过）

Step 5: 02-25 15:37:29 publish 尝试加载 PK index
  - 读取 sstable_meta → 尝试加载 SST_A → 404 Not Found
  - "prepare_primary_index: load primary index failed: Not found"
```

#### 代码层面的关键证据

1. **`remove_compacted_sst` 将旧 SST 移入 `orphan_files`**（`meta_file.cpp:322-331`）：

```cpp
void MetaFileBuilder::remove_compacted_sst(const TxnLogPB_OpCompaction& op_compaction) {
    for (auto& input_sstable : op_compaction.input_sstables()) {
        FileMetaPB file_meta;
        file_meta.set_name(input_sstable.filename());
        file_meta.set_size(input_sstable.filesize());
        file_meta.set_shared(input_sstable.shared());
        _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
    }
}
```

2. **`collect_garbage_files` 无条件删除 `orphan_files` 中的文件**（`vacuum.cpp:252-263`）：

```cpp
for (const auto& file : metadata.orphan_files()) {
    if (retain_info.contains_file(file.name())) {
        continue;
    }
    RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, file.name())));
}
```

3. **`force_publish` 跳过 compaction 时，SST compaction 也被跳过**（`transactions.cpp:387-391`）：

```cpp
if (ignore_txn_log) {
    log_applier->observe_empty_compaction();
    continue;  // SST compaction 的所有效果（包括更新 sstable_meta）都被跳过
}
```

### 6.4 与 Issue 2 的对比

| 维度 | Issue 2 (segment 丢失) | Issue 3 (SST 丢失) |
|------|------------------------|---------------------|
| 丢失文件类型 | segment .dat（数据文件） | persistent index .sst（索引文件） |
| metadata 引用位置 | `rowsets[].segments[]` | `sstable_meta.sstables[]` |
| 变为垃圾的方式 | compaction 后加入 `compaction_inputs` | SST compaction 后加入 `orphan_files` |
| vacuum 删除路径 | `collect_garbage_files` → `compaction_inputs` | `collect_garbage_files` → `orphan_files` |
| 重新引入引用的方式 | publish 重试跳过 data compaction | publish 重试跳过 SST compaction |
| 表现 | 查询 404 | publish 失败（PK index load failed） |
| 同一集群 | ✅ | ✅ |
| 同一次 FE 切主事件 | ✅ | ✅ |
| 同一天暴露 | ✅ (02-25 14:48) | ✅ (02-25 15:37) |

### 6.5 时间线

```
02-24 03:28:09   FE 切主 #1（BDB JE quorum rollback）
...              多次 FE 切主
02-25 02:27:23   FE 切主 #7
02-25 14:33:06   partition 40569261 publish 成功 v724963（SST_A 在 sstable_meta 中）
                 ... SST_A 被错误删除 ...
02-25 14:48:02   Issue 2 暴露：segment 404（另一个 partition 40588770）
02-25 15:37:29   Issue 3 暴露：SST 404（partition 40569261）
02-25 19:23:09   通过 DROP PERSISTENT INDEX 重建后 publish 成功 v724966
                 SST_A 被加入 orphan_files（因为重建后用了新的 index）
```

两个 issue 在 **同一天、相隔不到 1 小时** 暴露，影响 **同一 DB 下不同表的不同 partition**。这进一步证实了系统性根因（FE 切主丢 edit log）。

## 七、统一结论

### 7.1 三个 Issue 的统一根因

```
FE 多次切主（02-24 ~ 02-25）
  └→ BDB JE quorum rollback 丢失 edit log
      ├→ Issue 1: nextVersion 未递增 → 版本重复
      ├→ Issue 2: visibleVersion 回退 → publish 重试跳过 data compaction
      │           → vacuum 已删除的 segment 重新被引用 → 查询 404
      └→ Issue 3: visibleVersion 回退 → publish 重试跳过 SST compaction
                  → vacuum 已删除的 SST 重新被引用 → PK index load 404
```

### 7.2 核心漏洞总结

系统存在一个 **"vacuum-publish 不变量违反"** 问题：

**不变量**：vacuum 删除的文件，不应再被任何后续 metadata 引用。

**违反条件**：
1. compaction（data 或 SST）将旧文件标记为垃圾（`compaction_inputs` 或 `orphan_files`）
2. vacuum 删除旧文件 + txn_log
3. publish 重试从旧 base_version 重建 metadata，跳过 compaction（`force_publish` + txn_log 缺失）
4. 重建的 metadata 引用了已被 vacuum 删除的文件

**触发条件**：
- FE 切主导致 visibleVersion 回退（最常见）
- batch → single publish 模式切换 + FE 响应丢失
- CN 重启 + FE 未更新 visibleVersion

### 7.3 修复方向建议

1. **Publish 层**：当 `force_publish` 跳过 compaction 时，应检查旧文件是否仍存在于 S3。如果不存在，不应使用旧 metadata 的引用。
2. **Vacuum 层**：在删除文件前，验证没有比 `min_retain_version` 更早的 metadata 版本仍引用这些文件（但这会增加 I/O 开销）。
3. **FE 层**：加强 FE 切主后的 visibleVersion 一致性校验，例如从 S3 读取实际最新 metadata version 来校准。
4. **txn_log 清理**：vacuum 不应删除 `force_publish` 事务的 txn_log，或者在删除前检查 txn_log 对应的版本是否已经 safely retained。

---

## 八、修正分析：排除 "Compaction B" 假设

### 8.1 用户反馈

> "Compaction B 执行时已经找不到文件了，说明不是B导致的"

这否定了之前的 "二次 Compaction + vacuum 删除 + publish 重试" 假设。关键逻辑：

**如果 Compaction B 要把 segment_X 作为输入，Compaction B 的执行阶段必须能读到 segment_X 文件。文件已经不存在了，Compaction B 不可能成功执行，因此不可能把 segment_X 移入 `compaction_inputs`。**

### 8.2 重新分析：排除 Compaction B 后的可能删除路径

系统中能删除 segment/SST 文件的所有代码路径：

| # | 删除路径 | 代码位置 | 触发条件 | 适用性 |
|---|---------|---------|---------|--------|
| 1 | `collect_garbage_files` → `compaction_inputs` | `vacuum.cpp:228-249` | 版本的 compaction_inputs 中有该文件 | 需要 compaction 消费过该文件 |
| 2 | `collect_garbage_files` → `orphan_files` | `vacuum.cpp:252-263` | 版本的 orphan_files 中有该文件 | SST 可能（通过 SST compaction），segment 不太可能 |
| 3 | `datafile_gc` (`find_orphan_data_files`) | `vacuum.cpp:1113-1211` | 所有存活 metadata 都不引用该文件 + 超过 expired_seconds | 需要一个 metadata 空窗期 |
| 4 | `abort_txn` → `collect_files_in_log` | `transactions.cpp:541-579` | compaction txn 被 abort → 删除 compaction **输出** segments | 需要 txn 被错误 abort |
| 5 | `delete_tablets` → `delete_files_under_txnlog` | `vacuum.cpp:713-747` | tablet 被删除时，读 txn_log 删除 compaction **输出** | tablet 仍活跃，不适用 |

### 8.3 Issue 3（SST 文件丢失）—— 跨时间线 orphan_files（不需要 Compaction B）

SST 文件与 segment 文件的关键区别：**SST 是 persistent index 文件，生命周期长，可能在 FE 切主之前就已存在。**

#### 可能场景

```
Timeline A（切主前的 old leader）:
  v_old:   sstable_meta = [SST_A]
  v_comp:  SST compaction: SST_A → orphan_files, SST_B → sstable_meta
           prev_garbage_version = v_old（因 orphan_files 非空）

FE 切主 → visibleVersion 回退到 v_old 之前

Timeline B（new leader）:
  new v_old:  sstable_meta = [SST_A]（继承自回退的 base_version，SST compaction 没发生）
  new v_old+1: ...仍引用 SST_A...

S3 上混合状态:
  v_old ~ v_comp-1: new leader 的 metadata（SST_A 在 sstable_meta）
  v_comp+: old leader 的 metadata（SST_A 在 orphan_files）
```

当 vacuum 运行时：
1. 沿 `prev_garbage_version` 链到达 v_comp（old leader 的）
2. 从 v_comp 的 `orphan_files` 收集 SST_A → **删除 SST_A**
3. 但 new leader 的 metadata 仍在 `sstable_meta` 中引用 SST_A → 404

**此场景不需要 Compaction B。** old leader 时间线上的 SST compaction 就是直接原因。

#### 验证方式

- 确认 SST_A 的创建时间是否在 FE 切主之前
- 在 old leader 的日志中搜索是否有 SST compaction 记录
- 检查 `prev_garbage_version` 链是否跨越了时间线

### 8.4 Issue 2（segment 文件丢失）—— 待进一步排查

这是更难解释的 case。segment `0000000003cfb698_87198a2e-...` 由 Compaction A (txn 63944344) 在 **02-25 03:16** 创建，发生在最后一次 FE 切主（02-25 02:27）之后。因此：

- **old leader 从未拥有过此 segment** → 跨时间线 compaction_inputs 路径不适用
- **Compaction B 无法成功执行** → 二次 Compaction 路径不适用

#### 候选路径 1：`abort_txn` 删除 compaction 输出

`abort_txn` 中的 `collect_files_in_log`（`transactions.cpp:551-558`）会删除 compaction 的 **输出** segments：

```cpp
if (txn_log.has_op_compaction()) {
    size_t new_segment_offset = txn_log.op_compaction().new_segment_offset();
    size_t new_segment_count = txn_log.op_compaction().new_segment_count();
    const auto& segments = txn_log.op_compaction().output_rowset().segments();
    for (size_t idx = new_segment_offset, cnt = 0; ...) {
        files_to_delete->emplace_back(tablet_mgr->segment_location(tablet_id, segments[idx]));
    }
}
```

**如果 Compaction A 的 txn 被错误 abort，其输出 segment 会被直接删除。**

可能触发条件：
- FE 切主后，new leader 不知道此 txn 已 VISIBLE（VISIBLE edit log 丢失）
- New leader 看到此 txn 处于 COMMITTED 状态，但 publish 失败
- 某种条件触发 abort（超时、冲突等）
- `LakeTableTxnStateListener.postAbort()` 发送 `AbortTxnRequest` 到 CN
- CN 的 `abort_txn()` 读取 txn_log，删除 compaction 输出 segment

**但存在疑点**：Compaction A 在 03:16 成为 VISIBLE，这是在最后一次 FE 切主（02:27）之后。除非有未记录的 FE 切主/重启。

**验证方式**：在 FE/CN 日志中搜索 `abort.*63944344` 或 `abort.*3cfb698`。

#### 候选路径 2：vacuum metadata 删除 + datafile_gc 时间窗口

1. Vacuum 删除中间 metadata 版本（v787637 到 v791755 之间）
2. 在某个瞬间，S3 上没有 metadata 引用此 segment（取决于 new/old leader metadata 的覆盖情况）
3. datafile_gc 恰好在此窗口运行，识别 segment 为孤儿
4. 但 `expired_seconds` 保护应该防止这种情况（文件只创建了几小时）

**验证方式**：检查 `lake_datafile_gc_expired_seconds` 配置值。

#### 候选路径 3：跨时间线 metadata 覆盖

虽然 Compaction A 在 FE 切主后执行，但如果之前的 FE 切主导致此 partition 的 visibleVersion 已经混乱：

1. 02-24 的某次 FE 切主导致此 partition 的 visibleVersion 低于实际
2. New leader 从旧 base 开始 publish，覆盖了 S3 上的部分 metadata
3. 后来 Compaction A 执行并 publish（v787637）
4. 但 vacuum 基于的 `prev_garbage_version` 链可能经过了 被覆盖的旧 metadata
5. 旧 metadata 的 `compaction_inputs` 中可能有 **不同的** compaction 留下的垃圾文件条目，这些条目恰好包含与此 segment 同名的文件？

这种可能性极低，因为 segment 文件名包含 txn_id 前缀，不会重复。

### 8.5 修正后的结论

| Issue | 最可能的删除路径 | 置信度 | 需要进一步验证 |
|-------|-----------------|--------|---------------|
| Issue 2 (segment) | `abort_txn` 删除 compaction 输出 | 中 | 搜索 abort 日志 |
| Issue 2 (segment) | 跨时间线 metadata 覆盖 + datafile_gc | 低 | 检查 expired_seconds 配置 |
| Issue 3 (SST) | 跨时间线 orphan_files（old leader 的 SST compaction） | 高 | 确认 SST 创建时间和 old leader SST compaction 记录 |

**Issue 3 的解释不依赖 "Compaction B"**，直接通过跨时间线的 orphan_files 即可解释。

**Issue 2 的确切删除路径仍不完全明确**，`abort_txn` 是当前最可疑的路径，但需要日志验证。

### 8.6 关键验证建议

1. **在 FE 日志中搜索 Compaction A (txn 63944344) 是否有被 abort 的记录**：
   ```
   grep "63944344" fe.log* | grep -i abort
   ```

2. **在 CN 日志中搜索 abort_txn 调用**：
   ```
   grep "abort_txn.*63944344\|abort_txn.*3cfb698" cn.log*
   ```

3. **检查 FE 切主后是否有其他未记录的 FE 重启**：
   ```
   grep "notify new FE type" fe.log* | grep "2026-02-25"
   ```

4. **检查 datafile_gc 的 expired_seconds 配置值**

5. **确认 SST 文件 `40ca5914-...-b1cc2e.sst` 的 S3 创建时间**（是否在 02-24 FE 切主之前）
