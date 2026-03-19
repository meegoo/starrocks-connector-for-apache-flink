# Multi-Table 集成测试问题定位报告

基于 TSP 集群 (172.26.95.98) 上运行的诊断日志分析。

---

## 1. testMultipleConsecutiveTransactions 失败

### 诊断日志
```
[DIAG testMultipleConsecutiveTransactions] After txn-1 commit (waited 2000ms): 
  orders.size=2, orders=[[1, 100, 10.00, created], [2, 101, 20.00, created]]
```

### 预期 vs 实际
- **预期**：txn-1 提交后仅有 1 行 (order_id=1)，txn-2 数据不应可见
- **实际**：已有 2 行，同时包含 order_id=1 (txn-1) 和 order_id=2 (txn-2)

### 根因分析
**txn-2 的数据被错误地包含在 txn-1 的提交中**。

Source 发射顺序：`txn-1 数据` → `txn-1 txnEnd` → `txn-2 数据`。按设计，txnEnd 应作为边界：只有 txnEnd 之前的数据参与提交。但 txn-2 数据却在 txn-1 提交后已经可见，说明：

1. **可能的记录处理顺序问题**：Source 在 `TXN1_END_LATCH.countDown()` 后几乎同时发射 txn-1 txnEnd 和 txn-2 数据，可能在 Flink 网络/算子中被合并到同一批，导致 sink 在“提交 txn-1”时缓冲区中已含有 txn-2 数据。
2. **Sink 边界语义**：Sink 需保证“提交边界严格在 txnEnd 记录处”——只提交 txnEnd 之前接收到的数据，之后的属于下一事务。

### 建议
- 检查 `DefaultStreamLoadManager` 在处理 txnEnd 时的记录边界划分逻辑
- 检查 Sink 是否在“已收到 txnEnd”与“开始提交”之间又消费了新的数据记录
- 考虑在 Source 中在 txn-1 txnEnd 与 txn-2 数据之间增加短暂 delay，以验证是否为纯时序/批处理导致

---

## 2. testAtomicVisibilityAcrossTables 失败

### 诊断日志
```
[DIAG testAtomicVisibilityAcrossTables] Finished transactions with prefix 'test-atomic-dd9a4e77-': count=2
[DIAG testAtomicVisibilityAcrossTables]   txn[0]: label=test-atomic-dd9a4e77--188df854-3ba6-46c0-b5c5-1d87b0b0d087, status=VISIBLE, txnId=338, coordinator=BE: 172.26.95.99
[DIAG testAtomicVisibilityAcrossTables]   txn[1]: label=test-atomic-dd9a4e77--22a7d0a8-1572-48f0-83b0-8f52972525a2, status=VISIBLE, txnId=337, coordinator=BE: 172.26.95.100
[DIAG testAtomicVisibilityAcrossTables] committedCount=2 (expected 1), orders.size=1, order_items.size=2
```

### 预期 vs 实际
- **预期**：一个多表事务应对应 **1 个** StarRocks 事务（一个共享 label）
- **实际**：`show proc '/transactions/{db}/finished'` 中出现 **2 个** 不同 label 的事务

### 关键发现
两个 label 不同，说明存在两个独立事务：
- `test-atomic-dd9a4e77--188df854-3ba6-46c0-b5c5-1d87b0b0d087` (txnId=338, BE: 172.26.95.99)
- `test-atomic-dd9a4e77--22a7d0a8-1572-48f0-83b0-8f52972525a2` (txnId=337, BE: 172.26.95.100)

多表事务模式要求 orders 和 order_items 使用 **同一个共享 label**，目前显然未满足。

### 根因分析
可能原因包括：

1. **每个表（region）单独生成 label**：在 `SharedTransactionCoordinator` 注入共享 label 之前，各 region 可能因 flush 触发而调用 `labelGenerator.next()` 使用了各自的 label 发送 load。
2. **Age-based flush 在 multi-table 模式仍生效**：若 FlushAndCommitStrategy 中 `age-based commit` 被禁用，但 **flush（发送 HTTP load）** 仍基于时间触发，则各 region 可能在不同时刻 flush，各自生成 label，导致多事务。
3. **两个 region 分配到不同 BE**：coordinator 分别为 BE 172.26.95.99 和 172.26.95.100，可能代表 orders 与 order_items 走了不同的 load 路径，进而使用了不同 label。

### 建议
- 检查 `SharedTransactionCoordinator.begin()` 与 `injectLabel()` 的调用时机：确保在所有 region 首次 flush/load 之前完成共享 label 的生成与注入
- 检查 `TransactionTableRegion` / `DefaultStreamLoadManager` 中，在 multi-table 模式下是否仍存在“未注入 label 时由 region 自行生成 label 并 load”的路径
- 检查 StarRocks `show proc '/transactions/{db}/finished'` 的语义：是否对多表 stream load 一行 per-table 还是 per-transaction，以确认“期望 1 个事务”的断言是否与 FE 行为一致

---

## 3. 总结

| 测试 | 现象 | 根因方向 |
|------|------|----------|
| testMultipleConsecutiveTransactions | txn-1 提交后可见 txn-2 数据 | txnEnd 边界划分或处理顺序导致 txn-2 数据被并入 txn-1 提交 |
| testAtomicVisibilityAcrossTables | 2 个事务而非 1 个共享事务 | 各 region/表单独生成或使用 label，未正确使用 SharedTransactionCoordinator 的共享 label |

两个问题都与 **多表事务下 label 与提交边界** 的管理有关，建议集中审查：
- `DefaultStreamLoadManager.processMultiTableCommit()` 与 `SharedTransactionCoordinator` 的时序
- `TransactionTableRegion` 在 multi-table 模式下何时、如何获取并使用 label
