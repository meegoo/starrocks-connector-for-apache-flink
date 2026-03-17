/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.data.load.stream.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tracks per-partition transaction boundary state for multi-table transaction mode.
 *
 * <p>Each source partition independently signals when its current transaction is
 * complete ({@code txnEnd}). A commit is only triggered when <em>all</em> active
 * partitions have reached a transaction boundary AND the configured flush interval
 * has elapsed since the last commit.
 *
 * <p>This ensures that a {@code switchChunk} (which freezes buffered data for commit)
 * never captures a partial source transaction from any partition.
 */
public class PartitionCommitTracker {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionCommitTracker.class);

    enum PartitionState {
        /** Partition has written data but has not yet reached a txnEnd. */
        ACTIVE,
        /** Partition has received txnEnd but its regions have not been switched yet. */
        TXN_END_RECEIVED,
        /** Partition's regions have been switched; waiting for global commit. */
        SWITCHED
    }

    private final Map<Integer, PartitionState> partitions = new LinkedHashMap<>();
    /**
     * Tracks txnEnd signals that arrived while a partition was SWITCHED (during an
     * in-flight commit). These are replayed during {@link #reset()} so the boundary
     * is not lost for the next commit cycle.
     */
    private final Set<Integer> pendingTxnEnd = new HashSet<>();
    /**
     * Tracks partitions that received new writes while SWITCHED. These partitions
     * must NOT be removed during {@link #reset()} and must NOT transition to ACTIVE
     * during the commit cycle (to prevent a second switch that would mix next-
     * transaction data into the current shared label).
     */
    private final Set<Integer> pendingWrites = new HashSet<>();
    private final long commitIntervalMs;
    private volatile long lastCommitTimeMs;

    public PartitionCommitTracker(long commitIntervalMs) {
        this.commitIntervalMs = commitIntervalMs;
        this.lastCommitTimeMs = System.currentTimeMillis();
    }

    /**
     * Called when data is written for a partition.
     *
     * <p>If the partition is SWITCHED (commit in flight), the state is NOT changed
     * to ACTIVE. Doing so would allow a subsequent {@code onTxnEnd} → {@code
     * getReadyToSwitch} → {@code switchChunkForCommit} sequence within the same
     * commit cycle, which would mix next-transaction data into the current shared
     * label. Instead, writes accumulate in the region's active chunk; the partition
     * is recorded in {@link #pendingWrites} so {@link #reset()} retains it as ACTIVE.
     *
     * <p>For all other states (null, ACTIVE, TXN_END_RECEIVED), the partition is
     * (re-)set to ACTIVE so the new transaction data is tracked correctly.
     */
    public synchronized void onWrite(int partition) {
        PartitionState current = partitions.get(partition);
        if (current == PartitionState.SWITCHED) {
            pendingWrites.add(partition);
            LOG.debug("[MultiTxn] onWrite for SWITCHED partition {}, deferring to next cycle", partition);
            return;
        }
        partitions.put(partition, PartitionState.ACTIVE);
        pendingTxnEnd.remove(partition);
    }

    /**
     * Called when a txnEnd marker arrives for a partition.
     *
     * @return {@code true} if the flush interval has elapsed (caller should attempt switch)
     */
    public synchronized boolean onTxnEnd(int partition) {
        PartitionState state = partitions.get(partition);
        if (state == null) {
            // Partition was never registered or was evicted after being idle.
            // Re-register it as TXN_END_RECEIVED so it participates correctly in the
            // next commit cycle; the empty-chunk case is handled gracefully downstream.
            LOG.info("[MultiTxn] txnEnd for unknown/evicted partition {}, re-registering", partition);
            partitions.put(partition, PartitionState.TXN_END_RECEIVED);
            return isIntervalElapsed();
        }
        if (state == PartitionState.ACTIVE) {
            partitions.put(partition, PartitionState.TXN_END_RECEIVED);
        } else if (state == PartitionState.SWITCHED) {
            // Partition is currently in an in-flight commit cycle. Record the txnEnd
            // so reset() can promote it to TXN_END_RECEIVED for the next cycle,
            // preventing the boundary from being lost. If the partition also has
            // pendingWrites, the combined effect in reset() is TXN_END_RECEIVED
            // (the next transaction's data + boundary are both ready).
            pendingTxnEnd.add(partition);
            LOG.debug("[MultiTxn] txnEnd for SWITCHED partition {}, recorded as pending", partition);
        }
        // If already TXN_END_RECEIVED, a second txnEnd accumulates more data into the
        // same commit cycle (N:1 mapping) — no state change needed.
        return isIntervalElapsed();
    }

    /** Marks a partition as switched (its regions have been frozen for commit). */
    public synchronized void markSwitched(int partition) {
        partitions.put(partition, PartitionState.SWITCHED);
        LOG.debug("[MultiTxn] partition {} marked SWITCHED", partition);
    }

    /**
     * Returns partitions that have reached txnEnd but have not yet been switched.
     * Only returns results when the flush interval has elapsed.
     */
    public synchronized List<Integer> getReadyToSwitch() {
        if (!isIntervalElapsed()) {
            return Collections.emptyList();
        }
        List<Integer> ready = new ArrayList<>();
        for (Map.Entry<Integer, PartitionState> entry : partitions.entrySet()) {
            if (entry.getValue() == PartitionState.TXN_END_RECEIVED) {
                ready.add(entry.getKey());
            }
        }
        return ready;
    }

    /** Returns {@code true} when all tracked partitions have been switched. */
    public synchronized boolean allSwitched() {
        if (partitions.isEmpty()) {
            return false;
        }
        for (PartitionState state : partitions.values()) {
            if (state != PartitionState.SWITCHED) {
                return false;
            }
        }
        return true;
    }

    private boolean isIntervalElapsed() {
        return System.currentTimeMillis() - lastCommitTimeMs >= commitIntervalMs;
    }

    /**
     * Resets state after a successful commit.
     *
     * <p>For each SWITCHED partition, the outcome depends on what happened during
     * the commit phase:
     * <ul>
     *   <li>Both {@code pendingWrites} and {@code pendingTxnEnd} → TXN_END_RECEIVED
     *       (next transaction's data and boundary are both ready)</li>
     *   <li>Only {@code pendingWrites} → ACTIVE (has data, awaiting txnEnd)</li>
     *   <li>Only {@code pendingTxnEnd} → TXN_END_RECEIVED (boundary arrived, may
     *       have empty data which is handled gracefully downstream)</li>
     *   <li>Neither → removed from tracking (idle partition; will be re-registered
     *       by {@link #onWrite} if it produces data later)</li>
     * </ul>
     *
     * <p>This prevents idle partitions from permanently blocking
     * {@link #allSwitched()}.
     */
    public synchronized void reset() {
        lastCommitTimeMs = System.currentTimeMillis();

        List<Integer> toRemove = new ArrayList<>();
        for (Map.Entry<Integer, PartitionState> entry : partitions.entrySet()) {
            if (entry.getValue() == PartitionState.SWITCHED) {
                int partition = entry.getKey();
                boolean hasPendingWrites = pendingWrites.contains(partition);
                boolean hasPendingTxnEnd = pendingTxnEnd.contains(partition);

                if (hasPendingTxnEnd) {
                    // txnEnd arrived (possibly with writes) — promote to TXN_END_RECEIVED
                    entry.setValue(PartitionState.TXN_END_RECEIVED);
                    LOG.debug("[MultiTxn] Promoted partition {} to TXN_END_RECEIVED from pending " +
                            "(writes={}, txnEnd={})", partition, hasPendingWrites, hasPendingTxnEnd);
                } else if (hasPendingWrites) {
                    // Only writes, no txnEnd — partition has next-transaction data, awaiting boundary
                    entry.setValue(PartitionState.ACTIVE);
                    LOG.debug("[MultiTxn] Reset partition {} to ACTIVE from pending writes", partition);
                } else {
                    // Idle — no data or boundary arrived during commit
                    toRemove.add(partition);
                }
            }
        }

        for (Integer partition : toRemove) {
            partitions.remove(partition);
            LOG.debug("[MultiTxn] Removed idle SWITCHED partition {} from tracking", partition);
        }

        pendingWrites.clear();
        pendingTxnEnd.clear();
        LOG.info("[MultiTxn] PartitionCommitTracker reset, partitions: {}", partitions);
    }

    public synchronized boolean isEmpty() {
        return partitions.isEmpty();
    }

    @Override
    public synchronized String toString() {
        return "PartitionCommitTracker{" +
                "partitions=" + partitions +
                ", lastCommitTimeMs=" + lastCommitTimeMs +
                ", commitIntervalMs=" + commitIntervalMs +
                '}';
    }
}
