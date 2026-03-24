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
    private final long commitIntervalMs;
    private volatile long lastCommitTimeMs;

    public PartitionCommitTracker(long commitIntervalMs) {
        this.commitIntervalMs = commitIntervalMs;
        this.lastCommitTimeMs = System.currentTimeMillis();
    }

    /**
     * Called when data is written for a partition. Sets the partition to ACTIVE
     * unless it is already SWITCHED (part of an in-flight commit cycle).
     *
     * <p>When a partition is SWITCHED, its previous transaction's data has been
     * frozen via {@code switchChunkForCommit()} and a commit is pending. New
     * writes go into the fresh active chunk created by the switch, so there is
     * no risk of mixing transactions. Resetting to ACTIVE here would lose the
     * SWITCHED state and prevent the pending commit from completing.
     */
    public synchronized void onWrite(int partition) {
        PartitionState state = partitions.get(partition);
        if (state == PartitionState.SWITCHED) {
            return;
        }
        partitions.put(partition, PartitionState.ACTIVE);
        pendingTxnEnd.remove(partition);
    }

    /**
     * Called when a txnEnd marker arrives for a partition.
     *
     * @return {@code true} if the partition transitioned to TXN_END_RECEIVED and
     *         should be switched immediately by the caller
     */
    public synchronized boolean onTxnEnd(int partition) {
        PartitionState state = partitions.get(partition);
        if (state == null) {
            // Partition was never registered or was evicted after being idle.
            // Re-register it as TXN_END_RECEIVED so it participates correctly in the
            // next commit cycle; the empty-chunk case is handled gracefully downstream.
            LOG.info("[MultiTxn] txnEnd for unknown/evicted partition {}, re-registering", partition);
            partitions.put(partition, PartitionState.TXN_END_RECEIVED);
            return true;
        }
        if (state == PartitionState.ACTIVE) {
            partitions.put(partition, PartitionState.TXN_END_RECEIVED);
            return true;
        } else if (state == PartitionState.SWITCHED) {
            // Partition is currently in an in-flight commit cycle. Record the txnEnd
            // so reset() can promote it to TXN_END_RECEIVED for the next cycle,
            // preventing the boundary from being lost.
            pendingTxnEnd.add(partition);
            LOG.debug("[MultiTxn] txnEnd for SWITCHED partition {}, recorded as pending", partition);
        }
        // If already TXN_END_RECEIVED, a second txnEnd accumulates more data into the
        // same commit cycle (N:1 mapping) — no state change needed.
        return false;
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
     * <p>SWITCHED partitions that received new data during the commit phase will
     * already have been moved to ACTIVE by {@link #onWrite}, so they are retained.
     * Remaining SWITCHED partitions (idle — no new data arrived) are removed from
     * tracking entirely. If they produce data later, {@link #onWrite} will re-register
     * them. This prevents idle partitions from permanently blocking
     * {@link #allSwitched()}.
     *
     * <p>Partitions that received a txnEnd while SWITCHED (recorded in
     * {@link #pendingTxnEnd}) are promoted to TXN_END_RECEIVED so the boundary
     * is preserved for the next commit cycle.
     */
    public synchronized void reset() {
        lastCommitTimeMs = System.currentTimeMillis();

        // Remove idle SWITCHED partitions (those that received no new data during
        // the commit phase). Partitions that got new writes are already ACTIVE.
        List<Integer> toRemove = new ArrayList<>();
        for (Map.Entry<Integer, PartitionState> entry : partitions.entrySet()) {
            if (entry.getValue() == PartitionState.SWITCHED) {
                int partition = entry.getKey();
                if (pendingTxnEnd.contains(partition)) {
                    // A txnEnd arrived while SWITCHED — promote to TXN_END_RECEIVED
                    // so the next commit cycle picks it up immediately.
                    entry.setValue(PartitionState.TXN_END_RECEIVED);
                    LOG.debug("[MultiTxn] Promoted partition {} to TXN_END_RECEIVED from pending txnEnd",
                            partition);
                } else {
                    toRemove.add(partition);
                }
            }
        }

        for (Integer partition : toRemove) {
            partitions.remove(partition);
            LOG.debug("[MultiTxn] Removed idle SWITCHED partition {} from tracking", partition);
        }

        pendingTxnEnd.clear();
        LOG.info("[MultiTxn] PartitionCommitTracker reset, partitions: {}", partitions);
    }

    /**
     * Returns the list of partitions that are still in ACTIVE state (have not
     * received a txnEnd marker). In multi-table transaction mode, upstream must
     * ensure all transactions are complete before a checkpoint barrier arrives.
     * If any partitions are ACTIVE at savepoint time, it indicates a violation
     * of this contract.
     */
    public synchronized List<Integer> getActivePartitions() {
        List<Integer> active = new ArrayList<>();
        for (Map.Entry<Integer, PartitionState> entry : partitions.entrySet()) {
            if (entry.getValue() == PartitionState.ACTIVE) {
                active.add(entry.getKey());
            }
        }
        return active;
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
