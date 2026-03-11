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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    /** Counts consecutive commit cycles where a partition stayed SWITCHED (idle). */
    private final Map<Integer, Integer> idleCycleCount = new LinkedHashMap<>();
    /** Remove a partition after this many consecutive idle commit cycles. */
    private static final int MAX_IDLE_CYCLES = 3;
    private final long commitIntervalMs;
    private volatile long lastCommitTimeMs;

    public PartitionCommitTracker(long commitIntervalMs) {
        this.commitIntervalMs = commitIntervalMs;
        this.lastCommitTimeMs = System.currentTimeMillis();
    }

    /**
     * Called when data is written for a partition. Registers the partition as active
     * if not already tracked (or re-activates it after a commit reset).
     */
    public synchronized void onWrite(int partition) {
        partitions.putIfAbsent(partition, PartitionState.ACTIVE);
        idleCycleCount.remove(partition);
    }

    /**
     * Called when a txnEnd marker arrives for a partition.
     *
     * @return {@code true} if the flush interval has elapsed (caller should attempt switch)
     */
    public synchronized boolean onTxnEnd(int partition) {
        PartitionState state = partitions.get(partition);
        if (state == null) {
            LOG.warn("[MultiTxn] txnEnd for unknown partition {}, ignoring", partition);
            return false;
        }
        if (state == PartitionState.ACTIVE) {
            partitions.put(partition, PartitionState.TXN_END_RECEIVED);
        }
        // If already TXN_END_RECEIVED or SWITCHED, a second txnEnd accumulates
        // more data into the same commit cycle (N:1 mapping) — no state change needed.
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

    public boolean isIntervalElapsed() {
        return System.currentTimeMillis() - lastCommitTimeMs >= commitIntervalMs;
    }

    /**
     * Resets state after a successful commit.
     *
     * <p>SWITCHED partitions are reset to ACTIVE (awaiting next txnEnd).
     * Partitions that received new txnEnd during the commit phase retain
     * their TXN_END_RECEIVED state for the next commit cycle.
     */
    public synchronized void reset() {
        lastCommitTimeMs = System.currentTimeMillis();

        // Track idle cycles and evict partitions that have been idle too long
        List<Integer> toRemove = new ArrayList<>();
        partitions.replaceAll((partition, state) -> {
            if (state == PartitionState.SWITCHED) {
                int cycles = idleCycleCount.getOrDefault(partition, 0) + 1;
                if (cycles >= MAX_IDLE_CYCLES) {
                    toRemove.add(partition);
                } else {
                    idleCycleCount.put(partition, cycles);
                }
                return PartitionState.ACTIVE;
            }
            return state;
        });

        for (Integer partition : toRemove) {
            partitions.remove(partition);
            idleCycleCount.remove(partition);
            LOG.info("[MultiTxn] Evicted idle partition {} after {} consecutive idle cycles",
                    partition, MAX_IDLE_CYCLES);
        }

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
