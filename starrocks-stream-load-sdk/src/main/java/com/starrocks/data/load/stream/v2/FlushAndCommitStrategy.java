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

import com.starrocks.data.load.stream.StreamLoadStrategy;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class FlushAndCommitStrategy implements StreamLoadStrategy {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlushAndCommitStrategy.class);

    private final long expectDelayTime;
    private final long scanFrequency;
    private final long ageThreshold;
    private final long maxCacheBytes;
    private final boolean enableAutoCommit;

    private final AtomicLong numAgeTriggerFlush = new AtomicLong(0);
    private final AtomicLong numCacheTriggerFlush = new AtomicLong(0);
    private final AtomicLong numTableTriggerFlush = new AtomicLong(0);

    /**
     * Whether commit is allowed for the current source transaction.
     *
     * <p>In multi-table transaction mode the upstream source controls when a logical
     * transaction ends by sending a special "transaction-end" marker row.  Until that
     * marker arrives, the sink must buffer data without committing it to StarRocks even
     * if the flush interval has elapsed.  Once the marker is received the flag is set
     * to {@code true} and the next manager-thread scan will commit all pending regions.
     *
     * <p>In normal (non-multi-table) mode this flag is always {@code true} so the
     * existing age-based commit behaviour is unchanged.
     */
    private final boolean multiTableTransactionEnabled;
    private volatile boolean commitAllowed;

    public FlushAndCommitStrategy(StreamLoadProperties properties, boolean enableAutoCommit) {
        this.expectDelayTime = properties.getExpectDelayTime();
        this.scanFrequency = properties.getScanningFrequency();
        this.ageThreshold = expectDelayTime / scanFrequency;
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.enableAutoCommit = enableAutoCommit;
        this.multiTableTransactionEnabled = properties.isEnableMultiTableTransaction();
        // In non-multi-table mode commit is always allowed (preserves original behaviour).
        this.commitAllowed = !multiTableTransactionEnabled;

        LOG.info("{}", this);
    }

    /**
     * Sets whether commit is allowed for the current source transaction.
     *
     * <p>Only meaningful when {@link #multiTableTransactionEnabled} is {@code true}.
     * Calling this with {@code true} unblocks the next age-triggered commit cycle;
     * calling it with {@code false} re-arms the guard for the next transaction.
     */
    public void setCommitAllowed(boolean allowed) {
        if (multiTableTransactionEnabled) {
            this.commitAllowed = allowed;
            LOG.debug("setCommitAllowed: {}", allowed);
        }
    }

    @Override
    public List<TableRegion> select(Iterable<TableRegion> regions) {
       throw new UnsupportedOperationException();
    }

    public List<SelectFlushResult> selectFlushRegions(Queue<TransactionTableRegion> regions, long currentCacheBytes) {
        List<SelectFlushResult> flushRegions = new ArrayList<>();
        for (TransactionTableRegion region : regions) {
            if (shouldCommit(region)) {
                numAgeTriggerFlush.getAndIncrement();
                flushRegions.add(new SelectFlushResult(FlushReason.COMMIT, region));
                LOG.debug("Choose region {} to flush because the region should commit, age: {}, " +
                            "threshold: {}, scanFreq: {}, expectDelayTime: {}", region.getUniqueKey(),
                                region.getAge(), ageThreshold, scanFrequency, expectDelayTime);
            } else {
                FlushReason reason = region.shouldFlush();
                if (reason != FlushReason.NONE) {
                    numTableTriggerFlush.getAndIncrement();
                    flushRegions.add(new SelectFlushResult(reason, region));
                    LOG.debug("Choose region {} to flush because the region itself decide to flush, age: {}, " +
                                    "threshold: {}, scanFreq: {}, expectDelayTime: {}, reason: {}", region.getUniqueKey(),
                            region.getAge(), ageThreshold, scanFrequency, expectDelayTime, reason);
                }
            }
        }

        // simply choose the region with maximum bytes
        if (flushRegions.isEmpty() && currentCacheBytes >= maxCacheBytes) {
            TransactionTableRegion region = regions.stream()
                    .max(Comparator.comparingLong(TableRegion::getCacheBytes)).orElse(null);
            if (region != null) {
                numCacheTriggerFlush.getAndIncrement();
                flushRegions.add(new SelectFlushResult(FlushReason.CACHE_FULL, region));
                LOG.debug("Choose region {} to flush because it's force flush, age: {}, " +
                            "threshold: {}, scanFreq: {}, expectDelayTime: {}", region.getUniqueKey(),
                                region.getAge(), ageThreshold, scanFrequency, expectDelayTime);
            }
        }

        return flushRegions;
    }
    
    public boolean shouldCommit(TableRegion region) {
        return enableAutoCommit && commitAllowed && region.getAge() > ageThreshold;
    }

    /**
     * Returns whether commit is currently allowed.
     * Always {@code true} in non-multi-table mode.
     */
    public boolean isCommitAllowed() {
        return commitAllowed;
    }

    /** Returns whether multi-table transaction mode is enabled. */
    public boolean isMultiTableTransactionEnabled() {
        return multiTableTransactionEnabled;
    }

    @Override
    public String toString() {
        return "FlushAndCommitStrategy{" +
                "expectDelayTime=" + expectDelayTime +
                ", scanFrequency=" + scanFrequency +
                ", ageThreshold=" + ageThreshold +
                ", maxCacheBytes=" + maxCacheBytes +
                ", enableAutoCommit=" + enableAutoCommit +
                ", multiTableTransactionEnabled=" + multiTableTransactionEnabled +
                ", commitAllowed=" + commitAllowed +
                ", numAgeTriggerFlush=" + numAgeTriggerFlush +
                ", numCacheTriggerFlush=" + numCacheTriggerFlush +
                ", numTableTriggerFlush=" + numTableTriggerFlush +
                '}';
    }

    public static class SelectFlushResult {

        private final FlushReason reason;
        private TransactionTableRegion region;

        public SelectFlushResult(FlushReason reason, TransactionTableRegion region) {
            this.reason = reason;
            this.region = region;
        }

        public FlushReason getReason() {
            return reason;
        }

        public TransactionTableRegion getRegion() {
            return region;
        }
    }
}
