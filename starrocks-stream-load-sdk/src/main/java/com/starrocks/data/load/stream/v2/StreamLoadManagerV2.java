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

import com.starrocks.data.load.stream.DefaultStreamLoader;
import com.starrocks.data.load.stream.EnvUtils;
import com.starrocks.data.load.stream.LabelGenerator;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.LoadMetrics;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.TransactionStreamLoader;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of {@link StreamLoadManager}. In this manager, you can use normal stream load or
 * transaction stream load to load data to StarRocks. You can control which to use when constructing
 * the manager with parameter **properties**. If {@link StreamLoadProperties#isEnableTransaction()}
 * is true, transaction stream load will be used, otherwise the normal stream load. You can also control
 * how to commit the transaction stream load by parameter **enableAutoCommit**. If it's true, the
 * manager will commit the load automatically, otherwise you need to commit the load manually. Note that
 * this parameter should always be true for the normal stream load currently.
 * The usage for manual commit should like this
 *     manager.write(); // write some recodes
 *     manager.flush();    // ensure the data is flushed to StarRocks, and the transaction is prepared
 *     manager.snapshot(); // take a snapshot the current transactions, mainly recording the labels
 *     manager.commit();   // commit those snapshots
 */
public class StreamLoadManagerV2 implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadManagerV2.class);

    private static final long serialVersionUID = 1L;

    enum State {
        ACTIVE,
        INACTIVE
    }

    private final StreamLoadProperties properties;
    private final boolean enableAutoCommit;
    private final boolean multiTableTxnEnabled;
    private final StreamLoader streamLoader;
    private final int maxRetries;
    private final int retryIntervalInMs;
    // threshold to trigger flush
    private final long maxCacheBytes;
    // threshold to block write
    private final long maxWriteBlockCacheBytes;
    private final Map<String, TableRegion> regions = new ConcurrentHashMap<>();
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final AtomicLong totalFlushRows = new AtomicLong(0L);

    private final AtomicLong numberTotalRows = new AtomicLong(0L);
    private final AtomicLong numberLoadRows = new AtomicLong(0L);

    private final FlushAndCommitStrategy flushAndCommitStrategy;
    private final long scanningFrequency;
    private Thread current;
    private Thread manager;
    private volatile boolean savepoint = false;
    private volatile boolean allRegionsCommitted;

    private final Lock lock = new ReentrantLock();
    private final Condition writable = lock.newCondition();
    private final Condition flushable = lock.newCondition();

    private final AtomicReference<State> state = new AtomicReference<>(State.INACTIVE);
    private volatile Throwable e;

    private final Queue<TransactionTableRegion> flushQ = new ConcurrentLinkedQueue<>();

    // Multi-table transaction state: all regions share one label per commit cycle
    private volatile String sharedTxnLabel;
    private volatile String sharedTxnDatabase;
    private volatile String sharedTxnTable;
    private volatile boolean sharedTxnBegun;

    private volatile boolean commitAllowed;
    private volatile long lastCommitTimeMs;
    // True while async commit (wait for loads + prepare + commit) is in flight.
    // During this period: no new commit is triggered, and manager thread skips
    // buffer-management flushes to prevent new data leaking into the old transaction.
    private volatile boolean commitInFlight;

    /**
     * Whether write() has triggered a flush after currentCacheBytes > maxCacheBytes.
     * This flag is set true after the flush is triggered in writer(), and set false
     * after the flush completed in callback(). During this period, there is no need
     * to re-trigger a flush.
     */
    private transient AtomicBoolean writeTriggerFlush;
    private transient LoadMetrics loadMetrics;
    private transient StreamLoadListener streamLoadListener;
    private transient LabelGeneratorFactory labelGeneratorFactory;

    public StreamLoadManagerV2(StreamLoadProperties properties, boolean enableAutoCommit) {
        this.properties = properties;
        if (!enableAutoCommit && !properties.isEnableTransaction()) {
            throw new IllegalArgumentException("You must use transaction stream load if not enable auto-commit");
        }
        this.enableAutoCommit = enableAutoCommit;
        this.multiTableTxnEnabled = properties.isEnableMultiTableTransaction();
        if (!enableAutoCommit) {
            streamLoader = new TransactionStreamLoader(false);
            maxRetries = 0;
            retryIntervalInMs = 0;
        } else {
            // TODO transaction stream load can't support retry currently
            streamLoader = (properties.getMaxRetries() > 0 || !properties.isEnableTransaction())
                    ? new DefaultStreamLoader() : new TransactionStreamLoader(true);
            maxRetries = properties.getMaxRetries();
            retryIntervalInMs = properties.getRetryIntervalInMs();
        }
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.flushAndCommitStrategy = new FlushAndCommitStrategy(properties, enableAutoCommit);
    }

    @Override
    public void init() {
        if (labelGeneratorFactory == null) {
            this.labelGeneratorFactory =
                    new LabelGeneratorFactory.DefaultLabelGeneratorFactory(properties.getLabelPrefix());
        }
        this.writeTriggerFlush = new AtomicBoolean(false);
        this.loadMetrics = new LoadMetrics();
        this.lastCommitTimeMs = System.currentTimeMillis();
        if (state.compareAndSet(State.INACTIVE, State.ACTIVE)) {
            this.manager = new Thread(() -> {
                long lastPrintTimestamp = -1;
                LOG.info("manager running, scanningFrequency : {}", scanningFrequency);
                while (true) {
                    lock.lock();
                    try {
                        flushable.await(scanningFrequency, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (savepoint) {
                            savepoint = false;
                            LockSupport.unpark(current);
                        }
                        break;
                    } finally {
                        lock.unlock();
                    }

                    if (lastPrintTimestamp == -1 || System.currentTimeMillis() - lastPrintTimestamp > 10000) {
                        lastPrintTimestamp = System.currentTimeMillis();
                        LOG.debug("Audit information: {}, {}", loadMetrics, flushAndCommitStrategy);
                    }

                    if (savepoint) {
                        for (TransactionTableRegion region : flushQ) {
                            boolean flush = region.flush(FlushReason.FORCE);
                            LOG.debug("Trigger flush table region {} because of savepoint, region cache bytes: {}, flush: {}",
                                    region.getUniqueKey(), region.getCacheBytes(), flush);
                        }

                        if (enableAutoCommit) {
                            if (multiTableTxnEnabled) {
                                // If an async commit is in flight, complete it first
                                if (commitInFlight) {
                                    completeAsyncCommit();
                                }

                                // Multi-table mode: unified commit for shared transaction
                                boolean allFlushed = true;
                                for (TransactionTableRegion region : flushQ) {
                                    if (region.isFlushing() || region.getCacheBytes() > 0) {
                                        allFlushed = false;
                                        break;
                                    }
                                }
                                if (allFlushed && sharedTxnBegun && !commitInFlight) {
                                    commitSharedTransaction();
                                    allRegionsCommitted = true;
                                    LOG.info("Multi-table transaction committed for savepoint, label: {}", sharedTxnLabel);
                                } else if (allFlushed && !sharedTxnBegun && !commitInFlight) {
                                    allRegionsCommitted = true;
                                } else {
                                    LOG.debug("Multi-table savepoint: waiting for regions to finish flushing or commit in flight");
                                }
                            } else {
                                // Original per-region commit
                                int committedRegions = 0;
                                for (TransactionTableRegion region : flushQ) {
                                    boolean success = region.commit();
                                    if (success && region.getCacheBytes() == 0) {
                                        committedRegions += 1;
                                        region.resetAge();
                                    }
                                    LOG.debug("Commit region {} for savepoint, success: {}", region.getUniqueKey(), success);
                                }

                                if (committedRegions == flushQ.size()) {
                                    allRegionsCommitted = true;
                                    LOG.info("All regions committed for savepoint, number of regions: {}", committedRegions);
                                } else {
                                    LOG.debug("Some regions not committed for savepoint, expected num: {}, actual num: {}",
                                            flushQ.size(), committedRegions);
                                }
                            }
                        }
                        LockSupport.unpark(current);
                    } else {
                        if (multiTableTxnEnabled) {
                            if (commitInFlight) {
                                // Async commit in progress: wait for loads, then commit.
                                // Do NOT flush new data — it must stay in active chunks
                                // to avoid leaking into the old transaction.
                                completeAsyncCommit();
                            } else if (sharedTxnBegun) {
                                // Normal buffer-management flushes (send data via load).
                                // Only when a shared transaction is active (label is set).
                                // Commit is triggered by task thread in setCommitAllowed().
                                for (TransactionTableRegion region : flushQ) {
                                    region.getAndIncrementAge();
                                }

                                for (FlushAndCommitStrategy.SelectFlushResult result : flushAndCommitStrategy.selectFlushRegions(flushQ, currentCacheBytes.get())) {
                                    TransactionTableRegion region = result.getRegion();
                                    boolean flush = region.flush(result.getReason());
                                    LOG.debug("Trigger flush table region {} because of selection, region cache bytes: {}," +
                                            " flush: {}", region.getUniqueKey(), region.getCacheBytes(), flush);
                                }
                            }
                        } else {
                            // Original per-region commit logic
                            for (TransactionTableRegion region : flushQ) {
                                region.getAndIncrementAge();
                                if (flushAndCommitStrategy.shouldCommit(region)) {
                                    boolean success = region.commit();
                                    if (success) {
                                        region.resetAge();
                                    }
                                    LOG.debug("Commit region {} for normal, success: {}", region.getUniqueKey(), success);
                                }
                            }

                            for (FlushAndCommitStrategy.SelectFlushResult result : flushAndCommitStrategy.selectFlushRegions(flushQ, currentCacheBytes.get())) {
                                TransactionTableRegion region = result.getRegion();
                                boolean flush = region.flush(result.getReason());
                                LOG.debug("Trigger flush table region {} because of selection, region cache bytes: {}," +
                                        " flush: {}", region.getUniqueKey(), region.getCacheBytes(), flush);
                            }
                        }
                    }
                }
            }, "StarRocks-Sink-Manager");
            manager.setDaemon(true);
            manager.start();
            manager.setUncaughtExceptionHandler((t, ee) -> {
                LOG.error("StarRocks-Sink-Manager error", ee);
                e = ee;
            });
            LOG.info("StarRocks-Sink-Manager start, enableAutoCommit: {}, streamLoader: {}, {}",
                    enableAutoCommit, streamLoader.getClass().getName(), EnvUtils.getGitInformation());

            streamLoader.start(properties, this);
        }
    }

    public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
        this.streamLoadListener = streamLoadListener;
    }

    public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
        this.labelGeneratorFactory = labelGeneratorFactory;
    }

    /**
     * Called by the sink after each write <b>on the Flink task thread</b>.
     *
     * <p>When the conditions are met (txnEnd + timer + no in-flight commit),
     * switchChunk is performed on the task thread (guaranteeing no concurrent
     * writes), then the async commit is handed off to the manager thread.
     * The task thread returns immediately without blocking.
     */
    public void setCommitAllowed(boolean allowed) {
        if (!multiTableTxnEnabled) {
            return;
        }
        this.commitAllowed = allowed;
        if (allowed && sharedTxnBegun && !commitInFlight
                && System.currentTimeMillis() - lastCommitTimeMs >= properties.getExpectDelayTime()) {
            triggerCommitFromTaskThread();
        }
    }

    /**
     * Phase 1 of the commit: runs on the task thread.
     * Performs switchChunk (capturing a clean data boundary) and then signals
     * the manager thread to handle the async remainder (wait + prepare + commit).
     * The task thread returns immediately and continues processing new rows.
     */
    private void triggerCommitFromTaskThread() {
        AssertNotException();

        for (TransactionTableRegion region : flushQ) {
            region.flush(FlushReason.FORCE);
        }

        commitInFlight = true;
        lastCommitTimeMs = System.currentTimeMillis();

        lock.lock();
        try {
            flushable.signal();
        } finally {
            lock.unlock();
        }

        LOG.info("Commit triggered from task thread, label: {}, regions: {}",
                sharedTxnLabel, flushQ.size());
    }

    /**
     * Phase 2 of the commit: runs on the manager thread.
     * Waits for all loads to complete, then prepare + commit. After commit,
     * resets the shared transaction state so the next write starts a new cycle.
     */
    private void completeAsyncCommit() {
        for (TableRegion tableRegion : regions.values()) {
            Future<?> result = tableRegion.getResult();
            if (result != null) {
                try {
                    result.get();
                } catch (Exception ex) {
                    LOG.error("Async commit: flush result failed", ex);
                    this.e = ex;
                    commitInFlight = false;
                    return;
                }
            }
        }

        boolean allDone = true;
        for (TransactionTableRegion region : flushQ) {
            if (region.isFlushing()) {
                allDone = false;
                break;
            }
        }

        if (allDone) {
            commitSharedTransaction();
            commitInFlight = false;
        }
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        if (multiTableTxnEnabled) {
            ensureSharedTxnBegun(database, table);
        }
        TableRegion region = getCacheRegion(uniqueKey, database, table);
        for (String row : rows) {
            AssertNotException();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Write uniqueKey {}, database {}, table {}, row {}",
                        uniqueKey == null ? "null" : uniqueKey, database, table, row);
            }
            int bytes = region.write(row.getBytes(StandardCharsets.UTF_8));
            long cachedBytes = currentCacheBytes.addAndGet(bytes);
            if (cachedBytes >= maxWriteBlockCacheBytes) {
                long startTime = System.nanoTime();
                lock.lock();
                try {
                    int idx = 0;
                    while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                        AssertNotException();
                        LOG.info("Cache full, wait flush, currentBytes: {}, maxWriteBlockCacheBytes: {}",
                                currentCacheBytes.get(), maxWriteBlockCacheBytes);
                        flushable.signal();
                        writable.await(Math.min(++idx, 5), TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ex) {
                    this.e = ex;
                    throw new RuntimeException(ex);
                } finally {
                    lock.unlock();
                }
                loadMetrics.updateWriteBlock(1, System.nanoTime() - startTime);
            } else if (cachedBytes >= maxCacheBytes && writeTriggerFlush.compareAndSet(false, true)) {
                lock.lock();
                try {
                    flushable.signal();
                } finally {
                    lock.unlock();
                }
                loadMetrics.updateWriteTriggerFlush(1);
                LOG.info("Trigger flush, currentBytes: {}, maxCacheBytes: {}", cachedBytes, maxCacheBytes);
            }
        }
    }

    @Override
    public void callback(StreamLoadResponse response) {
        long cacheByteBeforeFlush = response.getFlushBytes() != null ? currentCacheBytes.getAndAdd(-response.getFlushBytes()) : currentCacheBytes.get();
        if (response.getFlushRows() != null) {
            totalFlushRows.addAndGet(response.getFlushRows());
        }
        writeTriggerFlush.set(false);

        LOG.info("Receive load response, cacheByteBeforeFlush: {}, currentCacheBytes: {}, totalFlushRows : {}",
                cacheByteBeforeFlush, currentCacheBytes.get(), totalFlushRows.get());

        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }

        if (response.getException() != null) {
            LOG.error("Stream load failed", response.getException());
            this.e = response.getException();
        }

        if (response.getBody() != null) {
            if (response.getBody().getNumberTotalRows() != null) {
                numberTotalRows.addAndGet(response.getBody().getNumberTotalRows());
            }
            if (response.getBody().getNumberLoadedRows() != null) {
                numberLoadRows.addAndGet(response.getBody().getNumberLoadedRows());
            }
        }

        if (response.getException() != null) {
            this.loadMetrics.updateFailedLoad();
        } else {
            this.loadMetrics.updateSuccessLoad(response);
        }

        if (streamLoadListener != null) {
            streamLoadListener.onResponse(response);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}", loadMetrics);
        }
    }

    @Override
    public void callback(Throwable e) {
        LOG.error("Stream load failed", e);
        this.e = e;
    }

    public Throwable getException() {
        return e;
    }

    @Override
    public void flush() {
        LOG.info("Stream load manager flush");
        savepoint = true;
        allRegionsCommitted = false;
        current = Thread.currentThread();
        while (!isSavepointFinished()) {
            lock.lock();
            try {
                flushable.signal();
            } finally {
                lock.unlock();
            }
            LockSupport.park(current);
            if (!savepoint) {
                break;
            }
            try {
                for (TableRegion tableRegion : regions.values()) {
                    Future<?> result = tableRegion.getResult();
                    if (result != null) {
                        result.get();
                    }
                }
            } catch (ExecutionException | InterruptedException ex) {
                LOG.warn("Flush get result failed", ex);
            }
        }
        AssertNotException();
        savepoint = false;
    }

    @Override
    public StreamLoadSnapshot snapshot() {
        StreamLoadSnapshot snapshot = StreamLoadSnapshot.snapshot(regions.values());
        for (TableRegion region : regions.values()) {
            region.setLabel(null);
        }
        return snapshot;
    }

    public StreamLoader getStreamLoader() {
        return streamLoader;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return streamLoader.prepare(snapshot);
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return streamLoader.commit(snapshot);
    }

    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return streamLoader.rollback(snapshot);
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.ACTIVE, State.INACTIVE)) {
            LOG.info("StreamLoadManagerV2 close, loadMetrics: {}, flushAndCommit: {}",
                    loadMetrics, flushAndCommitStrategy);
            manager.interrupt();
            streamLoader.close();
        }
    }

    private boolean isSavepointFinished() {
        if (e != null) {
            return true;
        }
        return currentCacheBytes.compareAndSet(0L, 0L) && (!enableAutoCommit || allRegionsCommitted);
    }

    private void AssertNotException() {
        if (e != null) {
            LOG.error("catch exception, wait rollback ", e);
            streamLoader.rollback(snapshot());
            close();
            throw new RuntimeException(e);
        }
    }

    /**
     * Ensure the shared transaction is begun. Called on write() in multi-table mode.
     * The first call generates a label and sends /api/transaction/begin to StarRocks.
     * Subsequent calls within the same commit cycle are no-ops.
     */
    private synchronized void ensureSharedTxnBegun(String database, String table) {
        if (sharedTxnBegun) {
            return;
        }
        LabelGenerator gen = labelGeneratorFactory.create(database, table);
        sharedTxnLabel = gen.next();
        sharedTxnDatabase = database;
        sharedTxnTable = table;

        if (!streamLoader.beginTransaction(sharedTxnLabel, database, table)) {
            throw new RuntimeException("Failed to begin multi-table transaction, label: " + sharedTxnLabel);
        }

        // Pre-set the shared label on all existing regions so their send() calls
        // skip begin (label is already set → TransactionStreamLoader.begin() is no-op)
        for (TableRegion region : regions.values()) {
            region.setLabel(sharedTxnLabel);
        }

        sharedTxnBegun = true;
        LOG.info("Multi-table transaction begun, label: {}, db: {}, table: {}", sharedTxnLabel, database, table);
    }

    /**
     * Unified prepare + commit for the shared multi-table transaction.
     */
    private void commitSharedTransaction() {
        if (!sharedTxnBegun || sharedTxnLabel == null) {
            return;
        }
        String label = sharedTxnLabel;
        StreamLoadSnapshot.Transaction txn = new StreamLoadSnapshot.Transaction(
                sharedTxnDatabase, sharedTxnTable, label);
        try {
            if (!streamLoader.prepare(txn)) {
                throw new RuntimeException("Failed to prepare multi-table transaction, label: " + label);
            }
            if (!streamLoader.commit(txn)) {
                throw new RuntimeException("Failed to commit multi-table transaction, label: " + label);
            }
        } catch (Exception ex) {
            LOG.error("Multi-table transaction commit failed, label: {}", label, ex);
            this.e = ex;
            return;
        }

        // Reset for next transaction cycle
        sharedTxnBegun = false;
        sharedTxnLabel = null;
        sharedTxnDatabase = null;
        sharedTxnTable = null;
        commitAllowed = false;
        lastCommitTimeMs = System.currentTimeMillis();
        for (TableRegion region : regions.values()) {
            region.setLabel(null);
            region.resetAge();
        }
        LOG.info("Multi-table transaction committed, label: {}", label);
    }

    protected TableRegion getCacheRegion(String uniqueKey, String database, String table) {
        if (uniqueKey == null) {
            uniqueKey = StreamLoadUtils.getTableUniqueKey(database, table);
        }

        TableRegion region = regions.get(uniqueKey);
        if (region == null) {
            // currently write() will not be called concurrently, so regions will also not be
            // created concurrently, but for future extension, protect it with synchronized
            synchronized (regions) {
                region = regions.get(uniqueKey);
                if (region == null) {
                    StreamLoadTableProperties tableProperties = properties.getTableProperties(uniqueKey, database, table);
                    LabelGenerator labelGenerator = labelGeneratorFactory.create(database, table);
                    region = new TransactionTableRegion(uniqueKey, database, table, this,
                            tableProperties, streamLoader, labelGenerator, maxRetries, retryIntervalInMs);
                    if (multiTableTxnEnabled && sharedTxnBegun && sharedTxnLabel != null) {
                        region.setLabel(sharedTxnLabel);
                    }
                    regions.put(uniqueKey, region);
                    flushQ.offer((TransactionTableRegion) region);
                }
            }
        }
        return region;
    }
}
