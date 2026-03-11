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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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
public class DefaultStreamLoadManager implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamLoadManager.class);

    private static final long serialVersionUID = 1L;

    enum State {
        ACTIVE,
        INACTIVE
    }

    private final StreamLoadProperties properties;
    private final boolean enableAutoCommit;
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
    private long flushTimeoutMs = 660000L; // default stream load timeout is 600s, 1.1x for flush

    private volatile boolean commitInFlight = false;

    private final Lock lock = new ReentrantLock();
    private final Condition writable = lock.newCondition();
    private final Condition flushable = lock.newCondition();

    private final AtomicReference<State> state = new AtomicReference<>(State.INACTIVE);
    private volatile Throwable e;

    private final Queue<TransactionTableRegion> flushQ = new ConcurrentLinkedQueue<>();

    /** Per-partition region index for multi-table transaction mode. */
    private final Map<Integer, List<TransactionTableRegion>> partitionRegions = new ConcurrentHashMap<>();

    private final boolean multiTableTransactionEnabled;

    /** Tracks per-partition transaction boundaries (multi-table mode only). */
    private transient PartitionCommitTracker partitionTracker;

    /** Coordinates shared label begin/prepare/commit (multi-table mode only). */
    private transient SharedTransactionCoordinator txnCoordinator;

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

    public DefaultStreamLoadManager(StreamLoadProperties properties, boolean enableAutoCommit) {
        this.properties = properties;
        if (!enableAutoCommit && !properties.isEnableTransaction()) {
            throw new IllegalArgumentException("You must use transaction stream load if not enable auto-commit");
        }
        this.enableAutoCommit = enableAutoCommit;
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
        if (properties.isEnableMultiTableTransaction() && properties.getMultiTableTransactionBufferSize() > 0) {
            this.maxCacheBytes = properties.getMultiTableTransactionBufferSize();
        } else {
            this.maxCacheBytes = properties.getMaxCacheBytes();
        }
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.multiTableTransactionEnabled = properties.isEnableMultiTableTransaction();
        this.flushAndCommitStrategy = new FlushAndCommitStrategy(properties, enableAutoCommit);
        // get timeout from properties's header
        String timeoutStr = properties.getHeaders().get("timeout");
        if (timeoutStr != null) {
            try {
                this.flushTimeoutMs = Long.parseLong(timeoutStr) * 1100; // 1.1x for flush
            } catch (NumberFormatException ex) {
                LOG.warn("Invalid timeout value in properties header: {}, using default", timeoutStr);
            }
        }
    }

    @Override
    public void init() {
        if (labelGeneratorFactory == null) {
            this.labelGeneratorFactory =
                    new LabelGeneratorFactory.DefaultLabelGeneratorFactory(properties.getLabelPrefix());
        }
        this.writeTriggerFlush = new AtomicBoolean(false);
        this.loadMetrics = new LoadMetrics();
        if (multiTableTransactionEnabled) {
            this.partitionTracker = new PartitionCommitTracker(properties.getExpectDelayTime());
        }
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
                        if (multiTableTransactionEnabled && txnCoordinator != null && txnCoordinator.isActive()) {
                            LOG.info("[MultiTxn] Savepoint with active shared transaction; completing before savepoint");
                            // Wait for any in-flight loads to complete
                            for (TransactionTableRegion region : flushQ) {
                                Future<?> result = region.getResult();
                                if (result != null) {
                                    try {
                                        result.get();
                                    } catch (Exception ignored) {
                                        // errors will be handled by the callback
                                    }
                                }
                            }
                            // Trigger loads for regions with pending data that haven't loaded yet
                            for (TransactionTableRegion region : flushQ) {
                                region.triggerLoadIfNeeded();
                            }
                            // Wait for triggered loads to complete
                            boolean allLoadsDone = false;
                            while (!allLoadsDone) {
                                allLoadsDone = true;
                                for (TransactionTableRegion region : flushQ) {
                                    if (region.isFlushing()) {
                                        allLoadsDone = false;
                                        break;
                                    }
                                }
                                if (!allLoadsDone) {
                                    LockSupport.parkNanos(1_000_000L);
                                }
                            }
                            // Commit the shared transaction to avoid losing loaded data
                            String anyTable = null;
                            for (TransactionTableRegion region : flushQ) {
                                if (anyTable == null) {
                                    anyTable = region.getTable();
                                }
                            }
                            if (anyTable != null) {
                                try {
                                    txnCoordinator.prepareAndCommit(anyTable);
                                    LOG.info("[MultiTxn] Shared transaction committed during savepoint");
                                } catch (Exception ex) {
                                    LOG.error("[MultiTxn] Failed to commit shared transaction during savepoint", ex);
                                    this.e = ex;
                                }
                            }
                            txnCoordinator.reset();
                            commitInFlight = false;
                            if (partitionTracker != null) {
                                partitionTracker.reset();
                            }
                            for (TransactionTableRegion region : flushQ) {
                                region.setLabel(null);
                            }
                        }
                        for (TransactionTableRegion region : flushQ) {
                            boolean flush = region.flush(FlushReason.FORCE);
                            LOG.debug("Trigger flush table region {} because of savepoint, region cache bytes: {}, flush: {}",
                                    region.getUniqueKey(), region.getCacheBytes(), flush);
                        }

                        // should ensure all data is committed for auto-commit mode
                        if (enableAutoCommit) {
                            int committedRegions = 0;
                            for (TransactionTableRegion region : flushQ) {
                                // savepoint makes sure no more data is written, so these conditions
                                // can guarantee commit after all data has been written to StarRocks
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
                        LockSupport.unpark(current);
                    } else if (commitInFlight) {
                        // Multi-table coordinator-based commit (manager thread)
                        processMultiTableCommit();
                    } else {
                        // Normal timer-driven path (non-multi-table, or multi-table between commits)
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
            }, "StarRocks-Sink-Manager");
            manager.setDaemon(true);
            manager.setUncaughtExceptionHandler((t, ee) -> {
                LOG.error("StarRocks-Sink-Manager error", ee);
                e = ee;
            });
            manager.start();
            LOG.info("StarRocks-Sink-Manager start, enableAutoCommit: {}, streamLoader: {}, {}",
                    enableAutoCommit, streamLoader.getClass().getName(), EnvUtils.getGitInformation());

            streamLoader.start(properties, this);

            if (multiTableTransactionEnabled) {
                this.txnCoordinator = new SharedTransactionCoordinator(streamLoader, labelGeneratorFactory);
                LOG.info("[MultiTxn] Multi-table transaction mode enabled");
            }
        }
    }

    @Override
    public void setCommitAllowed(boolean allowed) {
        // Legacy no-partition variant: no-op in multi-table mode
    }

    @Override
    public void setCommitAllowed(int partition, boolean allowed) {
        if (!multiTableTransactionEnabled) {
            return;
        }
        if (!allowed) {
            return;
        }

        // txnEnd received for this partition
        boolean intervalReady = partitionTracker.onTxnEnd(partition);
        LOG.debug("[MultiTxn] txnEnd for partition={}, intervalReady={}", partition, intervalReady);

        if (intervalReady) {
            trySwitchAndCommit();
        }
    }

    /**
     * Attempts to switch ready partitions and trigger a commit if all are switched.
     * Called on the task thread.
     */
    private void trySwitchAndCommit() {
        List<Integer> readyPartitions = partitionTracker.getReadyToSwitch();
        for (int p : readyPartitions) {
            List<TransactionTableRegion> pRegions = partitionRegions.get(p);
            if (pRegions != null) {
                for (TransactionTableRegion region : pRegions) {
                    region.switchChunkForCommit();
                }
            }
            partitionTracker.markSwitched(p);
            LOG.debug("[MultiTxn] partition {} switched, regions={}", p,
                    pRegions == null ? 0 : pRegions.size());
        }

        if (partitionTracker.allSwitched() && !commitInFlight) {
            commitInFlight = true;
            lock.lock();
            try {
                flushable.signal();
            } finally {
                lock.unlock();
            }
            LOG.info("[MultiTxn] All partitions switched, commitInFlight=true, signaling manager");
        }
    }

    /**
     * Processes a multi-table commit cycle using the SharedTransactionCoordinator.
     * Called on the manager thread when commitInFlight=true.
     *
     * <p>The method is invoked repeatedly by the manager thread loop. It uses the
     * coordinator's {@code isActive()} state to track progress across iterations:
     * <ol>
     *   <li>First iteration: begin shared transaction, inject label, trigger loads</li>
     *   <li>Subsequent iterations: poll until all loads complete</li>
     *   <li>Final iteration: unified prepare + commit, then reset</li>
     * </ol>
     */
    private void processMultiTableCommit() {
        // Take a snapshot of flushQ to ensure consistent iteration and avoid
        // missing regions added concurrently by the task thread.
        final List<TransactionTableRegion> regionSnapshot =
                Collections.unmodifiableList(new ArrayList<>(flushQ));
        try {
            if (!txnCoordinator.isActive()) {
                // Ensure no region is still flushing from a previous failed cycle
                // (e.g. retrying a load). Starting a new shared transaction while a
                // region retries would fail to inject the shared label.
                for (TransactionTableRegion region : regionSnapshot) {
                    if (region.isFlushing()) {
                        LOG.debug("[MultiTxn] Region {} still flushing before begin, waiting",
                                region.getUniqueKey());
                        return;
                    }
                }

                String anyDb = null;
                String anyTable = null;
                for (TransactionTableRegion region : regionSnapshot) {
                    if (anyDb == null) {
                        anyDb = region.getDatabase();
                        anyTable = region.getTable();
                    }
                }

                if (anyDb == null) {
                    commitInFlight = false;
                    partitionTracker.reset();
                    LOG.info("[MultiTxn] No regions registered; commitInFlight=false");
                    return;
                }

                txnCoordinator.begin(anyDb, anyTable);

                for (TransactionTableRegion region : regionSnapshot) {
                    region.setLabel(txnCoordinator.getSharedLabel());
                    if (region.triggerLoadIfNeeded()) {
                        LOG.debug("[MultiTxn] triggered load for region={}", region.getUniqueKey());
                    }
                }
            }

            for (TransactionTableRegion region : regionSnapshot) {
                if (region.isFlushing()) {
                    LOG.debug("[MultiTxn] Region {} still flushing, will retry next scan",
                            region.getUniqueKey());
                    return;
                }
            }

            String anyTable = null;
            for (TransactionTableRegion region : regionSnapshot) {
                if (anyTable == null) {
                    anyTable = region.getTable();
                }
            }

            if (anyTable != null) {
                txnCoordinator.prepareAndCommit(anyTable);
            }

            for (TransactionTableRegion region : regionSnapshot) {
                region.setLabel(null);
                region.resetAge();
            }
            commitInFlight = false;
            partitionTracker.reset();
            LOG.info("[MultiTxn] Shared transaction committed; commitInFlight=false");
        } catch (Exception ex) {
            LOG.error("[MultiTxn] Shared transaction commit failed", ex);
            txnCoordinator.reset();
            commitInFlight = false;
            partitionTracker.reset();
            for (TransactionTableRegion region : regionSnapshot) {
                region.setLabel(null);
            }
            this.e = ex;
        }
    }

    public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
        this.streamLoadListener = streamLoadListener;
    }

    public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
        this.labelGeneratorFactory = labelGeneratorFactory;
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        TableRegion region = getCacheRegion(uniqueKey, database, table);
        for (String row : rows) {
            checkAndThrowException();
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
                        checkAndThrowException();
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

        LOG.debug("Receive load response, cacheByteBeforeFlush: {}, currentCacheBytes: {}, totalFlushRows : {}",
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
        LOG.info("Stream load manager flush start - currentCacheBytes: {}, maxCacheBytes: {}",
                currentCacheBytes.get(), maxCacheBytes);

        initializeFlushState();

        long startTime = System.currentTimeMillis();
        long waitTime = 100; // Initial wait time: 100ms

        while (!isSavepointFinished()) {
            checkFlushTimeout(startTime);

            triggerFlushSignal();
            LockSupport.park(current);

            if (!savepoint) {
                break;
            }

            waitForRegionResults(waitTime);
            waitTime = calculateNextWaitTime(waitTime);
        }

        finishFlush();
    }

    private void initializeFlushState() {
        savepoint = true;
        allRegionsCommitted = false;
        current = Thread.currentThread();
    }

    private void checkFlushTimeout(long startTime) {
        long elapsedMs = System.currentTimeMillis() - startTime;
        if (elapsedMs > flushTimeoutMs) {
            String errorMsg = String.format(
                    "Stream load manager flush timeout: elapsed %dms, timeout %dms, " +
                            "currentCacheBytes: %d, allRegionsCommitted: %s, savepoint: %s",
                    elapsedMs, flushTimeoutMs, currentCacheBytes.get(), allRegionsCommitted, savepoint);

            LOG.error(errorMsg);
            throw new RuntimeException(String.format(
                    "Stream load manager flush timeout: elapsed %dms, timeout %dms", elapsedMs, flushTimeoutMs));
        }
    }

    private void triggerFlushSignal() {
        lock.lock();
        try {
            flushable.signal();
        } finally {
            lock.unlock();
        }
    }

    private void waitForRegionResults(long waitTime) {
        try {
            for (TableRegion tableRegion : regions.values()) {
                Future<?> result = tableRegion.getResult();
                if (result != null) {
                    result.get();
                }
            }

            if (waitTime > 200) {
                LockSupport.parkNanos(waitTime * 1_000_000L);
                LOG.info("Stream load manager flush waiting: {}ms", waitTime);
            }
        } catch (ExecutionException | InterruptedException ex) {
            LOG.warn("Stream load manager flush get result failed", ex);
            throw new RuntimeException(ex);
        }
    }

    private long calculateNextWaitTime(long currentWaitTime) {
        return Math.min(currentWaitTime * 2, 10000); // Max wait time: 10s
    }

    private void finishFlush() {
        LOG.info("Stream load manager flush finished - currentCacheBytes: {}, maxCacheBytes: {}, allRegionsCommitted: {}",
                currentCacheBytes.get(), maxCacheBytes, allRegionsCommitted);
        checkAndThrowException();
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
        return currentCacheBytes.get() == 0L && (!enableAutoCommit || allRegionsCommitted);
    }

    private void checkAndThrowException() {
        if (e != null) {
            LOG.error("catch exception, wait rollback ", e);
            streamLoader.rollback(snapshot());
            close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(int partition, String database, String table, String... rows) {
        if (!multiTableTransactionEnabled) {
            write(null, database, table, rows);
            return;
        }
        String uniqueKey = "P" + partition + "-" + StreamLoadUtils.getTableUniqueKey(database, table);
        partitionTracker.onWrite(partition);
        TableRegion region = getCacheRegion(uniqueKey, database, table, partition);
        for (String row : rows) {
            checkAndThrowException();
            int bytes = region.write(row.getBytes(StandardCharsets.UTF_8));
            long cachedBytes = currentCacheBytes.addAndGet(bytes);
            if (cachedBytes >= maxWriteBlockCacheBytes) {
                long startTime = System.nanoTime();
                lock.lock();
                try {
                    int idx = 0;
                    while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                        checkAndThrowException();
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
            }
        }
    }

    protected TableRegion getCacheRegion(String uniqueKey, String database, String table) {
        return getCacheRegion(uniqueKey, database, table, -1);
    }

    protected TableRegion getCacheRegion(String uniqueKey, String database, String table, int partition) {
        if (uniqueKey == null) {
            uniqueKey = StreamLoadUtils.getTableUniqueKey(database, table);
        }

        TableRegion region = regions.get(uniqueKey);
        if (region == null) {
            synchronized (regions) {
                region = regions.get(uniqueKey);
                if (region == null) {
                    // For per-partition regions, look up table properties by the real table key
                    String tableKey = StreamLoadUtils.getTableUniqueKey(database, table);
                    StreamLoadTableProperties tableProperties = properties.getTableProperties(tableKey, database, table);
                    LabelGenerator labelGenerator = labelGeneratorFactory.create(database, table);
                    TransactionTableRegion newRegion = new TransactionTableRegion(
                            uniqueKey, database, table, this,
                            tableProperties, streamLoader, labelGenerator, maxRetries, retryIntervalInMs);
                    regions.put(uniqueKey, newRegion);
                    flushQ.offer(newRegion);
                    if (partition >= 0) {
                        partitionRegions.computeIfAbsent(partition, k -> new CopyOnWriteArrayList<>()).add(newRegion);
                    }
                    region = newRegion;
                }
            }
        }
        return region;
    }
}