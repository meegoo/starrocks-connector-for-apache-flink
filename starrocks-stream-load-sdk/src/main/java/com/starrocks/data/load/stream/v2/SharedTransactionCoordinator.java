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

import com.starrocks.data.load.stream.LabelGenerator;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Coordinates a single StarRocks transaction across multiple table regions.
 *
 * <p>In multi-table transaction mode, all table regions within one commit cycle
 * share a single transaction label. This coordinator manages the lifecycle:
 * <ol>
 *   <li>{@link #begin} — generate a shared label and call {@code /api/transaction/begin} once</li>
 *   <li>Each region sends data via {@code /api/transaction/load} using the shared label</li>
 *   <li>{@link #prepareAndCommit} — call {@code /api/transaction/prepare} and
 *       {@code /api/transaction/commit} once for the shared label</li>
 * </ol>
 *
 * <p>The coordinator does not own any data buffers; data management remains in
 * {@link TransactionTableRegion}.
 */
public class SharedTransactionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SharedTransactionCoordinator.class);

    private final StreamLoader streamLoader;
    private final LabelGeneratorFactory labelGeneratorFactory;

    private String sharedLabel;
    private String database;
    private String table;

    /** Tracks whether any HTTP load was sent under the current shared label. */
    private boolean dataLoaded;

    public SharedTransactionCoordinator(StreamLoader streamLoader,
                                        LabelGeneratorFactory labelGeneratorFactory) {
        this.streamLoader = streamLoader;
        this.labelGeneratorFactory = labelGeneratorFactory;
    }

    /**
     * Begins a shared transaction.
     *
     * <p>Generates a new label and issues a single {@code /api/transaction/begin}
     * request to StarRocks. The label is then injected into all regions that have
     * data to load.
     *
     * @param database the database (all tables must be in the same database)
     * @param anyTable any table in the database (StarRocks requires a table in begin)
     */
    public synchronized void begin(String database, String anyTable) {
        LabelGenerator generator = labelGeneratorFactory.create(database, anyTable);
        this.sharedLabel = generator.next();
        this.database = database;
        this.table = anyTable;

        LOG.info("[MultiTxn] SharedTransaction begin: label={}, db={}, table={}",
                sharedLabel, database, anyTable);

        this.dataLoaded = false;

        boolean ok = streamLoader.beginTransaction(sharedLabel, database, anyTable);
        if (!ok) {
            throw new StreamLoadFailException(
                    "Failed to begin shared transaction, label: " + sharedLabel +
                    ", db: " + database + ", table: " + anyTable);
        }
    }

    /**
     * Marks that at least one HTTP load has been sent under the current shared label.
     * Called by the manager when a region triggers a load.
     */
    public synchronized void markDataLoaded() {
        this.dataLoaded = true;
    }

    /**
     * Returns {@code true} if any data has been loaded under the current shared label.
     */
    public synchronized boolean hasDataLoaded() {
        return dataLoaded;
    }

    /**
     * Injects the shared label into all regions that have pending data.
     * After this call, each region's {@code streamLoad()} will use the shared label
     * (because {@code TransactionStreamLoader.begin(region)} skips begin when
     * label is already set).
     */
    public synchronized void injectLabel(Collection<TransactionTableRegion> regions) {
        for (TransactionTableRegion region : regions) {
            region.setLabel(sharedLabel);
        }
        LOG.info("[MultiTxn] Injected sharedLabel={} into {} regions", sharedLabel, regions.size());
    }

    /**
     * Executes a unified prepare + commit for the shared transaction.
     *
     * <p>Must be called after all regions have completed their HTTP loads.
     * Uses any table from the database (StarRocks resolves by label).
     *
     * @param anyTable any table in the database
     */
    public synchronized void prepareAndCommit(String anyTable) {
        StreamLoadSnapshot.Transaction txn =
                new StreamLoadSnapshot.Transaction(database, anyTable, sharedLabel);

        LOG.info("[MultiTxn] SharedTransaction prepare: label={}", sharedLabel);
        if (!streamLoader.prepare(txn)) {
            throw new StreamLoadFailException(
                    "Failed to prepare shared transaction, label: " + sharedLabel);
        }

        LOG.info("[MultiTxn] SharedTransaction commit: label={}", sharedLabel);
        if (!streamLoader.commit(txn)) {
            throw new StreamLoadFailException(
                    "Failed to commit shared transaction, label: " + sharedLabel);
        }

        LOG.info("[MultiTxn] SharedTransaction committed successfully: label={}", sharedLabel);
        this.sharedLabel = null;
        this.database = null;
        this.table = null;
        this.dataLoaded = false;
    }

    public synchronized String getSharedLabel() {
        return sharedLabel;
    }

    public synchronized boolean isActive() {
        return sharedLabel != null;
    }

    /**
     * Attempts to rollback the in-progress shared transaction, then resets state.
     * Used on error paths and savepoint interruption. If rollback fails, the
     * StarRocks-side transaction will be cleaned up by its timeout.
     */
    public synchronized void reset() {
        if (sharedLabel != null) {
            LOG.warn("[MultiTxn] SharedTransactionCoordinator reset, attempting rollback for label={}", sharedLabel);
            try {
                StreamLoadSnapshot.Transaction txn =
                        new StreamLoadSnapshot.Transaction(database, table, sharedLabel);
                streamLoader.rollback(txn);
                LOG.info("[MultiTxn] Rollback succeeded for label={}", sharedLabel);
            } catch (Exception ex) {
                LOG.warn("[MultiTxn] Rollback failed for label={}, will rely on server-side timeout",
                        sharedLabel, ex);
            }
        }
        this.sharedLabel = null;
        this.database = null;
        this.table = null;
        this.dataLoaded = false;
    }
}
