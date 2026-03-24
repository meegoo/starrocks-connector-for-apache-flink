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

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PartitionCommitTrackerTest {

    /**
     * Verifies the pending txnEnd promotion path:
     * 1. Partition 0 writes data → ACTIVE
     * 2. Partition 0 receives txnEnd → TXN_END_RECEIVED
     * 3. Partition 0 is marked SWITCHED (commit in flight)
     * 4. While SWITCHED, another txnEnd arrives → recorded in pendingTxnEnd
     * 5. After reset(), the partition is promoted to TXN_END_RECEIVED (not removed)
     */
    @Test
    public void testPendingTxnEndPromotedAfterReset() {
        PartitionCommitTracker tracker = new PartitionCommitTracker(0);

        // Step 1-2: write + txnEnd
        tracker.onWrite(0);
        boolean shouldSwitch = tracker.onTxnEnd(0);
        Assert.assertTrue("Should switch on first txnEnd", shouldSwitch);

        // Step 3: mark switched (simulates commit in flight)
        tracker.markSwitched(0);
        Assert.assertTrue("All should be switched", tracker.allSwitched());

        // Step 4: another txnEnd arrives while SWITCHED
        boolean shouldSwitch2 = tracker.onTxnEnd(0);
        Assert.assertFalse("Should NOT switch for SWITCHED partition", shouldSwitch2);

        // Step 5: reset after commit completes
        tracker.reset();

        // The partition should be promoted to TXN_END_RECEIVED, not removed
        Assert.assertFalse("Tracker should not be empty after pending promotion", tracker.isEmpty());

        // It should be ready to switch immediately (interval=0)
        List<Integer> readyToSwitch = tracker.getReadyToSwitch();
        Assert.assertEquals("Promoted partition should be ready to switch", 1, readyToSwitch.size());
        Assert.assertEquals(Integer.valueOf(0), readyToSwitch.get(0));

        // No active partitions (it's TXN_END_RECEIVED, not ACTIVE)
        Assert.assertTrue("No ACTIVE partitions expected", tracker.getActivePartitions().isEmpty());
    }

    /**
     * Verifies that idle SWITCHED partitions (no pending txnEnd, no new writes)
     * are removed from tracking during reset().
     */
    @Test
    public void testIdleSwitchedPartitionRemovedOnReset() {
        PartitionCommitTracker tracker = new PartitionCommitTracker(0);

        tracker.onWrite(0);
        tracker.onTxnEnd(0);
        tracker.markSwitched(0);

        // No pending txnEnd, no new writes → idle
        tracker.reset();

        Assert.assertTrue("Idle SWITCHED partition should be removed", tracker.isEmpty());
    }

    /**
     * Verifies that a SWITCHED partition receiving new writes (via onWrite)
     * stays SWITCHED (not demoted to ACTIVE), because writes go into
     * the fresh active chunk and must not disrupt the pending commit.
     */
    @Test
    public void testWriteDuringSwitchedDoesNotDemote() {
        PartitionCommitTracker tracker = new PartitionCommitTracker(0);

        tracker.onWrite(0);
        tracker.onTxnEnd(0);
        tracker.markSwitched(0);

        // New write arrives while SWITCHED
        tracker.onWrite(0);

        // Should still be all-switched (write doesn't change SWITCHED state)
        Assert.assertTrue("allSwitched should still be true", tracker.allSwitched());
    }

    /**
     * Verifies the basic state machine: ACTIVE → TXN_END_RECEIVED → SWITCHED.
     */
    @Test
    public void testBasicStateTransitions() {
        PartitionCommitTracker tracker = new PartitionCommitTracker(0);

        // Initially empty
        Assert.assertTrue(tracker.isEmpty());
        Assert.assertFalse(tracker.allSwitched());

        // Write → ACTIVE
        tracker.onWrite(0);
        Assert.assertFalse(tracker.isEmpty());
        List<Integer> active = tracker.getActivePartitions();
        Assert.assertEquals(1, active.size());
        Assert.assertEquals(Integer.valueOf(0), active.get(0));

        // txnEnd → TXN_END_RECEIVED
        tracker.onTxnEnd(0);
        Assert.assertTrue("No active after txnEnd", tracker.getActivePartitions().isEmpty());
        Assert.assertFalse("Not yet switched", tracker.allSwitched());

        // markSwitched → SWITCHED
        tracker.markSwitched(0);
        Assert.assertTrue("All switched", tracker.allSwitched());
    }

    /**
     * Verifies that txnEnd for an unknown/evicted partition re-registers it
     * as TXN_END_RECEIVED.
     */
    @Test
    public void testTxnEndForUnknownPartitionReRegisters() {
        PartitionCommitTracker tracker = new PartitionCommitTracker(0);

        // txnEnd for a never-seen partition
        boolean shouldSwitch = tracker.onTxnEnd(42);
        Assert.assertTrue("Unknown partition should be switched", shouldSwitch);
        Assert.assertFalse("Tracker should not be empty", tracker.isEmpty());
        Assert.assertTrue("No ACTIVE partitions", tracker.getActivePartitions().isEmpty());
    }

    /**
     * Verifies N:1 mapping: multiple txnEnd for the same ACTIVE partition
     * stays in TXN_END_RECEIVED without error.
     */
    @Test
    public void testMultipleTxnEndForSamePartition() {
        PartitionCommitTracker tracker = new PartitionCommitTracker(0);

        tracker.onWrite(0);
        boolean first = tracker.onTxnEnd(0);
        Assert.assertTrue("First txnEnd should trigger switch", first);

        // Second txnEnd while still TXN_END_RECEIVED (N:1 accumulation)
        boolean second = tracker.onTxnEnd(0);
        Assert.assertFalse("Second txnEnd should not trigger switch again", second);
    }

    /**
     * Verifies that allSwitched() blocks when only some partitions are switched.
     */
    @Test
    public void testAllSwitchedBlocksOnPartialSwitch() {
        PartitionCommitTracker tracker = new PartitionCommitTracker(0);

        tracker.onWrite(0);
        tracker.onWrite(1);

        tracker.onTxnEnd(0);
        tracker.markSwitched(0);

        // Partition 1 is still ACTIVE
        Assert.assertFalse("allSwitched should be false with one ACTIVE", tracker.allSwitched());

        tracker.onTxnEnd(1);
        tracker.markSwitched(1);
        Assert.assertTrue("allSwitched should be true when all switched", tracker.allSwitched());
    }

    /**
     * Verifies that getReadyToSwitch() respects the commit interval.
     */
    @Test
    public void testGetReadyToSwitchRespectsInterval() throws InterruptedException {
        // 200ms interval
        PartitionCommitTracker tracker = new PartitionCommitTracker(200);

        tracker.onWrite(0);
        tracker.onTxnEnd(0);

        // Immediately after construction, interval has not elapsed
        List<Integer> ready = tracker.getReadyToSwitch();
        Assert.assertTrue("Should not be ready before interval elapses", ready.isEmpty());

        Thread.sleep(250);

        ready = tracker.getReadyToSwitch();
        Assert.assertEquals("Should be ready after interval elapses", 1, ready.size());
    }
}
