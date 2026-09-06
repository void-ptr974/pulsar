/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.BrokerTestUtil.createMockConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.service.DrainingHashesTracker.DrainingHashEntry;
import org.apache.pulsar.broker.service.DrainingHashesTracker.UnblockingHandler;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Exercises concurrent tracker calls directly. Dispatcher topology changes have additional locking,
 * so these tests alone do not establish that consumer churn can produce these interleavings.
 */
public class DrainingHashesTrackerConcurrencyTest {
    @Test(timeOut = 30000)
    public void lastAckShouldCompleteWhenOwnerReclaimsHash() throws Exception {
        Consumer owner = createMockConsumer("owner");
        Consumer other = createMockConsumer("other");
        UnblockingHandler handler = mock(UnblockingHandler.class);
        PausingTracker tracker = new PausingTracker(handler);
        int hash = 1;
        tracker.addEntry(owner, hash);
        DrainingHashEntry entry = tracker.getEntry(hash);
        assertThat(entry.getRefCount()).isEqualTo(1);
        assertThat(tracker.shouldBlockStickyKeyHash(other, hash)).isTrue();

        LookupPause pause = tracker.pauseNextLookup();
        ExecutorService executor = newExecutor();
        try {
            Future<?> ack = executor.submit(() -> tracker.reduceRefCount(owner, hash, false));
            pause.awaitCaptured();
            assertThat(tracker.shouldBlockStickyKeyHash(owner, hash)).isFalse();
            assertThat(tracker.getEntry(hash)).isNull();
            pause.resume();

            // Unpatched code throws ExecutionException caused by the NPE at removed.isBlocking().
            ack.get(10, TimeUnit.SECONDS);

            assertThat(entry.getRefCount()).isNotNegative();
            assertThat(tracker.getEntry(hash)).isNull();
            assertStats(tracker, owner, 0, 1, 0);
            verifyNoInteractions(handler);
        } finally {
            pause.resume();
            shutdown(executor);
        }
    }

    @Test(timeOut = 30000)
    public void ownerReassignmentShouldNotClearStatsAfterLastAckAlreadyRemovedHash() throws Exception {
        Consumer owner = createMockConsumer("owner");
        Consumer other = createMockConsumer("other");
        UnblockingHandler handler = mock(UnblockingHandler.class);
        PausingTracker tracker = new PausingTracker(handler);
        int hash = 1;
        tracker.addEntry(owner, hash);
        assertThat(tracker.shouldBlockStickyKeyHash(other, hash)).isTrue();

        LookupPause pause = tracker.pauseNextLookup();
        ExecutorService executor = newExecutor();
        try {
            Future<Boolean> reassignment = executor.submit(() -> tracker.shouldBlockStickyKeyHash(owner, hash));
            pause.awaitCaptured();
            tracker.reduceRefCount(owner, hash, false);
            assertStats(tracker, owner, 0, 1, 0);
            pause.resume();

            assertThat(reassignment.get(10, TimeUnit.SECONDS)).isFalse();
            assertThat(tracker.getEntry(hash)).isNull();
            assertStats(tracker, owner, 0, 1, 0);
            verify(handler).stickyKeyHashUnblocked(hash);
            verifyNoMoreInteractions(handler);
        } finally {
            pause.resume();
            shutdown(executor);
        }
    }

    @DataProvider(name = "replacementOwners")
    public Object[][] replacementOwners() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "replacementOwners", timeOut = 30000)
    public void staleAckShouldNotRemoveReplacementEntry(boolean sameOwner) throws Exception {
        Consumer owner = createMockConsumer("owner");
        Consumer replacementOwner = sameOwner ? owner : createMockConsumer("replacement-owner");
        UnblockingHandler handler = mock(UnblockingHandler.class);
        PausingTracker tracker = new PausingTracker(handler);
        int hash = 1;
        tracker.addEntry(owner, hash);
        DrainingHashEntry oldEntry = tracker.getEntry(hash);

        LookupPause pause = tracker.pauseNextLookup();
        ExecutorService executor = newExecutor();
        try {
            Future<?> ack = executor.submit(() -> tracker.reduceRefCount(owner, hash, false));
            pause.awaitCaptured();
            assertThat(tracker.shouldBlockStickyKeyHash(owner, hash)).isFalse();
            tracker.addEntry(replacementOwner, hash);
            DrainingHashEntry replacement = tracker.getEntry(hash);
            assertThat(replacement).isNotSameAs(oldEntry);
            pause.resume();
            ack.get(10, TimeUnit.SECONDS);

            assertThat(tracker.getEntry(hash)).isSameAs(replacement);
            assertThat(replacement.getConsumer()).isSameAs(replacementOwner);
            assertThat(replacement.getRefCount()).isEqualTo(1);
            assertThat(oldEntry.getRefCount()).isNotNegative();
            assertStats(tracker, owner, sameOwner ? 1 : 0, 1, sameOwner ? 1 : 0);
            if (!sameOwner) {
                assertStats(tracker, replacementOwner, 1, 0, 1);
            }
            verifyNoInteractions(handler);
        } finally {
            pause.resume();
            shutdown(executor);
        }
    }

    @Test(dataProvider = "replacementOwners", timeOut = 30000)
    public void staleNonFinalAckShouldNotAffectReplacementEntry(boolean sameOwner) throws Exception {
        Consumer owner = createMockConsumer("owner");
        Consumer replacementOwner = sameOwner ? owner : createMockConsumer("replacement-owner");
        UnblockingHandler handler = mock(UnblockingHandler.class);
        PausingTracker tracker = new PausingTracker(handler);
        int hash = 1;
        tracker.addEntry(owner, hash);
        tracker.addEntry(owner, hash);
        DrainingHashEntry oldEntry = tracker.getEntry(hash);
        assertThat(oldEntry.getRefCount()).isEqualTo(2);

        LookupPause pause = tracker.pauseNextLookup();
        ExecutorService executor = newExecutor();
        try {
            Future<?> ack = executor.submit(() -> tracker.reduceRefCount(owner, hash, false));
            pause.awaitCaptured();
            assertThat(tracker.shouldBlockStickyKeyHash(owner, hash)).isFalse();
            tracker.addEntry(replacementOwner, hash);
            DrainingHashEntry replacement = tracker.getEntry(hash);
            assertThat(replacement).isNotSameAs(oldEntry);
            pause.resume();
            ack.get(10, TimeUnit.SECONDS);

            assertThat(oldEntry.getRefCount()).isEqualTo(1);
            assertThat(tracker.getEntry(hash)).isSameAs(replacement);
            assertThat(replacement.getConsumer()).isSameAs(replacementOwner);
            assertThat(replacement.getRefCount()).isEqualTo(1);
            assertStats(tracker, owner, sameOwner ? 1 : 0, 1, sameOwner ? 1 : 0);
            if (!sameOwner) {
                assertStats(tracker, replacementOwner, 1, 0, 1);
            }
            verifyNoInteractions(handler);
        } finally {
            pause.resume();
            shutdown(executor);
        }
    }

    @Test(timeOut = 30000)
    public void concurrentReductionsShouldRemoveAndNotifyOnce() throws Exception {
        Consumer owner = createMockConsumer("owner");
        Consumer other = createMockConsumer("other");
        UnblockingHandler handler = mock(UnblockingHandler.class);
        DrainingHashesTracker tracker = new DrainingHashesTracker("dispatcher", handler);
        int hash = 1;
        int workerCount = 8;
        int reductionsPerWorker = 8;
        for (int i = 0; i < workerCount * reductionsPerWorker; i++) {
            tracker.addEntry(owner, hash);
        }
        DrainingHashEntry entry = tracker.getEntry(hash);
        assertThat(tracker.shouldBlockStickyKeyHash(other, hash)).isTrue();

        ExecutorService executor = newExecutor(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        Future<?>[] workers = new Future<?>[workerCount];
        try {
            for (int i = 0; i < workerCount; i++) {
                workers[i] = executor.submit(() -> {
                    await(start);
                    for (int j = 0; j < reductionsPerWorker; j++) {
                        tracker.reduceRefCount(owner, hash, false);
                    }
                });
            }
            start.countDown();
            for (Future<?> worker : workers) {
                worker.get(10, TimeUnit.SECONDS);
            }

            assertThat(entry.getRefCount()).isZero();
            assertThat(tracker.getEntry(hash)).isNull();
            assertStats(tracker, owner, 0, 1, 0);
            verify(handler).stickyKeyHashUnblocked(hash);
            verifyNoMoreInteractions(handler);
        } finally {
            start.countDown();
            shutdown(executor);
        }
    }

    @Test(timeOut = 30000)
    public void twoCapturedLastAcksShouldOnlyRemoveAndNotifyOnce() throws Exception {
        Consumer owner = createMockConsumer("owner");
        Consumer other = createMockConsumer("other");
        UnblockingHandler handler = mock(UnblockingHandler.class);
        PausingTracker tracker = new PausingTracker(handler);
        int hash = 1;
        tracker.addEntry(owner, hash);
        DrainingHashEntry entry = tracker.getEntry(hash);
        assertThat(tracker.shouldBlockStickyKeyHash(other, hash)).isTrue();

        LookupPause firstPause = tracker.pauseNextLookup();
        LookupPause secondPause = null;
        ExecutorService executor = newExecutor();
        try {
            Future<?> firstAck = executor.submit(() -> tracker.reduceRefCount(owner, hash, false));
            firstPause.awaitCaptured();
            secondPause = tracker.pauseNextLookup();
            Future<?> secondAck = executor.submit(() -> tracker.reduceRefCount(owner, hash, false));
            secondPause.awaitCaptured();
            firstPause.resume();
            firstAck.get(10, TimeUnit.SECONDS);
            secondPause.resume();
            secondAck.get(10, TimeUnit.SECONDS);

            assertThat(entry.getRefCount()).isZero();
            assertThat(tracker.getEntry(hash)).isNull();
            assertStats(tracker, owner, 0, 1, 0);
            verify(handler).stickyKeyHashUnblocked(hash);
            verifyNoMoreInteractions(handler);
        } finally {
            firstPause.resume();
            if (secondPause != null) {
                secondPause.resume();
            }
            shutdown(executor);
        }
    }

    private static ExecutorService newExecutor() {
        return newExecutor(2);
    }

    private static ExecutorService newExecutor(int threadCount) {
        return Executors.newFixedThreadPool(threadCount, new DefaultThreadFactory("draining-hash-test"));
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).as("Workers must start together").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting to start", e);
        }
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).as("Test threads must terminate").isTrue();
    }

    private static void assertStats(DrainingHashesTracker tracker, Consumer consumer,
                                    int count, long clearedTotal, int unackedMessages) {
        ConsumerStatsImpl stats = new ConsumerStatsImpl();
        tracker.updateConsumerStats(consumer, stats);
        assertThat(stats.drainingHashesCount).isEqualTo(count);
        assertThat(stats.drainingHashesClearedTotal).isEqualTo(clearedTotal);
        assertThat(stats.drainingHashesUnackedMessages).isEqualTo(unackedMessages);
    }

    private static class PausingTracker extends DrainingHashesTracker {
        private final AtomicReference<LookupPause> nextPause = new AtomicReference<>();

        PausingTracker(UnblockingHandler handler) {
            super("dispatcher", handler);
        }

        LookupPause pauseNextLookup() {
            LookupPause pause = new LookupPause();
            assertThat(nextPause.compareAndSet(null, pause)).as("Previous lookup pause must be consumed").isTrue();
            return pause;
        }

        @Override
        public DrainingHashEntry getEntry(int stickyKeyHash) {
            DrainingHashEntry entry = super.getEntry(stickyKeyHash);
            LookupPause pause = nextPause.getAndSet(null);
            if (pause != null) {
                pause.captured.countDown();
                try {
                    assertThat(pause.resumed.await(10, TimeUnit.SECONDS)).as("Paused lookup must resume").isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while waiting to resume lookup", e);
                }
            }
            return entry;
        }
    }

    private static class LookupPause {
        private final CountDownLatch captured = new CountDownLatch(1);
        private final CountDownLatch resumed = new CountDownLatch(1);

        void awaitCaptured() throws InterruptedException {
            assertThat(captured.await(10, TimeUnit.SECONDS)).as("Worker must capture the real entry").isTrue();
        }

        void resume() {
            resumed.countDown();
        }
    }
}
