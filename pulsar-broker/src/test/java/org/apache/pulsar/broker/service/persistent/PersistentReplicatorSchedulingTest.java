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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.AbstractReplicator.State;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class PersistentReplicatorSchedulingTest {

    private static final long OBSERVATION_MILLIS = TimeUnit.SECONDS.toMillis(800);

    @DataProvider
    public Object[][] connectionDeadline() {
        return new Object[][] {{3_000L, 2_100L}, {1_000L, 100L}, {0L, 100L}};
    }

    @Test(dataProvider = "connectionDeadline")
    public void testReadRetryUsesClockAndCoalescesOnBrokerExecutor(long connectionDeadline, long expectedDelay)
            throws Exception {
        Fixture fixture = new Fixture(false);
        SchedulingReplicator replicator = fixture.newReplicator(connectionDeadline);
        PersistentReplicator.InFlightTask context =
                replicator.createOrRecycleInFlightTaskIntoQueue(PositionFactory.EARLIEST, 1);

        replicator.readEntriesComplete(List.of(), context);
        replicator.readMoreEntries();
        replicator.readMoreEntries();

        ArgumentCaptor<Runnable> timer = ArgumentCaptor.forClass(Runnable.class);
        verify(fixture.brokerExecutor).schedule(timer.capture(), eq(expectedDelay), eq(TimeUnit.MILLISECONDS));
        verify(fixture.brokerExecutor).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        assertThat(replicator.lastPublishTime()).isEqualTo(1_000L);

        // The real timer callback clears the coalescing flag before requesting the next owner turn.
        fixture.scheduler.now.set(1_000L + expectedDelay);
        timer.getValue().run();
        ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);
        verify(fixture.brokerExecutor, times(2))
                .schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.MILLISECONDS));
        assertThat(delays.getAllValues()).containsExactly(expectedDelay, 100L);
        assertThat(replicator.lastPublishTime()).isEqualTo(1_000L);

        fixture.producerCreation.complete(fixture.producer);

        assertThat(replicator.replicatedBatches).isEqualTo(1);
        assertThat(replicator.lastPublishTime()).isEqualTo(1_000L + expectedDelay);
    }

    @Test
    public void testDeleteRetryPreservesPulsarExecutorAndDelay() throws Exception {
        Fixture fixture = new Fixture(true);
        SchedulingReplicator replicator = fixture.newReplicator(0);
        Position deletedPosition = PositionFactory.create(1, 2);
        when(fixture.cursor.getMarkDeletedPosition()).thenReturn(PositionFactory.create(1, 1));

        replicator.deleteFailed(new ManagedLedgerException("injected delete failure"), deletedPosition);

        assertThat(fixture.scheduler.tasks).hasSize(1);
        ScheduledTask retry = fixture.scheduler.tasks.element();
        assertThat(retry.executor()).isSameAs(fixture.pulsarExecutor);
        assertThat(retry.delay()).isEqualTo(10L);
        assertThat(retry.unit()).isEqualTo(TimeUnit.SECONDS);
        verify(fixture.cursor, never()).asyncDelete(deletedPosition, replicator, deletedPosition);
        fixture.scheduler.advanceTo(retry.deadlineMillis());
        verify(fixture.cursor).asyncDelete(deletedPosition, replicator, deletedPosition);
    }

    @Test
    public void testReadFailureRetriesOnBrokerExecutorWithOriginalBackoff() throws Exception {
        Fixture fixture = new Fixture(true);
        SchedulingReplicator replicator = fixture.newReplicator(0);
        fixture.producerCreation.complete(fixture.producer);
        assertThat(replicator.getState()).isEqualTo(State.Started);
        assertThat(fixture.cursorReads).hasSize(1);
        PersistentReplicator.InFlightTask context = fixture.cursorReads.get(0);

        replicator.readEntriesFailed(new ManagedLedgerException("injected read failure"), context);

        assertThat(context.isDone()).isTrue();
        assertThat(fixture.scheduler.tasks).hasSize(1);
        ScheduledTask firstRetry = fixture.scheduler.tasks.element();
        assertThat(firstRetry.executor()).isSameAs(fixture.brokerExecutor);
        assertThat(firstRetry.unit()).isEqualTo(TimeUnit.MILLISECONDS);
        // The original one-second read backoff includes symmetric five-percent jitter.
        assertThat(firstRetry.delay()).isBetween(950L, 1_050L);
        assertThat(fixture.cursorReads).hasSize(1);
        fixture.scheduler.advanceTo(firstRetry.deadlineMillis());
        assertThat(fixture.cursorReads).hasSize(2);

        PersistentReplicator.InFlightTask nextContext = fixture.cursorReads.get(1);
        replicator.readEntriesFailed(new ManagedLedgerException.TooManyRequestsException("throttled"), nextContext);

        assertThat(fixture.scheduler.tasks).hasSize(1);
        ScheduledTask secondRetry = fixture.scheduler.tasks.element();
        assertThat(secondRetry.executor()).isSameAs(fixture.brokerExecutor);
        assertThat(secondRetry.unit()).isEqualTo(TimeUnit.MILLISECONDS);
        assertThat(secondRetry.delay()).isBetween(1_900L, 2_100L);
        fixture.scheduler.advanceTo(secondRetry.deadlineMillis());
        assertThat(fixture.cursorReads).hasSize(3);
        assertThat(fixture.scheduler.tasks).isEmpty();
    }

    @Test
    public void testLateReadCompletionRunsRetriesThroughObservationBoundary() throws Exception {
        Fixture fixture = new Fixture(true);
        SchedulingReplicator replicator = fixture.newReplicator(0);
        long end = fixture.scheduler.now.get() + OBSERVATION_MILLIS;
        PersistentReplicator.InFlightTask context =
                replicator.createOrRecycleInFlightTaskIntoQueue(PositionFactory.EARLIEST, 1);
        fixture.scheduler.schedule(() -> replicator.readEntriesComplete(List.of(), context), null,
                OBSERVATION_MILLIS - 1000, TimeUnit.MILLISECONDS);
        fixture.scheduler.schedule(() -> fixture.producerCreation.complete(fixture.producer), null,
                OBSERVATION_MILLIS + 1, TimeUnit.MILLISECONDS);

        fixture.scheduler.advanceTo(end);

        // The late completion creates ten 100 ms retries, including the one exactly at the boundary.
        assertThat(fixture.scheduler.executedTasks).isEqualTo(11);
        assertThat(fixture.scheduler.tasks).hasSize(2);
        assertThat(replicator.getState()).isEqualTo(State.Starting);
        assertThat(replicator.replicatedBatches).isZero();
        assertThat(fixture.cursorReads).isEmpty();

        fixture.scheduler.advanceTo(end + 1);

        assertThat(replicator.replicatedBatches).isEqualTo(1);
        assertThat(replicator.lastPublishTime()).isEqualTo(end + 1);
        assertThat(fixture.cursorReads).hasSize(1);
        assertThat(fixture.scheduler.tasks).hasSize(1);
        ScheduledTask remainingRetry = fixture.scheduler.tasks.element();
        assertThat(remainingRetry.deadlineMillis()).isEqualTo(end + 100);
        assertThat(remainingRetry.executor()).isSameAs(fixture.brokerExecutor);
        assertThat(remainingRetry.delay()).isEqualTo(100L);
        assertThat(remainingRetry.unit()).isEqualTo(TimeUnit.MILLISECONDS);

        fixture.scheduler.advanceTo(end + 100);

        assertThat(fixture.cursorReads).hasSize(1);
        assertThat(fixture.scheduler.tasks).isEmpty();
    }

    @Test
    public void testInactivityDisconnectUsesClockAndStrictThreshold() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.config.setBrokerReplicationInactiveThresholdSeconds(2);
        SchedulingReplicator replicator = fixture.newReplicator(0);
        fixture.producerCreation.complete(fixture.producer);
        assertThat(replicator.getState()).isEqualTo(State.Started);
        assertThat(replicator.lastPublishTime()).isEqualTo(1_000L);

        fixture.scheduler.now.set(2_999L);
        replicator.disconnectIfNoTrafficAndBacklog();
        fixture.scheduler.now.set(3_000L);
        replicator.disconnectIfNoTrafficAndBacklog();

        assertThat(replicator.getState()).isEqualTo(State.Started);
        verify(fixture.producer, never()).closeAsync();

        fixture.scheduler.now.set(3_001L);
        replicator.disconnectIfNoTrafficAndBacklog();

        verify(fixture.producer).closeAsync();
        assertThat(replicator.getState()).isEqualTo(State.Disconnected);
    }

    private record ScheduledTask(long deadlineMillis, long sequence, Runnable task,
                                 ScheduledExecutorService executor, long delay, TimeUnit unit) {
    }

    // Drives replication retry timers and injected completions, not client reconnect or heartbeat timers.
    private static final class ReplicationScheduler {
        private final AtomicLong now = new AtomicLong(1_000L);
        private final Queue<ScheduledTask> tasks = new PriorityQueue<>(Comparator
                .comparingLong(ScheduledTask::deadlineMillis).thenComparingLong(ScheduledTask::sequence));
        private long sequence;
        private int executedTasks;

        private void schedule(Runnable task, ScheduledExecutorService executor, long delay, TimeUnit unit) {
            tasks.add(new ScheduledTask(now.get() + unit.toMillis(delay), sequence++, task, executor, delay, unit));
        }

        private void advanceTo(long deadlineMillis) {
            assertThat(deadlineMillis).isGreaterThanOrEqualTo(now.get());
            while (!tasks.isEmpty() && tasks.element().deadlineMillis() <= deadlineMillis) {
                ScheduledTask task = tasks.remove();
                now.set(task.deadlineMillis());
                assertThat(++executedTasks).as("bound recursive retry execution").isLessThan(10_000);
                task.task().run();
            }
            now.set(deadlineMillis);
        }
    }

    private static final class Fixture {
        private final ReplicationScheduler scheduler = new ReplicationScheduler();
        private final boolean controlScheduling;
        private final Clock clock = mock(Clock.class);
        private final BrokerService broker = mock(BrokerService.class);
        private final ManagedCursor cursor = mock(ManagedCursor.class);
        private final PersistentTopic topic = mock(PersistentTopic.class);
        private final PulsarClientImpl client = mock(PulsarClientImpl.class);
        private final ScheduledExecutorService pulsarExecutor = mock(ScheduledExecutorService.class);
        private final EventLoopGroup brokerExecutor = mock(EventLoopGroup.class);
        private final ServiceConfiguration config = new ServiceConfiguration();
        private final CompletableFuture<Producer<byte[]>> producerCreation = new CompletableFuture<>();
        private final List<PersistentReplicator.InFlightTask> cursorReads = new ArrayList<>();
        @SuppressWarnings("unchecked")
        private final ProducerImpl<byte[]> producer = mock(ProducerImpl.class);

        @SuppressWarnings({"deprecation", "unchecked"})
        private Fixture(boolean controlScheduling) throws Exception {
            this.controlScheduling = controlScheduling;
            PulsarService pulsar = mock(PulsarService.class);
            ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
            when(clock.millis()).thenAnswer(invocation -> scheduler.now.get());
            when(broker.getClock()).thenReturn(clock);
            when(broker.executor()).thenReturn(brokerExecutor);
            when(broker.pulsar()).thenReturn(pulsar);
            when(broker.getPulsar()).thenReturn(pulsar);
            when(pulsar.getClient()).thenReturn(client);
            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getConfig()).thenReturn(config);
            when(pulsar.getExecutor()).thenReturn(pulsarExecutor);
            when(topic.getName()).thenReturn("persistent://tenant/namespace/topic");
            when(topic.getReplicatorPrefix()).thenReturn("pulsar.repl");
            when(topic.getBrokerService()).thenReturn(broker);
            when(topic.getMaxReadPosition()).thenReturn(PositionFactory.EARLIEST);
            when(cursor.getName()).thenReturn("pulsar.repl.remote");
            when(cursor.getReadPosition()).thenReturn(PositionFactory.EARLIEST);
            when(cursor.getManagedLedger()).thenReturn(ledger);
            when(ledger.getLastConfirmedEntry()).thenReturn(PositionFactory.EARLIEST);
            doAnswer(invocation -> {
                cursorReads.add(invocation.getArgument(3));
                return null;
            }).when(cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
            when(producer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            ProducerBuilderImpl<byte[]> producerBuilder = new ProducerBuilderImpl<>(client, Schema.BYTES) {
                @Override
                public CompletableFuture<Producer<byte[]>> createAsync() {
                    // Keep producer creation in progress until the test completes it.
                    return producerCreation;
                }
            };
            when(client.newProducer(any(Schema.class))).thenReturn(producerBuilder);
        }

        private SchedulingReplicator newReplicator(long connectionDeadline) throws PulsarServerException {
            return new SchedulingReplicator(this, connectionDeadline);
        }
    }

    private static final class SchedulingReplicator extends PersistentReplicator {
        private final Fixture fixture;
        private int replicatedBatches;

        private SchedulingReplicator(Fixture fixture, long connectionDeadline) throws PulsarServerException {
            super("local", fixture.topic, fixture.cursor, "remote", fixture.topic.getName(),
                    fixture.broker, fixture.client, null);
            this.fixture = fixture;
            estimatedTimeStampProducerConnected = connectionDeadline;
        }

        @Override
        protected String getProducerName() {
            return "pulsar.repl.test";
        }

        @Override
        protected boolean replicateEntries(List<Entry> entries, InFlightTask inFlightTask) {
            replicatedBatches++;
            return false;
        }

        @Override
        protected void scheduleTask(Runnable task, ScheduledExecutorService executor, long delay, TimeUnit unit) {
            if (fixture.controlScheduling) {
                fixture.scheduler.schedule(task, executor, delay, unit);
            } else {
                super.scheduleTask(task, executor, delay, unit);
            }
        }

        private long lastPublishTime() {
            return latestPublishTime;
        }
    }
}
