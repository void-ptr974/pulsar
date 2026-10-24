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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class ReplicatorSchedulingTest {

    private static final String TOPIC_NAME = "persistent://public/default/replicator-scheduling";
    private static final long OBSERVATION_MILLIS = TimeUnit.SECONDS.toMillis(800);

    @Test
    public void testDefaultSchedulingPreservesExecutorAndDelay() throws Exception {
        Fixture fixture = new Fixture(false);
        Runnable task = mock(Runnable.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        fixture.replicator.scheduleTask(task, executor, 37, TimeUnit.SECONDS);
        verify(executor).schedule(task, 37, TimeUnit.SECONDS);

        fixture.replicator.scheduleCheckTopicActiveAndStartProducer(1234);
        verify(fixture.executor).schedule(any(Runnable.class), eq(1234L), eq(TimeUnit.MILLISECONDS));
        assertThat(fixture.creationTimes).isEmpty();
    }

    @Test
    public void testProducerFailureRetriesThroughoutObservationPeriod() throws Exception {
        Fixture fixture = new Fixture(true);
        long start = fixture.scheduler.now.get();
        assertThat(fixture.replicator.latestPublishTime).isEqualTo(start);

        fixture.replicator.startProducer();
        assertThat(fixture.replicator.estimatedTimeStampProducerConnected)
                .isEqualTo(fixture.scheduler.nextDeadlineMillis());

        fixture.scheduler.advanceTo(start + OBSERVATION_MILLIS);

        assertThat(fixture.creationTimes).hasSizeGreaterThan(10).isSorted();
        assertThat(fixture.creationTimes.get(fixture.creationTimes.size() - 1))
                .isBetween(start + OBSERVATION_MILLIS - 63_000, start + OBSERVATION_MILLIS);
        assertThat(fixture.scheduler.executedTasks).isEqualTo(fixture.creationTimes.size() - 1);
        assertThat(fixture.scheduler.tasks).hasSize(1);
        assertThat(fixture.scheduler.nextDeadlineMillis()).isGreaterThan(start + OBSERVATION_MILLIS);
        assertThat(fixture.replicator.estimatedTimeStampProducerConnected)
                .isEqualTo(fixture.scheduler.nextDeadlineMillis());
        verifyNoInteractions(fixture.executor);
    }

    @Test
    public void testLateFailureRunsNestedRetriesBeforeObservationEnds() throws Exception {
        Fixture fixture = new Fixture(true);
        long start = fixture.scheduler.now.get();
        CompletableFuture<Producer<byte[]>> lateFailure = new CompletableFuture<>();
        fixture.producerCreation = lateFailure;
        fixture.replicator.startProducer();
        fixture.scheduler.schedule(() -> {
            fixture.producerCreation = CompletableFuture.failedFuture(new IllegalStateException("producer failed"));
            lateFailure.completeExceptionally(new IllegalStateException("late producer failure"));
        }, OBSERVATION_MILLIS - 1000, TimeUnit.MILLISECONDS);
        Runnable atBoundary = mock(Runnable.class);
        Runnable afterBoundary = mock(Runnable.class);
        fixture.scheduler.schedule(atBoundary, OBSERVATION_MILLIS, TimeUnit.MILLISECONDS);
        fixture.scheduler.schedule(afterBoundary, OBSERVATION_MILLIS + 1, TimeUnit.MILLISECONDS);

        fixture.scheduler.advanceTo(start + OBSERVATION_MILLIS);

        // Even with the default backoff jitter, the 100/200/400 ms retries all fit in the last second.
        assertThat(fixture.creationTimes).hasSize(4);
        assertThat(fixture.creationTimes.subList(1, 4))
                .allSatisfy(time -> assertThat(time).isBetween(start + OBSERVATION_MILLIS - 1000,
                        start + OBSERVATION_MILLIS));
        assertThat(fixture.scheduler.tasks).hasSize(2);
        assertThat(fixture.replicator.estimatedTimeStampProducerConnected)
                .isGreaterThan(start + OBSERVATION_MILLIS);
        verify(atBoundary).run();
        verifyNoInteractions(afterBoundary);

        fixture.scheduler.advanceTo(start + OBSERVATION_MILLIS + 1);
        verify(afterBoundary).run();
        assertThat(fixture.creationTimes).hasSize(4);
        assertThat(fixture.scheduler.tasks).hasSize(1);
    }

    @Test
    public void testQueuedRetryDoesNotRestartTerminatedReplicator() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.replicator.startProducer();
        assertThat(fixture.scheduler.tasks).hasSize(1);
        assertThat(fixture.replicator.terminate()).isCompleted();
        assertThat(fixture.replicator.getState()).isEqualTo(AbstractReplicator.State.Terminated);

        fixture.scheduler.advanceTo(fixture.scheduler.now.get() + OBSERVATION_MILLIS);

        assertThat(fixture.scheduler.executedTasks).isEqualTo(1);
        assertThat(fixture.scheduler.tasks).isEmpty();
        assertThat(fixture.replicator.getState()).isEqualTo(AbstractReplicator.State.Terminated);
        verify(fixture.producerBuilder, times(1)).createAsync();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLateProducerCloseFailureRunsRetriesBeforeObservationEnds() throws Exception {
        Fixture fixture = new Fixture(true);
        long start = fixture.scheduler.now.get();
        Producer<byte[]> producer = mock(Producer.class);
        CompletableFuture<Void> lateClose = new CompletableFuture<>();
        AtomicInteger closeAttempts = new AtomicInteger();
        Runnable afterClosed = mock(Runnable.class);
        when(producer.closeAsync()).thenAnswer(invocation -> switch (closeAttempts.incrementAndGet()) {
            case 1 -> lateClose;
            case 2, 3 -> CompletableFuture.failedFuture(new IllegalStateException("close failed"));
            default -> CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> closing = fixture.replicator.doCloseProducerAsync(producer, afterClosed);
        fixture.scheduler.schedule(() -> lateClose.completeExceptionally(new IllegalStateException("late close")),
                OBSERVATION_MILLIS - 1000, TimeUnit.MILLISECONDS);
        assertThat(closing).isNotDone();
        verifyNoInteractions(afterClosed);

        fixture.scheduler.advanceTo(start + OBSERVATION_MILLIS);

        assertThat(closeAttempts).hasValue(4);
        assertThat(closing).isCompleted();
        assertThat(fixture.scheduler.tasks).isEmpty();
        verify(afterClosed).run();
        verifyNoInteractions(fixture.executor);
    }

    @Test
    public void testCloseWaitsForPendingProducerCreationAndStopsAfterTermination() throws Exception {
        Fixture fixture = new Fixture(true);
        CompletableFuture<Producer<byte[]>> creation = new CompletableFuture<>();
        fixture.producerCreation = creation;
        fixture.replicator.startProducer();
        assertThat(fixture.replicator.getState()).isEqualTo(AbstractReplicator.State.Starting);

        assertThat(fixture.replicator.closeProducerAsync(false)).isCompleted();
        assertThat(fixture.scheduler.tasks).isEmpty();
        assertThat(fixture.replicator.closeProducerAsync(true)).isCompleted();
        assertThat(fixture.scheduler.tasks).hasSize(1);
        long firstDeadline = fixture.scheduler.nextDeadlineMillis();
        assertThat(firstDeadline - fixture.scheduler.now.get()).isBetween(95L, 105L);

        fixture.scheduler.advanceTo(firstDeadline);

        assertThat(fixture.replicator.getState()).isEqualTo(AbstractReplicator.State.Starting);
        assertThat(fixture.scheduler.tasks).hasSize(1);
        assertThat(fixture.scheduler.nextDeadlineMillis() - firstDeadline).isBetween(190L, 210L);
        assertThat(fixture.replicator.terminate()).isCompleted();
        creation.completeExceptionally(new IllegalStateException("creation failed after termination"));
        fixture.scheduler.advanceTo(firstDeadline + OBSERVATION_MILLIS);

        assertThat(fixture.scheduler.executedTasks).isEqualTo(2);
        assertThat(fixture.scheduler.tasks).isEmpty();
        assertThat(fixture.replicator.getState()).isEqualTo(AbstractReplicator.State.Terminated);
        verify(fixture.producerBuilder, times(1)).createAsync();
    }

    @Test
    public void testDeferredAsyncCallbackCanScheduleRetryAfterAdvanceReturns() throws Exception {
        Fixture fixture = new Fixture(true);
        long end = fixture.scheduler.now.get() + OBSERVATION_MILLIS;
        CompletableFuture<Producer<byte[]>> creation = new CompletableFuture<>();
        CompletableFuture<Void> releaseCallback = new CompletableFuture<>();
        List<CompletableFuture<Void>> callbacks = new ArrayList<>();
        ExecutorService callbackExecutor = Executors.newSingleThreadExecutor();
        try {
            fixture.producerCreation = creation;
            fixture.replicator.startProducer();
            fixture.scheduler.schedule(() -> callbacks.add(releaseCallback.thenRunAsync(
                    () -> creation.completeExceptionally(new IllegalStateException("deferred failure")),
                    callbackExecutor)), OBSERVATION_MILLIS - 1000, TimeUnit.MILLISECONDS);

            fixture.scheduler.advanceTo(end);

            assertThat(callbacks).hasSize(1);
            assertThat(callbacks.get(0)).isNotDone();
            assertThat(fixture.scheduler.tasks).isEmpty();
            assertThat(fixture.replicator.getState()).isEqualTo(AbstractReplicator.State.Starting);

            releaseCallback.complete(null);
            callbacks.get(0).get(5, TimeUnit.SECONDS);

            // Draining this queue does not drain independent executors or establish async quiescence.
            assertThat(fixture.scheduler.tasks).hasSize(1);
            assertThat(fixture.scheduler.nextDeadlineMillis()).isGreaterThan(end);
            assertThat(fixture.creationTimes).hasSize(1);
        } finally {
            callbackExecutor.shutdownNow();
            assertThat(callbackExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class Fixture {
        private final ReplicationScheduler scheduler = new ReplicationScheduler();
        private final EventLoopGroup executor = mock(EventLoopGroup.class);
        private final List<Long> creationTimes = new ArrayList<>();
        private final ProducerBuilderImpl<byte[]> producerBuilder;
        private final TestReplicator replicator;
        private CompletableFuture<Producer<byte[]>> producerCreation =
                CompletableFuture.failedFuture(new IllegalStateException("producer failed"));

        @SuppressWarnings({"deprecation", "unchecked"})
        private Fixture(boolean controlScheduling) throws PulsarServerException {
            BrokerService broker = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration configuration = new ServiceConfiguration();
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            PulsarAdmin admin = mock(PulsarAdmin.class);
            Topic topic = mock(Topic.class);
            Clock clock = mock(Clock.class);
            producerBuilder = mock(ProducerBuilderImpl.class, RETURNS_SELF);
            when(clock.millis()).thenAnswer(invocation -> scheduler.now.get());
            when(broker.getClock()).thenReturn(clock);
            when(broker.executor()).thenReturn(executor);
            when(broker.pulsar()).thenReturn(pulsar);
            when(pulsar.getConfiguration()).thenReturn(configuration);
            when(pulsar.getClient()).thenReturn(client);
            when(pulsar.getAdminClient()).thenReturn(admin);
            when(topic.getName()).thenReturn(TOPIC_NAME);
            when(client.newProducer(any(Schema.class))).thenReturn(producerBuilder);
            when(producerBuilder.getConf()).thenReturn(new ProducerConfigurationData());
            when(producerBuilder.createAsync()).thenAnswer(invocation -> {
                creationTimes.add(scheduler.now.get());
                return producerCreation;
            });
            var topics = new ConcurrentHashMap<String, CompletableFuture<Optional<Topic>>>();
            topics.put(TOPIC_NAME, CompletableFuture.completedFuture(Optional.of(topic)));
            when(broker.getTopics()).thenReturn(topics);
            replicator = new TestReplicator(topic, broker, client, admin,
                    controlScheduling ? scheduler : null);
            doReturn(Map.of("remote", replicator)).when(topic).getReplicators();
        }
    }

    // Controls replication retry tasks only; client reconnect and heartbeat timers remain independent.
    private static final class ReplicationScheduler {
        private final AtomicLong now = new AtomicLong(1_000_000);
        private final Queue<ScheduledTask> tasks = new PriorityQueue<>(
                Comparator.comparingLong(ScheduledTask::deadlineMillis).thenComparingLong(ScheduledTask::sequence));
        private long sequence;
        private int executedTasks;

        private void schedule(Runnable task, long delay, TimeUnit unit) {
            tasks.add(new ScheduledTask(now.get() + unit.toMillis(delay), sequence++, task));
        }

        private long nextDeadlineMillis() {
            return tasks.element().deadlineMillis();
        }

        private void advanceTo(long deadlineMillis) {
            assertThat(deadlineMillis).isGreaterThanOrEqualTo(now.get());
            while (!tasks.isEmpty() && tasks.element().deadlineMillis() <= deadlineMillis) {
                ScheduledTask next = tasks.remove();
                now.set(next.deadlineMillis());
                assertThat(++executedTasks).as("bound recursive retry execution").isLessThan(10_000);
                next.action().run();
            }
            now.set(deadlineMillis);
        }

        private record ScheduledTask(long deadlineMillis, long sequence, Runnable action) { }
    }

    private static final class TestReplicator extends AbstractReplicator {
        private final ReplicationScheduler scheduler;

        private TestReplicator(Topic topic, BrokerService broker, PulsarClientImpl client, PulsarAdmin admin,
                               ReplicationScheduler scheduler) throws PulsarServerException {
            super("local", topic, "remote", TOPIC_NAME, "pulsar.repl", broker, client, admin);
            this.scheduler = scheduler;
        }

        @Override
        protected void scheduleTask(Runnable task, ScheduledExecutorService executor, long delay, TimeUnit unit) {
            if (scheduler == null) {
                super.scheduleTask(task, executor, delay, unit);
            } else {
                scheduler.schedule(task, delay, unit);
            }
        }

        @Override
        protected String getProducerName() {
            return "pulsar.repl.producer";
        }

        @Override
        protected void setProducerAndTriggerReadEntries(Producer<byte[]> producer) { }

        @Override
        protected Position getReplicatorReadPosition() {
            return PositionFactory.EARLIEST;
        }

        @Override
        public long getNumberOfEntriesInBacklog() {
            return 0;
        }

        @Override
        protected void disableReplicatorRead() { }

        @Override
        protected void beforeTerminate() { }

        @Override
        public ReplicatorStatsImpl computeStats() {
            return new ReplicatorStatsImpl();
        }

        @Override
        public ReplicatorStatsImpl getStats() {
            return computeStats();
        }

        @Override
        public void updateRates() { }
    }
}
