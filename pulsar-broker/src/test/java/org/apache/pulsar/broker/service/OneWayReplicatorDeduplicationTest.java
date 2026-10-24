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

import static com.google.common.base.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.vertx.core.impl.ConcurrentHashSet;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.GeoPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.awaitility.Awaitility;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public class OneWayReplicatorDeduplicationTest extends OneWayReplicatorDeduplicationTestBase {

    @Override
    protected boolean acceptsDeduplicationCase(boolean supportsReplDedupByLidAndEid, boolean multiSchemas) {
        return supportsReplDedupByLidAndEid && !multiSchemas;
    }

    @Test
    public void testRepeatedlyPublishMixedMessageAndReplMarkers() throws Exception {
        AtomicBoolean stuckSendReceipt = new AtomicBoolean(true);
        Runnable cleanInjection = injectReplicatorClientCnx((conf, eventLoopGroup) -> new ClientCnx(
                InstrumentProvider.NOOP, conf, eventLoopGroup) {
            @Override
            protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
                if (!stuckSendReceipt.get()) {
                    super.handleSendReceipt(sendReceipt);
                }
            }
        });
        int snapshotFrequency1 = pulsar1.getConfig().getReplicatedSubscriptionsSnapshotFrequencyMillis();
        int snapshotFrequency2 = pulsar2.getConfig().getReplicatedSubscriptionsSnapshotFrequencyMillis();
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        final String subscription = "s1";
        Throwable testFailure = null;
        try {
            // Keep the real snapshot timers and marker protocol, and wait for their persisted output.
            pulsar1.getConfig().setReplicatedSubscriptionsSnapshotFrequencyMillis(100);
            pulsar2.getConfig().setReplicatedSubscriptionsSnapshotFrequencyMillis(100);
            admin1.topics().createNonPartitionedTopic(topicName);
            try (Consumer<Integer> consumer = client1.newConsumer(Schema.INT32).topic(topicName)
                    .subscriptionName(subscription).replicateSubscriptionState(true).subscribe()) {
                admin2.topics().createSubscription(topicName, subscription, MessageId.earliest);
            }
            waitReplicatorStarted(topicName);
            PersistentTopic source = (PersistentTopic) pulsar1.getBrokerService()
                    .getTopic(topicName, false).get().orElseThrow();
            PersistentTopic target = (PersistentTopic) pulsar2.getBrokerService()
                    .getTopic(topicName, false).get().orElseThrow();
            admin2.topicPolicies().setDeduplicationStatus(topicName, true);
            Awaitility.await().untilAsserted(() -> assertEquals(
                    String.valueOf(target.getMessageDeduplication().getStatus()), "Enabled"));
            assertTrue(source.getSubscriptions().get(subscription).isReplicated());
            assertTrue(source.getReplicatedSubscriptionController().isPresent());
            admin2.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));

            int messageCount = 15;
            try (Producer<Integer> producer = client1.newProducer(Schema.INT32).topic(topicName).create()) {
                for (int i = 0; i < messageCount; i++) {
                    MessageIdAdv messageId = (MessageIdAdv) producer.send(i);
                    awaitReplicationMarkerAfter(source, messageId, cluster2);
                }
            }
            GeoPersistentReplicator replicator = (GeoPersistentReplicator) source.getReplicators().get(cluster2);
            Awaitility.await().untilAsserted(() -> assertThat(
                    replicator.getCursor().getNumberOfEntriesInBacklog(true))
                    .as("unacknowledged user messages and interleaved persisted replication markers")
                    .isGreaterThanOrEqualTo(messageCount * 2L));

            // Reconnect the real producer so every unacknowledged message and marker is replayed.
            stuckSendReceipt.set(false);
            replicator.producer.getClientCnx().ctx().channel().close().get(10, TimeUnit.SECONDS);
            try (Producer<Integer> producer = client1.newProducer(Schema.INT32).topic(topicName).create()) {
                waitReplicatorStarted(topicName);
            }
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertEquals(
                    replicator.getCursor().getNumberOfEntriesInBacklog(true), 0));
            try (Consumer<Integer> consumer = client2.newConsumer(Schema.INT32).topic(topicName)
                    .subscriptionName(subscription).replicateSubscriptionState(true).subscribe()) {
                for (int i = 0; i < messageCount; i++) {
                    Message<Integer> message = consumer.receive(2, TimeUnit.SECONDS);
                    assertNotNull(message);
                    assertEquals(message.getValue(), i);
                }
                assertNoLateMessages(consumer);
            }
        } catch (Exception | AssertionError error) {
            testFailure = error;
            throw error;
        } finally {
            closeAfterTest(List.of(
                    () -> admin1.topics().deleteSubscription(topicName, subscription),
                    () -> admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1)),
                    () -> admin2.topics().setReplicationClusters(topicName, Arrays.asList(cluster2)),
                    () -> admin1.topics().unload(topicName),
                    () -> admin2.topics().unload(topicName),
                    () -> pulsar1.getConfig().setReplicatedSubscriptionsSnapshotFrequencyMillis(snapshotFrequency1),
                    () -> pulsar2.getConfig().setReplicatedSubscriptionsSnapshotFrequencyMillis(snapshotFrequency2),
                    cleanInjection::run), testFailure);
        }
    }

    private static void awaitReplicationMarkerAfter(PersistentTopic topic, MessageIdAdv messageId,
                                                     String remoteCluster) throws Exception {
        ManagedCursor cursor = topic.getManagedLedger().newNonDurableCursor(
                PositionFactory.create(messageId.getLedgerId(), messageId.getEntryId()));
        Throwable testFailure = null;
        try {
            Awaitility.await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10)).until(() -> {
                if (!cursor.hasMoreEntries()) {
                    return false;
                }
                List<Entry> entries = cursor.readEntries(1);
                try {
                    return entries.stream().anyMatch(entry -> {
                        MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer().duplicate());
                        return isOutboundReplicationMarker(metadata, remoteCluster);
                    });
                } finally {
                    entries.forEach(Entry::release);
                }
            });
        } catch (Exception | AssertionError error) {
            testFailure = error;
            throw error;
        } finally {
            closeAfterTest(List.of(cursor::close), testFailure);
        }
    }

    static boolean isOutboundReplicationMarker(MessageMetadata metadata, String remoteCluster) {
        // Echoed markers and snapshots restricted to this cluster are deleted locally without a send receipt.
        return metadata.hasMarkerType() && Markers.isReplicationMarker(metadata.getMarkerType())
                && !metadata.hasReplicatedFrom()
                && (metadata.getReplicateTosCount() == 0 || metadata.getReplicateTosList().contains(remoteCluster));
    }

    @DataProvider(name = "enabledDeduplication")
    public Object[][] enabledDeduplication() {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @DataProvider(name = "completionBarrierArgs")
    public Object[][] completionBarrierArgs() {
        return new Object[][] {{true, false, false}, {false, false, false}, {false, true, false},
                {false, false, true}};
    }

    @Test(timeOut = 120_000, dataProvider = "completionBarrierArgs")
    public void testDeduplicationCompletionBarrier(boolean enabledDeduplication,
                                                   boolean disconnectDuringPublish,
                                                   boolean publishDuringObservation) throws Exception {
        // Exercise the same close barrier as the matrix with a delayed duplicate and a delayed close response.
        String topicName = BrokerTestUtil.newUniqueName(
                "persistent://" + nonReplicatedNamespace + "/completion-barrier-");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createNonPartitionedTopic(topicName);
        // Retain every entry, so an extra persisted duplicate cannot disappear before validation.
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        PersistentTopic source = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(topicName, false).get().orElseThrow();
        PersistentTopic target = (PersistentTopic) pulsar2.getBrokerService()
                .getTopic(topicName, false).get().orElseThrow();
        admin2.topicPolicies().setDeduplicationStatus(topicName, true);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(String.valueOf(target.getMessageDeduplication().getStatus())).isEqualTo("Enabled"));

        CompletableFuture<ReplicationDrainClientCnx> publishingConnection = new CompletableFuture<>();
        CompletableFuture<Void> closeResponseReceived = new CompletableFuture<>();
        CompletableFuture<Void> releaseCloseResponse = new CompletableFuture<>();
        Runnable clearInjection = injectReplicatorClientCnx((conf, eventLoopGroup) ->
                new ReplicationDrainClientCnx(conf, eventLoopGroup, publishingConnection, () -> target) {
                    @Override
                    protected void handleSuccess(CommandSuccess success) {
                        if (hasPublished()) {
                            long requestId = success.getRequestId();
                            closeResponseReceived.complete(null);
                            releaseCloseResponse.thenRun(() -> ctx().executor().execute(() ->
                                    super.handleSuccess(new CommandSuccess().setRequestId(requestId))));
                        } else {
                            super.handleSuccess(success);
                        }
                    }
                });
        Throwable testFailure = null;
        try {
            List<String> expected = List.of("one", "two", "three", "four");
            try (Producer<String> producer = client1.newProducer(Schema.STRING).topic(topicName)
                    .enableBatching(false).create()) {
                for (String value : expected) {
                    producer.send(value);
                }
            }
            admin1.topics().setReplicationClusters(topicName, List.of(cluster1, cluster2));
            ReplicationDrainClientCnx connection = publishingConnection.get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                Replicator replicator = source.getReplicators().get(cluster2);
                assertThat(replicator).as("source replicator must exist").isNotNull();
                assertThat(((GeoPersistentReplicator) replicator).getCursor().getNumberOfEntriesInBacklog(true))
                        .as("original replication backlog").isZero();
            });
            connection.seal().get(10, TimeUnit.SECONDS);
            assertThat(connection.completion()).as("held duplicate must keep the barrier open").isNotDone();
            assertReplicatedEntryCount(target, expected.size());

            if (!enabledDeduplication) {
                // Negative control: let the real destination persist the held duplicate after cursor catch-up.
                admin2.topicPolicies().setDeduplicationStatus(topicName, false);
                Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                        assertThat(String.valueOf(target.getMessageDeduplication().getStatus()))
                                .isEqualTo("Disabled"));
            }
            if (disconnectDuringPublish) {
                try {
                    for (BookieServer bookie : bkEnsemble2.getBookies()) {
                        bookie.suspendProcessing();
                    }
                    connection.releaseDelayedDuplicate().get(10, TimeUnit.SECONDS);
                    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                            assertThat(target.getPendingWriteOps().get()).as("delayed destination write")
                                    .isPositive());
                    connection.allowDisconnectedRetirement().get(10, TimeUnit.SECONDS);
                    connection.ctx().channel().close().get(10, TimeUnit.SECONDS);
                    admin1.topics().setReplicationClusters(topicName, List.of(cluster1));
                    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                        assertThat(source.getReplicators()).doesNotContainKey(cluster2);
                        assertThat(connection.isDestinationDisconnected()).isTrue();
                    });
                    assertThat(target.getPendingWriteOps().get()).as("write outlives its socket").isPositive();
                    assertReplicatedEntryCount(target, expected.size());
                    assertThatThrownBy(() -> connection.retireDisconnected().get(10, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(IllegalStateException.class)
                            .hasRootCauseMessage(
                                    "Disconnected replication connection has not drained on the destination");
                    assertThat(connection.completion()).as("retired socket must wait for persistence").isNotDone();
                } finally {
                    for (BookieServer bookie : bkEnsemble2.getBookies()) {
                        bookie.resumeProcessing();
                    }
                }
            } else {
                if (publishDuringObservation) {
                    try (Consumer<String> consumer = client2.newConsumer(Schema.STRING).topic(topicName)
                            .subscriptionName("s1").subscribe()) {
                        for (String value : expected) {
                            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                            assertThat(message).isNotNull();
                            assertThat(message.getValue()).isEqualTo(value);
                        }
                        // A duplicate generated after catch-up must still run while replication is active.
                        ScheduledFuture<?> lateSend = pulsar1.getExecutor().schedule(
                                () -> connection.releaseDelayedDuplicate(), 5, TimeUnit.SECONDS);
                        try {
                            assertThatThrownBy(() -> assertNoLateMessages(consumer))
                                    .isInstanceOf(AssertionError.class)
                                    .hasMessageContaining("no late duplicate while replication remains active");
                            lateSend.get(10, TimeUnit.SECONDS);
                            assertThat(source.getReplicators()).containsKey(cluster2);
                            assertThat(connection.isDisconnected()).isFalse();
                        } finally {
                            lateSend.cancel(false);
                        }
                    }
                } else {
                    connection.releaseDelayedDuplicate().get(10, TimeUnit.SECONDS);
                }
                admin1.topics().setReplicationClusters(topicName, List.of(cluster1));
                closeResponseReceived.get(30, TimeUnit.SECONDS);
                assertThat(connection.completion()).as("close response must pass the barrier").isNotDone();
                releaseCloseResponse.complete(null);
            }
            stopAndDrainReplication(topicName, source, target, List.of(connection));

            if (enabledDeduplication) {
                assertReplicatedEntryCount(target, expected.size());
                try (Consumer<String> consumer = client2.newConsumer(Schema.STRING).topic(topicName)
                        .subscriptionName("s1").subscribe()) {
                    List<String> received = new ArrayList<>();
                    for (int i = 0; i < expected.size(); i++) {
                        Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                        assertThat(message).as("replicated message %s", i).isNotNull();
                        received.add(message.getValue());
                    }
                    assertThat(received).containsExactlyElementsOf(expected);
                }
            } else {
                assertThatThrownBy(() -> assertReplicatedEntryCount(target, expected.size()))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("persisted replicated entries");
                assertThat(target.getManagedLedger().getNumberOfEntries()).isEqualTo(expected.size() + 1L);
            }
        } catch (Exception | AssertionError error) {
            testFailure = error;
            throw error;
        } finally {
            releaseCloseResponse.complete(null);
            List<AutoCloseable> cleanupTasks = new ArrayList<>();
            cleanupTasks.add(() -> {
                if (publishingConnection.isDone() && !publishingConnection.isCompletedExceptionally()) {
                    publishingConnection.getNow(null).discardDelayedDuplicate().get(10, TimeUnit.SECONDS);
                }
            });
            cleanupTasks.add(() -> admin1.topics().setReplicationClusters(topicName, List.of(cluster1)));
            cleanupTasks.add(() -> waitReplicatorStopped(pulsar1, pulsar2, topicName));
            cleanupTasks.add(() -> admin1.topics().delete(topicName, true));
            cleanupTasks.add(() -> admin2.topics().delete(topicName, true));
            cleanupTasks.add(clearInjection::run);
            closeAfterTest(cleanupTasks, testFailure);
        }
    }

    /***
     * To reproduce the issue that replication loss message if enabled deduplication
     * 1. Publishing in the source cluster
     *   1-1. Producer-1 send 2 messages: M1, M2
     *   1-2. Producer-2 send 2 messages: M3, M4
     * 2. Replicate messages to the remote cluster
     *   2-1. Copies M1 and M2
     *   2-2. Repeatedly copies M1 and M2. and copies M3 and M4.
     *     2-2-1. After repeatedly copies M1 and M2, the network broke.
     * 3. After a topic unloading.
     *   3-1. The replicator will start after the topic is loaded up.
     *   3-2. The client will create a new connection.
     * 4. Verify: All 4 messages are copied to the remote cluster.
     */
    @Test(timeOut = 360 * 1000, dataProvider = "enabledDeduplication")
    @SuppressWarnings("unchecked")
    public void testDeduplicationNotLostMessage(boolean enabledDeduplication) throws Exception {
        waitInternalClientCreated();

        /**
         * step-2: Inject a mechanism that makes the client connect broke after repeatedly copied M1 and M2.
         */
        final List<ByteBufPair> duplicatedMsgs = new ArrayList<>();
        final int repeatMsgIndex = 2;
        AtomicInteger msgSent = new AtomicInteger(0);
        ConcurrentHashSet<Channel> injectedChannel = new ConcurrentHashSet<>();
        Runnable taskToClearInjection = injectReplicatorClientCnx(
                (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {

                    @Override
                    public ChannelHandlerContext ctx() {
                        final ChannelHandlerContext originalCtx = super.ctx;
                        ChannelHandlerContext spyContext = spy(originalCtx);
                        Answer injectedAnswer = invocation -> {
                            // Do not repeat the messages re-sending, and clear the previous cached messages when
                            // calling re-sending, to avoid publishing outs of order.
                            for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
                                if (stackTraceElement.toString().contains("recoverProcessOpSendMsgFrom")
                                        || stackTraceElement.toString().contains("resendMessages")) {
                                    duplicatedMsgs.clear();
                                    return invocation.callRealMethod();
                                }
                            }

                            Object data = invocation.getArguments()[0];
                            if (!(data instanceof ByteBufPair)) {
                                return invocation.callRealMethod();
                            }
                            // Repeatedly send every message.
                            ByteBufPair byteBufPair = (ByteBufPair) data;
                            ByteBuf buf1 = byteBufPair.getFirst();
                            ByteBuf buf2 = byteBufPair.getSecond();
                            int bufferIndex1 = buf1.readerIndex();
                            int bufferIndex2 = buf2.readerIndex();
                            // Skip totalSize.
                            buf1.readInt();
                            int cmdSize = buf1.readInt();
                            BaseCommand cmd = new BaseCommand();
                            cmd.parseFrom(buf1, cmdSize);
                            buf1.readerIndex(bufferIndex1);
                            if (cmd.getType().equals(BaseCommand.Type.SEND)) {
                                synchronized (duplicatedMsgs) {
                                    if (duplicatedMsgs.isEmpty() && msgSent.get() == repeatMsgIndex) {
                                        return null;
                                    }
                                    if (msgSent.get() == repeatMsgIndex) {
                                        for (ByteBufPair bufferPair : duplicatedMsgs) {
                                            originalCtx.channel().write(bufferPair, originalCtx.voidPromise());
                                            originalCtx.channel().flush();
                                        }
                                        duplicatedMsgs.clear();
                                        return null;
                                    }
                                }
                                ByteBuf newBuffer1 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(
                                        buf1.readableBytes());
                                buf1.readBytes(newBuffer1);
                                buf1.readerIndex(bufferIndex1);
                                ByteBuf newBuffer2 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(
                                        buf2.readableBytes());
                                buf2.readBytes(newBuffer2);
                                buf2.readerIndex(bufferIndex2);
                                synchronized (duplicatedMsgs) {
                                    if (newBuffer2.readableBytes() > 0 && msgSent.incrementAndGet() <= repeatMsgIndex) {
                                        duplicatedMsgs.add(ByteBufPair.get(newBuffer1, newBuffer2));
                                    }
                                }
                                return invocation.callRealMethod();
                            } else {
                                return invocation.callRealMethod();
                            }
                        };
                        doAnswer(injectedAnswer).when(spyContext).write(any());
                        doAnswer(injectedAnswer).when(spyContext).write(any(), any(ChannelPromise.class));
                        doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any());
                        doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any(), any(ChannelPromise.class));
                        injectedChannel.add(originalCtx.channel());
                        return spyContext;
                    }
                });

        // Create topics and enable deduplication.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        PersistentTopic tp1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic tp2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml2 = (ManagedLedgerImpl) tp2.getManagedLedger();
        if (enabledDeduplication) {
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                PersistentTopic persistentTopic1 =
                        (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
                PersistentTopic persistentTopic2 =
                        (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
                admin1.topicPolicies().setDeduplicationStatus(topicName, true);
                admin2.topicPolicies().setDeduplicationStatus(topicName, true);
                assertEquals(persistentTopic1.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
                assertEquals(persistentTopic2.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
            });
        }
        // Let broker persist messages one by one, in other words, it starts to persist the next message after the
        // previous has been written into BKs.
        PersistentTopic spyTp2 = spy(tp2);
        doAnswer(invocation -> {
            try {
                Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                    assertEquals(ml2.getPendingAddEntriesCount(), 0);
                });
            } catch (Throwable throwable) {
                // Ignore this timeout error.
            }
            return invocation.callRealMethod();
        }).when(spyTp2).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));
        CompletableFuture<Optional<Topic>> originalTp2 = pulsar2.getBrokerService().getTopics().put(tp2.getName(),
                CompletableFuture.completedFuture(Optional.of(spyTp2)));

        /**
         * Step-1: Publishes messages in the source cluster and start replication,
         */
        ProducerImpl p1 = (ProducerImpl) client1.newProducer().topic(topicName).producerName("p1").create();
        ProducerImpl p2 = (ProducerImpl) client1.newProducer().topic(topicName).producerName("p2").create();
        p1.send("1".toString().getBytes(StandardCharsets.UTF_8));
        p1.send("2".toString().getBytes(StandardCharsets.UTF_8));
        p2.send("3".toString().getBytes(StandardCharsets.UTF_8));
        p2.send("4".toString().getBytes(StandardCharsets.UTF_8));

        // Enable replication and wait the task to be finished, it should not finish if no bug.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        waitReplicatorStarted(topicName);
        try {
            Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                    if (cursor.getName().equals("pulsar.repl.r2")) {
                        long replBacklog = cursor.getNumberOfEntriesInBacklog(true);
                        log.info().attr("replBacklog", replBacklog).log("repl backlog");
                        assertEquals(replBacklog, 0);
                    }
                }
            });
        } catch (Throwable t) {
            // Ignore the error.
        }

        /**
         * Step-3: remove the injections, unload topics and rebuild connections of the replicator.
         */
        taskToClearInjection.run();
        pulsar2.getBrokerService().getTopics().put(tp2.getName(), originalTp2);
        admin1.topics().unload(topicName);
        admin2.topics().unload(topicName);
        for (Channel channel : injectedChannel) {
            channel.close();
        }
        waitReplicatorStarted(topicName);
        PersistentTopic tp12 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic tp22 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            for (ManagedCursor cursor : tp12.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    long replBacklog = cursor.getNumberOfEntriesInBacklog(true);
                    log.info().attr("replBacklog", replBacklog).log("repl backlog");
                    assertEquals(replBacklog, 0);
                }
            }
        });

        /**
         * Verify: All 4 messages are copied to the remote cluster.
         */
        List<String> msgReceived = new ArrayList<>();
        Consumer consumer = client2.newConsumer().topic(topicName)
                .subscriptionName("s1").subscribe();
        while (true) {
            Message msg = consumer.receive(10, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            MessageIdAdv messageIdAdv = (MessageIdAdv) msg.getMessageId();
            log.info().attr("msgSource", StringUtils.join(msg.getProperties().values(), ":"))
                    .attr("target", messageIdAdv.getLedgerId()).attr("entryId", messageIdAdv.getEntryId())
                    .log("received msg. source, target");
            msgReceived.add(new String(msg.getData(), StandardCharsets.UTF_8));
            consumer.acknowledgeAsync(msg);
        }

        log.info().attr("receivedMsgs", msgReceived).log("received msgs");
        assertTrue(msgReceived.contains("1"));
        assertTrue(msgReceived.contains("2"));
        assertTrue(msgReceived.contains("3"));
        assertTrue(msgReceived.contains("4"));
        if (enabledDeduplication) {
            assertEquals(msgReceived, Arrays.asList("1", "2", "3", "4"));
        }

        // cleanup.
        consumer.close();
        p1.close();
        p2.close();
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(topicName);
        Awaitility.await().until(() -> {
            for (ManagedCursor cursor : tp12.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    return false;
                }
            }
            return true;
        });
        admin1.topics().delete(topicName);
        admin2.topics().delete(topicName);
    }

    @Test(timeOut = 360 * 1000, dataProvider = "enabledDeduplication")
    public void testReplicationLoadSchemaTimeout(boolean enabledDeduplication) throws Exception {
        waitInternalClientCreated();
        AtomicInteger getSchemaResponses = new AtomicInteger();
        AtomicInteger getOrCreateSchemaResponses = new AtomicInteger();
        AtomicInteger schemaTimeouts = new AtomicInteger();
        AtomicReference<Throwable> asynchronousFailure = new AtomicReference<>();
        CompletableFuture<Void> timeoutObserved = new CompletableFuture<>();
        CompletableFuture<Void> retryObserved = new CompletableFuture<>();
        CompletableFuture<Runnable> releaseLateResponse = new CompletableFuture<>();
        CompletableFuture<Void> lateResponseIgnored = new CompletableFuture<>();
        Runnable taskToClearInjection = injectReplicatorClientCnx(
                (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {
                    @Override
                    protected void handleGetSchemaResponse(CommandGetSchemaResponse response) {
                        getSchemaResponses.incrementAndGet();
                        super.handleGetSchemaResponse(response);
                    }

                    @Override
                    protected void handleGetOrCreateSchemaResponse(CommandGetOrCreateSchemaResponse response) {
                        try {
                            checkArgument(state == State.Ready);
                            int responseCount = getOrCreateSchemaResponses.incrementAndGet();
                            if (responseCount == 1) {
                                CompletableFuture<?> request = pendingRequests.remove(response.getRequestId());
                                assertThat(request).as("schema request to time out").isNotNull();
                                CommandGetOrCreateSchemaResponse lateResponse =
                                        new CommandGetOrCreateSchemaResponse().copyFrom(response);
                                releaseLateResponse.complete(() -> ctx().executor().execute(() -> {
                                    try {
                                        long duplicates = getDuplicatedResponseCount();
                                        super.handleGetOrCreateSchemaResponse(lateResponse);
                                        assertThat(getDuplicatedResponseCount())
                                                .as("late schema response must be ignored after its request timed out")
                                                .isEqualTo(duplicates + 1);
                                        lateResponseIgnored.complete(null);
                                    } catch (Throwable error) {
                                        asynchronousFailure.compareAndSet(null, error);
                                        lateResponseIgnored.completeExceptionally(error);
                                    }
                                }));
                                request.whenComplete((__, error) -> {
                                    if (error instanceof PulsarClientException.TimeoutException) {
                                        schemaTimeouts.incrementAndGet();
                                        timeoutObserved.complete(null);
                                    } else {
                                        timeoutObserved.completeExceptionally(new AssertionError(
                                                "Expected the schema request to fail with TimeoutException", error));
                                    }
                                });
                                assertThat(request.completeExceptionally(
                                        new PulsarClientException.TimeoutException("Mocked timeout")))
                                        .as("the injected timeout must complete a pending schema request").isTrue();
                                return;
                            }
                            assertThat(response.hasErrorCode()).as("retried schema response").isFalse();
                            CompletableFuture<?> request = pendingRequests.get(response.getRequestId());
                            assertThat(request).as("retried schema request").isNotNull();
                            super.handleGetOrCreateSchemaResponse(response);
                            assertThat(request).as("retried schema request must succeed").isCompleted();
                            assertThat(request.isCompletedExceptionally()).as("retried schema request failure")
                                    .isFalse();
                            retryObserved.complete(null);
                        } catch (Throwable error) {
                            asynchronousFailure.compareAndSet(null, error);
                            timeoutObserved.completeExceptionally(error);
                            retryObserved.completeExceptionally(error);
                            lateResponseIgnored.completeExceptionally(error);
                        }
                    }
                });
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        Throwable testFailure = null;
        try {
            admin1.topics().createNonPartitionedTopic(topicName);
            admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
            admin2.topics().createNonPartitionedTopic(topicName);
            admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
            PersistentTopic source = (PersistentTopic) pulsar1.getBrokerService()
                    .getTopic(topicName, false).get().orElseThrow();
            PersistentTopic target = (PersistentTopic) pulsar2.getBrokerService()
                    .getTopic(topicName, false).get().orElseThrow();
            if (enabledDeduplication) {
                admin1.topicPolicies().setDeduplicationStatus(topicName, true);
                admin2.topicPolicies().setDeduplicationStatus(topicName, true);
                Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    assertEquals(source.getHierarchyTopicPolicies().getDeduplicationEnabled().get(), Boolean.TRUE);
                    assertEquals(target.getHierarchyTopicPolicies().getDeduplicationEnabled().get(), Boolean.TRUE);
                });
            }
            admin1.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            admin2.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertEquals(source.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
                assertEquals(target.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            });

            try (Producer<byte[]> p1 = client1.newProducer().topic(topicName).producerName("p1").create();
                    Producer<byte[]> p2 = client1.newProducer().topic(topicName).producerName("p2").create();
                    Producer<String> p3 = client1.newProducer(Schema.STRING).topic(topicName)
                            .producerName("p3").create()) {
                p1.send("1".getBytes(StandardCharsets.UTF_8));
                p1.send("2".getBytes(StandardCharsets.UTF_8));
                p3.send("2-1");
                p3.send("2-2");
                p2.send("3".getBytes(StandardCharsets.UTF_8));
                p2.send("4".getBytes(StandardCharsets.UTF_8));

                admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
                waitReplicatorStarted(topicName);
                timeoutObserved.get(30, TimeUnit.SECONDS);
                retryObserved.get(30, TimeUnit.SECONDS);
                // Deliver the original real broker response after the retry has succeeded.
                releaseLateResponse.get(30, TimeUnit.SECONDS).run();
                lateResponseIgnored.get(30, TimeUnit.SECONDS);
                assertThat(schemaTimeouts.get()).as("injected schema timeouts actually observed").isEqualTo(1);
                assertThat(getOrCreateSchemaResponses.get()).as("schema registration and retry responses")
                        .isGreaterThanOrEqualTo(2);
                Awaitility.await().atMost(Duration.ofSeconds(180)).pollInterval(Duration.ofSeconds(1))
                        .untilAsserted(() -> assertThat(requireCursor(source, "pulsar.repl.r2")
                                .getNumberOfEntriesInBacklog(true)).as("replication backlog after schema recovery")
                                .isZero());

                List<String> received = new ArrayList<>();
                try (Consumer<byte[]> consumer = client2.newConsumer().topic(topicName)
                        .subscriptionName("s1").subscribe()) {
                    while (true) {
                        Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
                        if (message == null) {
                            break;
                        }
                        received.add(new String(message.getData(), StandardCharsets.UTF_8));
                        consumer.acknowledgeAsync(message);
                    }
                }
                List<String> expected = Arrays.asList("1", "2", "2-1", "2-2", "3", "4");
                assertThat(received).containsAll(expected);
                if (enabledDeduplication) {
                    assertThat(received).containsExactlyElementsOf(expected);
                }
                assertThat(asynchronousFailure.get()).as("schema callbacks throughout the observation window")
                        .isNull();
                // Source-schema loading uses the source broker's client, outside this target-client injection.
                assertThat(getSchemaResponses.get()).as("GET_SCHEMA is not exercised by the target client").isZero();
            }
        } catch (Exception | AssertionError error) {
            testFailure = error;
            throw error;
        } finally {
            closeAfterTest(List.of(
                    () -> admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1)),
                    () -> waitReplicatorStopped(topicName),
                    () -> admin1.topics().delete(topicName),
                    () -> admin2.topics().delete(topicName),
                    taskToClearInjection::run), testFailure);
        }
    }
}
