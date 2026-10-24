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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.GeoPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.ReplicatedSubscriptionsController;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
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
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.awaitility.Awaitility;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public class OneWayReplicatorDeduplicationTest extends OneWayReplicatorDeduplicationTestBase {

    @Override
    protected boolean accepts(boolean supportsReplDedupByLidAndEid, boolean multiSchemas) {
        return supportsReplDedupByLidAndEid && !multiSchemas;
    }

    @Test
    public void testRepeatedlyPublishMixedMessageAndReplMarkers() throws Exception {
        // Inject a mechanism to drop all send receipt, to implement a scenario: the messages of the internal producer
        // of the replicator will be resent after a disconnection.
        AtomicBoolean stuckSendReceipt = new AtomicBoolean(true);
        Runnable cleanInjection = injectReplicatorClientCnx((conf, eventLoopGroup) -> new ClientCnx(
                InstrumentProvider.NOOP, conf, eventLoopGroup) {
            @Override
            protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
                if (stuckSendReceipt.get()) {
                    // discard all send receipt, to make the producer republish.
                } else {
                    super.handleSendReceipt(sendReceipt);
                }
            }
        });

        // Create topics.
        // - Enable deduplication on the remote cluster.
        // - Enable replicated subscription.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        final String subscription = "s1";
        admin1.topics().createNonPartitionedTopic(topicName);
        Consumer<Integer> consumer1 = client1.newConsumer(Schema.INT32).topic(topicName).subscriptionName(subscription)
                .replicateSubscriptionState(true).subscribe();
        admin2.topics().createSubscription(topicName, subscription, MessageId.earliest);
        consumer1.close();
        waitReplicatorStarted(topicName);
        PersistentTopic persistentTopic1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic persistentTopic2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        admin2.topicPolicies().setDeduplicationStatus(topicName, true);
        Awaitility.await().untilAsserted(() -> {
            MessageDeduplication messageDeduplication2 = persistentTopic2.getMessageDeduplication();
            assertEquals(String.valueOf(messageDeduplication2.getStatus()), "Enabled");
        });
        assertTrue(persistentTopic1.getSubscriptions().get(subscription).isReplicated());
        assertTrue(persistentTopic1.getReplicatedSubscriptionController().isPresent());
        ReplicatedSubscriptionsController replicatedSubscriptionsController =
                persistentTopic1.getReplicatedSubscriptionController().get();
        admin2.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));

        // Publish mixed user messages and replicated markers/
        // - Publish a user msg.
        // - Replicated marker will be sent internally per seconds
        Producer<Integer> producer = client1.newProducer(Schema.INT32).topic(topicName).create();
        int messageCount = 15;
        for (int i = 0; i < messageCount; i++) {
            producer.send(i);
            log.info().attr("sentMessage", i).log("Sent message");
            // The replicated marker will be sent internally per seconds. We wait 1.2s here, a replicated marker will
            // be sent.
            Thread.sleep(1200);
            log.info().attr("replicationSnapshot", replicatedSubscriptionsController.getLastCompletedSnapshotId())
                    .log("latest replication snapshot");
        }
        producer.close();

        //
        GeoPersistentReplicator replicator = (GeoPersistentReplicator) persistentTopic1.getReplicators().get(cluster2);
        long backlog = replicator.getCursor().getNumberOfEntriesInBacklog(true);
        Awaitility.await().untilAsserted(() -> {
            log.info().attr("replicationBacklog", backlog).log("replication backlog");
            assertTrue(backlog >= messageCount * 2);
        });

        // 1. Remove the injection, which discarded send receipt of the internal producer of the replicator.
        // 2. Trigger a reconnection of the internal producer of the replicator.
        // The internal producer will retry to publish.
        stuckSendReceipt.set(false);
        replicator.producer.getClientCnx().ctx().channel().close();
        Producer<Integer> producer2 = client1.newProducer(Schema.INT32).topic(topicName).create();
        producer2.close();
        waitReplicatorStarted(topicName);

        // Verify: all messages are sent, and no duplicated messages.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertEquals(replicator.getCursor().getNumberOfEntriesInBacklog(true), 0);
        });
        Consumer<Integer> consumer2 = client2.newConsumer(Schema.INT32).topic(topicName).subscriptionName(subscription)
                .replicateSubscriptionState(true).subscribe();
        for (int i = 0; i < messageCount; i++) {
            Message<Integer> msg = consumer2.receive(2, TimeUnit.SECONDS);
            assertNotNull(msg);
            log.info().attr("receivedMessage", msg.getValue()).log("Received message");
            assertEquals(msg.getValue(), i);
        }

        // cleanup.
        cleanInjection.run();
        admin1.topics().deleteSubscription(topicName, subscription);
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        admin2.topics().setReplicationClusters(topicName, Arrays.asList(cluster2));
        admin1.topics().unload(topicName);
        admin2.topics().unload(topicName);
    }

    @DataProvider(name = "enabledDeduplication")
    public Object[][] enabledDeduplication() {
        return new Object[][] {
                {true},
                {false}
        };
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
    @SuppressWarnings("unchecked")
    public void testReplicationLoadSchemaTimeout(boolean enabledDeduplication) throws Exception {
        waitInternalClientCreated();

        /**
         * Inject a timeout error for Get Schema.
         */
        Field filedSchemaRegistryService = PulsarService.class.getDeclaredField("schemaRegistryService");
        filedSchemaRegistryService.setAccessible(true);
        SchemaRegistryService originalSchemaRegistryService =
                (SchemaRegistryService) filedSchemaRegistryService.get(pulsar2);
        SchemaRegistryService spySchemaRegistryService = spy(originalSchemaRegistryService);
        AtomicBoolean getSchemaSuccess = new AtomicBoolean(false);
        doAnswer(invocation -> {
            if (getSchemaSuccess.get()) {
                getSchemaSuccess.set(false);
                return invocation.callRealMethod();
            } else {
                getSchemaSuccess.set(true);
            }
            Thread.sleep(60 * 1000);
            return invocation.callRealMethod();
        }).when(spySchemaRegistryService).findSchemaVersion(any(String.class), any(SchemaData.class));
        filedSchemaRegistryService.set(pulsar2, spySchemaRegistryService);
        Runnable taskToClearInjection = injectReplicatorClientCnx(
            (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {
                @Override
                @SuppressWarnings("unchecked")
                protected void handleGetSchemaResponse(CommandGetSchemaResponse commandGetSchemaResponse) {
                    if (getSchemaSuccess.get()) {
                        getSchemaSuccess.set(false);
                        super.handleGetSchemaResponse(commandGetSchemaResponse);
                        return;
                    } else {
                        getSchemaSuccess.set(true);
                    }
                    checkArgument(state == State.Ready);
                    long requestId = commandGetSchemaResponse.getRequestId();
                    CompletableFuture<CommandGetSchemaResponse> future =
                            (CompletableFuture<CommandGetSchemaResponse>) pendingRequests.remove(requestId);
                    if (future == null) {
                        duplicatedResponseCounter.incrementAndGet();
                        log.warn().attr("channel", ctx.channel()).attr("fromServer", requestId)
                                .log("Received unknown request id from server");
                        return;
                    }
                    future.completeExceptionally(new PulsarClientException.TimeoutException("Mocked timeout"));
                }

                @Override
                @SuppressWarnings("unchecked")
                protected void handleGetOrCreateSchemaResponse(CommandGetOrCreateSchemaResponse
                                                                       commandGetOrCreateSchemaResponse) {

                    if (getSchemaSuccess.get()) {
                        getSchemaSuccess.set(false);
                        super.handleGetOrCreateSchemaResponse(commandGetOrCreateSchemaResponse);
                        return;
                    } else {
                        getSchemaSuccess.set(true);
                    }

                    checkArgument(state == State.Ready);
                    long requestId = commandGetOrCreateSchemaResponse.getRequestId();
                    CompletableFuture<CommandGetOrCreateSchemaResponse> future =
                            (CompletableFuture<CommandGetOrCreateSchemaResponse>) pendingRequests.remove(requestId);
                    if (future == null) {
                        duplicatedResponseCounter.incrementAndGet();
                        log.warn().attr("channel", ctx.channel()).attr("fromServer", requestId)
                                .log("Received unknown request id from server");
                        return;
                    }
                    future.completeExceptionally(new PulsarClientException.TimeoutException("Mocked timeout"));
                }
            });

        // Create topics and enable deduplication.
        @SuppressWarnings("unchecked")
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        PersistentTopic tp1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic tp2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
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
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            PersistentTopic persistentTopic1 =
                    (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
            PersistentTopic persistentTopic2 =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            admin1.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            admin2.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            assertEquals(persistentTopic1.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            assertEquals(persistentTopic2.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        });

        // Publishes messages in the source cluster.
        Producer p1 = client1.newProducer().topic(topicName).producerName("p1").create();
        Producer p2 = client1.newProducer().topic(topicName).producerName("p2").create();
        Producer p3 = client1.newProducer(Schema.STRING).topic(topicName).producerName("p3").create();
        p1.send("1".toString().getBytes(StandardCharsets.UTF_8));
        p1.send("2".toString().getBytes(StandardCharsets.UTF_8));
        p3.send("2-1");
        p3.send("2-2");
        p2.send("3".toString().getBytes(StandardCharsets.UTF_8));
        p2.send("4".toString().getBytes(StandardCharsets.UTF_8));

        // Enable replication and wait the task to be finished, it should not finish if no bug.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        waitReplicatorStarted(topicName);
        Awaitility.await().atMost(Duration.ofSeconds(180)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    long replBacklog = cursor.getNumberOfEntriesInBacklog(true);
                    log.info().attr("replBacklog", replBacklog).log("repl backlog");
                    assertEquals(replBacklog, 0);
                }
            }
        });

        // Verify: All messages are copied to the remote cluster.
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
        assertTrue(msgReceived.contains("2-1"));
        assertTrue(msgReceived.contains("2-2"));
        assertTrue(msgReceived.contains("3"));
        assertTrue(msgReceived.contains("4"));
        if (enabledDeduplication) {
            assertEquals(msgReceived, Arrays.asList("1", "2", "2-1", "2-2", "3", "4"));
        }

        // cleanup.
        taskToClearInjection.run();
        filedSchemaRegistryService.set(pulsar2, originalSchemaRegistryService);
        consumer.close();
        p1.close();
        p2.close();
        p3.close();
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(topicName);
        Awaitility.await().until(() -> {
            for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    return false;
                }
            }
            return true;
        });
        admin1.topics().delete(topicName);
        admin2.topics().delete(topicName);
    }
}
