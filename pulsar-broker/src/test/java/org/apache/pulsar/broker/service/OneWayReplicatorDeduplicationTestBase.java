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

import static org.apache.pulsar.client.impl.GeoReplicationProducerImpl.MSG_PROP_REPL_SOURCE_POSITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.persistent.GeoPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public abstract class OneWayReplicatorDeduplicationTestBase extends OneWayReplicatorTestBase {

    protected static final ObjectMapper JACKSON = new ObjectMapper();

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
        waitInternalClientCreated();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        // For check whether deduplication snapshot has done.
        config.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        // An entries-interval snapshot is skipped while the previous one is still in progress, so the last entries
        // are only snapshotted by the periodic check. Run it every second instead of the default 120 seconds.
        config.setBrokerDeduplicationSnapshotFrequencyInSeconds(1);
        config.setBrokerDeduplicationEntriesInterval(10);
        config.setReplicationStartAt("earliest");
        // To cover more cases, write more than one ledger.
        config.setManagedLedgerMaxEntriesPerLedger(100);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        config.setManagedLedgerMaxLedgerRolloverTimeMinutes(1);
    }

    protected void waitReplicatorStopped(String topicName) {
        Awaitility.await().untilAsserted(() -> {
            Optional<Topic> topicOptional2 = pulsar2.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional2.isPresent());
            PersistentTopic persistentTopic2 = (PersistentTopic) topicOptional2.get();
            assertTrue(persistentTopic2.getProducers().isEmpty());
            Optional<Topic> topicOptional1 = pulsar1.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional1.isPresent());
            PersistentTopic persistentTopic1 = (PersistentTopic) topicOptional1.get();
            assertTrue(persistentTopic1.getReplicators().isEmpty()
                    || !persistentTopic1.getReplicators().get(cluster2).isConnected());
        });
    }

    protected void waitInternalClientCreated() throws Exception {
        // Wait for the internal client created.
        final String topicNameTriggerInternalClientCreate =
                BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicNameTriggerInternalClientCreate);
        waitReplicatorStarted(topicNameTriggerInternalClientCreate);
        cleanupTopics(() -> {
            admin1.topics().delete(topicNameTriggerInternalClientCreate);
            admin2.topics().delete(topicNameTriggerInternalClientCreate);
        });
    }

    protected Runnable injectReplicatorClientCnx(
            InjectedClientCnxClientBuilder.ClientCnxFactory clientCnxFactory) throws Exception {
        String cluster2 = pulsar2.getConfig().getClusterName();
        BrokerService brokerService = pulsar1.getBrokerService();
        ClientBuilderImpl clientBuilder2 = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(url2.toString());

        // Inject spy client.
        final var replicationClients = brokerService.getReplicationClients();
        PulsarClientImpl internalClient = (PulsarClientImpl) replicationClients.get(cluster2);
        PulsarClientImpl injectedClient = InjectedClientCnxClientBuilder.create(clientBuilder2, clientCnxFactory);
        if (internalClient != null) {
            assertTrue(replicationClients.remove(cluster2, internalClient));
        }
        assertNull(replicationClients.putIfAbsent(cluster2, injectedClient));

        // Return a cleanup injection task;
        return () -> {
            assertTrue(replicationClients.remove(cluster2, injectedClient));
            if (internalClient != null) {
                assertNull(replicationClients.putIfAbsent(cluster2, internalClient));
            }
            injectedClient.closeAsync().orTimeout(30, TimeUnit.SECONDS).join();
        };
    }

    protected static Object[][] allDeduplicationArgs() {
        return new Object[][] {
            {true/* inject repeated publishing*/, 1/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 2/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 3/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 4/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 5/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 10/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            // ===== multi schema
            {true/* inject repeated publishing*/, 1/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 2/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 3/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 4/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 5/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 10/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            // ===== Compatability "source-cluster: old, target-cluster: new".
            {false/* inject repeated publishing*/, 0/* repeated messages window */,
                    false /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {false/* inject repeated publishing*/, 0/* repeated messages window */,
                    false /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 3/* repeated messages window */,
                    false /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
        };
    }

    @DataProvider(name = "deduplicationArgs")
    public Object[][] deduplicationArgs() {
        return Arrays.stream(allDeduplicationArgs())
                .filter(args -> acceptsDeduplicationCase((boolean) args[2], (boolean) args[3]))
                .toArray(Object[][]::new);
    }

    protected abstract boolean acceptsDeduplicationCase(boolean supportsReplDedupByLidAndEid, boolean multiSchemas);

    // TODO
    //  - Review the code to confirm that multi source-brokers can work when the source topic switch.
    @Test(timeOut = 360 * 1000, dataProvider = "deduplicationArgs")
    public void testDeduplication(final boolean injectRepeatedPublish, final int repeatedMessagesWindow,
                                  final boolean supportsReplDedupByLidAndEid, boolean multiSchemas) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        ReplicationPublishTracker publishTracker = new ReplicationPublishTracker(topicName);
        BrokerInterceptor originalInterceptor = broker2.getInterceptor();
        // 0. Inject a mechanism that duplicate all Send-Command for the replicator.
        Map<ReplicationDrainClientCnx, List<ByteBufPair>> duplicatedMsgs = new ConcurrentHashMap<>();
        List<ReplicationDrainClientCnx> connections = new CopyOnWriteArrayList<>();
        AtomicReference<PersistentTopic> destination = new AtomicReference<>();
        AtomicBoolean injectDuplicates = new AtomicBoolean(injectRepeatedPublish);
        FirstReplicationSend firstReplicationSend = new FirstReplicationSend();
        AtomicReference<Throwable> duplicateInjectionFailure = new AtomicReference<>();
        Runnable taskToClearInjection = injectReplicatorClientCnx(
            (conf, eventLoopGroup) -> {
                ReplicationDrainClientCnx connection = new ReplicationDrainClientCnx(
                        conf, eventLoopGroup, destination::get, publishTracker) {
                    private final ThreadLocal<Boolean> writing = ThreadLocal.withInitial(() -> false);

                    @Override
                    public void channelInactive(ChannelHandlerContext context) throws Exception {
                        try {
                            super.channelInactive(context);
                        } finally {
                            // These cached copies were never sent; retain only evidence for actual wire requests.
                            List<ByteBufPair> cached = duplicatedMsgs.get(this);
                            if (cached != null) {
                                synchronized (cached) {
                                    cached.forEach(ByteBufPair::release);
                                    cached.clear();
                                }
                            }
                        }
                    }

                    @Override
                    protected ByteBuf buildConnectCommand(AuthData authData) throws Exception {
                        if (supportsReplDedupByLidAndEid) {
                            return super.buildConnectCommand(authData);
                        }
                        BaseCommand cmd = Commands.newConnectWithoutSerialize(
                                authMethodName(), authData, this.protocolVersion, clientVersion,
                                proxyToTargetBrokerAddress,
                                null, null, null, null, null);
                        cmd.getConnect().getFeatureFlags().setSupportsReplDedupByLidAndEid(false);
                        return Commands.serializeWithSize(cmd);
                    }

                    @Override
                    public boolean isBrokerSupportsReplDedupByLidAndEid() {
                        return supportsReplDedupByLidAndEid;
                    }

                    @Override
                    public ChannelHandlerContext ctx() {
                        if (!injectDuplicates.get()) {
                            return super.ctx();
                        }
                        List<ByteBufPair> connectionDuplicates =
                                duplicatedMsgs.computeIfAbsent(this, ignored -> new ArrayList<>());
                        final ChannelHandlerContext originalCtx = super.ctx;
                        ChannelHandlerContext spyContext = spy(originalCtx);
                        Answer injectedAnswer = invocation -> {
                            // A context write overload may delegate to another intercepted overload.
                            if (writing.get()) {
                                return invocation.callRealMethod();
                            }
                            writing.set(true);
                            try {
                                Object data = invocation.getArguments()[0];
                                if (!(data instanceof ByteBufPair byteBufPair)) {
                                    return invocation.callRealMethod();
                                }
                                ByteBuf header = byteBufPair.getFirst().duplicate();
                                header.readInt(); // Total frame size.
                                int commandSize = header.readInt();
                                BaseCommand command = new BaseCommand();
                                command.parseFrom(header, commandSize);
                                if (command.getType() != BaseCommand.Type.SEND) {
                                    return invocation.callRealMethod();
                                }
                                // Schema buffering can put the first real send on a resend call stack. Classify by
                                // source entry across connections instead of excluding those call stacks wholesale.
                                if (!firstReplicationSend.isFirstSend(byteBufPair)) {
                                    synchronized (connectionDuplicates) {
                                        connectionDuplicates.forEach(ByteBufPair::release);
                                        connectionDuplicates.clear();
                                    }
                                    return invocation.callRealMethod();
                                }
                                synchronized (connectionDuplicates) {
                                    if (connectionDuplicates.size() >= repeatedMessagesWindow) {
                                        for (ByteBufPair bufferPair : connectionDuplicates) {
                                            publishTracker.recordInjectedDuplicate(bufferPair,
                                                    originalCtx.channel().localAddress());
                                            originalCtx.channel().write(bufferPair, originalCtx.voidPromise());
                                            originalCtx.channel().flush();
                                        }
                                        connectionDuplicates.clear();
                                    }
                                }
                                ByteBuf buf1 = byteBufPair.getFirst();
                                ByteBuf buf2 = byteBufPair.getSecond();
                                ByteBuf newBuffer1 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(buf1.readableBytes());
                                newBuffer1.writeBytes(buf1, buf1.readerIndex(), buf1.readableBytes());
                                ByteBuf newBuffer2 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(buf2.readableBytes());
                                newBuffer2.writeBytes(buf2, buf2.readerIndex(), buf2.readableBytes());
                                synchronized (connectionDuplicates) {
                                    if (injectDuplicates.get() && originalCtx.channel().isActive()
                                            && newBuffer2.readableBytes() > 0) {
                                        connectionDuplicates.add(ByteBufPair.get(newBuffer1, newBuffer2));
                                    } else {
                                        newBuffer1.release();
                                        newBuffer2.release();
                                    }
                                }
                                return invocation.callRealMethod();
                            } catch (Exception | AssertionError error) {
                                duplicateInjectionFailure.compareAndSet(null, error);
                                completion().completeExceptionally(error);
                                throw error;
                            } finally {
                                writing.remove();
                            }
                        };
                    doAnswer(injectedAnswer).when(spyContext).write(any());
                    doAnswer(injectedAnswer).when(spyContext).write(any(), any(ChannelPromise.class));
                    doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any());
                    doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any(), any(ChannelPromise.class));
                    return spyContext;
                }
                };
                connections.add(connection);
                return connection;
            });

        AtomicReference<Runnable> clearInjection = new AtomicReference<>(taskToClearInjection);
        List<AutoCloseable> resources = new ArrayList<>();
        Throwable testFailure = null;
        try {
            assertThat(originalInterceptor).as("destination interceptor before the matrix case").isNull();
            broker2.setInterceptor(publishTracker);
            // 1. Create topics and enable deduplication.
            admin1.topics().createNonPartitionedTopic(topicName);
            admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
            admin2.topics().createNonPartitionedTopic(topicName);
            admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                PersistentTopic persistentTopic1 =
                        (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
                PersistentTopic persistentTopic2 =
                        (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
                admin1.topicPolicies().setDeduplicationStatus(topicName, true);
                admin1.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
                admin2.topicPolicies().setDeduplicationStatus(topicName, true);
                admin2.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
                MessageDeduplication messageDeduplication1 = persistentTopic1.getMessageDeduplication();
                assertThat(messageDeduplication1).isNotNull();
                assertThat(String.valueOf(messageDeduplication1.getStatus())).isEqualTo("Enabled");
                MessageDeduplication messageDeduplication2 = persistentTopic2.getMessageDeduplication();
                assertThat(messageDeduplication2).isNotNull();
                assertThat(String.valueOf(messageDeduplication2.getStatus())).isEqualTo("Enabled");
                assertEquals(persistentTopic1.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
                assertEquals(persistentTopic1.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
                assertEquals(persistentTopic2.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
                assertEquals(persistentTopic2.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            });
            PersistentTopic tp1 =
                    (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
            PersistentTopic tp2 =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            destination.set(tp2);

            // 2, Publish messages.
            List<String> msgSent = new ArrayList<>();
            Producer<Integer> p1 = client1.newProducer(Schema.INT32).topic(topicName).create();
            resources.add(p1);
            Producer<Integer> p2 = client1.newProducer(Schema.INT32).topic(topicName).create();
            resources.add(p2);
            Producer<String> p3 = client1.newProducer(Schema.STRING).topic(topicName).create();
            resources.add(p3);
            Producer<Boolean> p4 = client1.newProducer(Schema.BOOL).topic(topicName).create();
            resources.add(p4);
            for (int i = 0; i < 10; i++) {
                p1.send(i);
                msgSent.add(String.valueOf(i));
            }
            for (int i = 10; i < 200; i++) {
                int msg1 = i;
                int msg2 = 1000 + i;
                String msg3 = (2000 + i) + "";
                boolean msg4 = i % 2 == 0;
                p1.send(msg1);
                p2.send(msg2);
                msgSent.add(String.valueOf(msg1));
                msgSent.add(String.valueOf(msg2));
                if (multiSchemas) {
                    p3.send(msg3);
                    p4.send(msg4);
                    msgSent.add(String.valueOf(msg3));
                    msgSent.add(String.valueOf(msg4));
                }
            }
            p1.close();
            p2.close();
            p3.close();
            p4.close();

            // 3. Enable replication and wait the task to be finished.
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
            waitReplicatorStarted(topicName);
            awaitOriginalReplication(tp1, duplicateInjectionFailure);
            // Verify: all messages were copied correctly.
            List<String> msgReceived = new ArrayList<>();
            List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
            Consumer<GenericRecord> consumer = client2.newConsumer(Schema.AUTO_CONSUME()).topic(topicName)
                    .subscriptionName("s1").subscribe();
            resources.add(consumer);
            for (int i = 0; i < msgSent.size(); i++) {
                Message<GenericRecord> msg = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(msg).as("replicated message %s", i).isNotNull();
                MessageIdAdv messageIdAdv = (MessageIdAdv) msg.getMessageId();
                log.info().attr("msgSource", StringUtils.join(msg.getProperties().values(), ":"))
                        .attr("target", messageIdAdv.getLedgerId()).attr("entryId", messageIdAdv.getEntryId())
                        .log("received msg. source, target");
                msgReceived.add(String.valueOf(msg.getValue()));
                acknowledgements.add(consumer.acknowledgeAsync(msg));
            }
            CompletableFuture.allOf(acknowledgements.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
            assertNoLateMessages(consumer);
            injectDuplicates.set(false);
            for (var duplicates : duplicatedMsgs.entrySet()) {
                duplicates.getKey().flushTailDuplicates(duplicates.getValue()).get(30, TimeUnit.SECONDS);
            }
            stopAndDrainReplication(topicName, tp1, tp2, connections);
            assertThat(duplicateInjectionFailure.get()).as("replication duplicate injection").isNull();
            publishTracker.assertDuplicatesReceived();
            if (injectRepeatedPublish) {
                assertThat(publishTracker.injectedDuplicateCount()).as("injected repeated publishes").isPositive();
            }
            assertReplicatedEntryCount(tp2, msgSent.size());
            log.info().attr("writeValueAsString",
                    JACKSON.writeValueAsString(admin1.topics().getInternalStats(topicName)))
                    .log("c1 topic stats-internal");
            log.info().attr("writeValueAsString",
                    JACKSON.writeValueAsString(admin2.topics().getInternalStats(topicName)))
                    .log("c2 topic stats-internal");
            log.info().attr("writeValueAsString", JACKSON.writeValueAsString(admin1.topics().getStats(topicName)))
                    .log("c1 topic stats-internal");
            log.info().attr("writeValueAsString", JACKSON.writeValueAsString(admin2.topics().getStats(topicName)))
                    .log("c2 topic stats-internal");
            assertEquals(msgReceived, msgSent);
            consumer.close();
            for (ReplicationDrainClientCnx connection : connections) {
                if (connection.hasPublished()) {
                    connection.checkHealthy().get(10, TimeUnit.SECONDS);
                }
            }

            // Verify: the deduplication cursor has been acked.
            // The entry-count snapshot threshold remains ten messages.
            Awaitility.await().untilAsserted(() -> {
                assertThat(requireCursor(tp1, "pulsar.dedup").getNumberOfEntriesInBacklog(true)).isLessThan(10L);
                assertThat(requireCursor(tp2, "pulsar.dedup").getNumberOfEntriesInBacklog(true)).isLessThan(10L);
            });
            // Remove the injection.
            clearInjection.getAndSet(null).run();

            log.info("======  Verify: all messages will be replicated after reopening replication  ======");

            // Verify: all messages will be replicated after reopening replication.
            // Reopen replication: stop replication.
            admin2.topics().unload(topicName);
            admin2.topics().delete(topicName);
            // Reopen replication: enable replication.
            admin2.topics().createNonPartitionedTopic(topicName);
            admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                PersistentTopic persistentTopic2 =
                        (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
                admin2.topicPolicies().setDeduplicationStatus(topicName, true);
                admin2.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
                MessageDeduplication messageDeduplication2 = persistentTopic2.getMessageDeduplication();
                assertThat(messageDeduplication2).isNotNull();
                assertThat(String.valueOf(messageDeduplication2.getStatus())).isEqualTo("Enabled");
                assertEquals(persistentTopic2.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
                assertEquals(persistentTopic2.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            });
            List<ReplicationDrainClientCnx> reopenedConnections = new CopyOnWriteArrayList<>();
            PersistentTopic reopenedTarget =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().orElseThrow();
            Runnable clearReopenedInjection = injectReplicatorClientCnx((conf, eventLoopGroup) -> {
                ReplicationDrainClientCnx connection = new ReplicationDrainClientCnx(
                        conf, eventLoopGroup, () -> reopenedTarget, publishTracker);
                reopenedConnections.add(connection);
                return connection;
            });
            clearInjection.set(clearReopenedInjection);
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
            awaitOriginalReplication(tp1, duplicateInjectionFailure);
            // Reopen replication: consumption.
            List<String> msgReceived2 = new ArrayList<>();
            List<CompletableFuture<Void>> reopenedAcknowledgements = new ArrayList<>();
            Consumer<GenericRecord> consumer2 = client2.newConsumer(Schema.AUTO_CONSUME()).topic(topicName)
                    .subscriptionName("s1").subscribe();
            resources.add(consumer2);
            for (int i = 0; i < msgSent.size(); i++) {
                Message<GenericRecord> msg = consumer2.receive(10, TimeUnit.SECONDS);
                assertThat(msg).as("replicated message after reopening %s", i).isNotNull();
                MessageIdAdv messageIdAdv = (MessageIdAdv) msg.getMessageId();
                log.info().attr("msgSource", StringUtils.join(msg.getProperties().values(), ":"))
                        .attr("target", messageIdAdv.getLedgerId()).attr("entryId", messageIdAdv.getEntryId())
                        .log("received msg. source, target");
                msgReceived2.add(String.valueOf(msg.getValue()));
                reopenedAcknowledgements.add(consumer2.acknowledgeAsync(msg));
            }
            CompletableFuture.allOf(reopenedAcknowledgements.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
            assertNoLateMessages(consumer2);
            stopAndDrainReplication(topicName, tp1, reopenedTarget, reopenedConnections);
            assertThat(duplicateInjectionFailure.get()).as("replication duplicate injection").isNull();
            publishTracker.assertDuplicatesReceived();
            assertReplicatedEntryCount(reopenedTarget, msgSent.size());
            // Verify: all messages were copied correctly.
            log.info().attr("writeValueAsString",
                    JACKSON.writeValueAsString(admin1.topics().getInternalStats(topicName)))
                    .log("c1 topic stats-internal");
            log.info().attr("writeValueAsString",
                    JACKSON.writeValueAsString(admin2.topics().getInternalStats(topicName)))
                    .log("c2 topic stats-internal");
            log.info().attr("writeValueAsString", JACKSON.writeValueAsString(admin1.topics().getStats(topicName)))
                    .log("c1 topic stats-internal");
            log.info().attr("writeValueAsString", JACKSON.writeValueAsString(admin2.topics().getStats(topicName)))
                    .log("c2 topic stats-internal");
            assertEquals(msgReceived2, msgSent);
            consumer2.close();
            for (ReplicationDrainClientCnx connection : reopenedConnections) {
                if (connection.hasPublished()) {
                    connection.checkHealthy().get(10, TimeUnit.SECONDS);
                }
            }
            clearInjection.getAndSet(null).run();

            // cleanup.
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
        } catch (Exception | AssertionError error) {
            testFailure = error;
            throw error;
        } finally {
            injectDuplicates.set(false);
            for (List<ByteBufPair> duplicates : duplicatedMsgs.values()) {
                synchronized (duplicates) {
                    duplicates.forEach(ByteBufPair::release);
                    duplicates.clear();
                }
            }
            List<AutoCloseable> cleanupTasks = new ArrayList<>();
            cleanupTasks.add(() -> {
                try {
                    admin1.topics().setReplicationClusters(topicName, List.of(cluster1));
                } catch (PulsarAdminException.NotFoundException ignored) {
                    // The normal path has already deleted the topic.
                }
            });
            for (int i = resources.size() - 1; i >= 0; i--) {
                cleanupTasks.add(resources.get(i));
            }
            cleanupTasks.add(() -> {
                Runnable cleanup = clearInjection.getAndSet(null);
                if (cleanup != null) {
                    cleanup.run();
                }
            });
            cleanupTasks.add(() -> broker2.setInterceptor(originalInterceptor));
            cleanupTasks.add(() -> deleteTopicIfPresent(admin1, topicName));
            cleanupTasks.add(() -> deleteTopicIfPresent(admin2, topicName));
            closeAfterTest(cleanupTasks, testFailure);
        }
    }

    static final class FirstReplicationSend {
        // A new producer epoch or socket does not turn a replay of an existing source entry into its first send.
        private final Set<SourcePosition> sent = ConcurrentHashMap.newKeySet();

        boolean isFirstSend(ByteBufPair message) {
            ByteBuf header = message.getFirst().duplicate();
            header.readInt(); // Total frame size.
            header.skipBytes(header.readInt()); // Command size and command.
            MessageMetadata metadata = Commands.parseMessageMetadata(header);
            SourcePosition sourcePosition = null;
            for (var property : metadata.getPropertiesList()) {
                if (!MSG_PROP_REPL_SOURCE_POSITION.equals(property.getKey())) {
                    continue;
                }
                String[] components = property.getValue().split(":", -1);
                if (components.length != 2) {
                    throw new IllegalStateException("Invalid replication source position: " + property.getValue());
                }
                long ledgerId = Long.parseLong(components[0]);
                long entryId = Long.parseLong(components[1]);
                if (ledgerId < 0 || entryId < 0) {
                    throw new IllegalStateException("Invalid replication source position: " + property.getValue());
                }
                SourcePosition parsed = new SourcePosition(ledgerId, entryId);
                if (sourcePosition != null && !sourcePosition.equals(parsed)) {
                    throw new IllegalStateException("Conflicting replication source positions");
                }
                sourcePosition = parsed;
            }
            if (sourcePosition == null) {
                throw new IllegalStateException("Replicated publish is missing its source position");
            }
            return sent.add(sourcePosition);
        }
    }

    private record SourcePosition(long ledgerId, long entryId) {
    }

    private static void deleteTopicIfPresent(PulsarAdmin admin, String topicName) throws Exception {
        try {
            admin.topics().delete(topicName, true);
        } catch (PulsarAdminException.NotFoundException ignored) {
            // It may have been deleted before a later assertion or cleanup failed.
        }
    }

    static void closeAfterTest(List<AutoCloseable> actions, Throwable failure) throws Exception {
        Throwable firstFailure = failure;
        for (AutoCloseable action : actions) {
            try {
                action.close();
            } catch (Exception | AssertionError error) {
                if (firstFailure == null) {
                    firstFailure = error;
                } else if (firstFailure != error) {
                    firstFailure.addSuppressed(error);
                }
            }
        }
        if (failure == null && firstFailure != null) {
            if (firstFailure instanceof Exception error) {
                throw error;
            }
            throw (AssertionError) firstFailure;
        }
    }

    protected void awaitOriginalReplication(PersistentTopic source) {
        awaitOriginalReplication(source, new AtomicReference<>());
    }

    private void awaitOriginalReplication(PersistentTopic source, AtomicReference<Throwable> injectionFailure) {
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            if (injectionFailure.get() != null) {
                throw new IllegalStateException("Cannot inject replication duplicates", injectionFailure.get());
            }
            GeoPersistentReplicator replicator = (GeoPersistentReplicator) source.getReplicators().get(cluster2);
            assertThat(replicator).as("source replicator must exist").isNotNull();
            assertThat(replicator.getCursor().getNumberOfEntriesInBacklog(true))
                    .as("original replication backlog").isZero();
            assertThat(replicator.getProducer()).as("source replication producer").isNotNull();
            assertThat(((ProducerImpl<?>) replicator.getProducer()).getPendingQueueSize())
                    .as("original replication sends").isZero();
        });
    }

    protected static ManagedCursor requireCursor(PersistentTopic topic, String name) {
        for (ManagedCursor cursor : topic.getManagedLedger().getCursors()) {
            if (cursor.getName().equals(name)) {
                return cursor;
            }
        }
        throw new AssertionError("Missing cursor " + name + " on " + topic.getName());
    }

    protected void stopAndDrainReplication(String topicName, PersistentTopic source, PersistentTopic target,
                                        List<ReplicationDrainClientCnx> connections) throws Exception {
        List<ReplicationDrainClientCnx> publishingConnections = connections.stream()
                .filter(ReplicationDrainClientCnx::hasPublished).toList();
        assertThat(publishingConnections).as("observed replication connections").isNotEmpty();
        for (ReplicationDrainClientCnx connection : publishingConnections) {
            connection.seal().get(10, TimeUnit.SECONDS);
        }
        // Closing the source producer prevents later schema/retry/read tasks from adding more sends.
        admin1.topics().setReplicationClusters(topicName, List.of(cluster1));
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(source.getReplicators()).doesNotContainKey(cluster2));
        for (ReplicationDrainClientCnx connection : publishingConnections) {
            // Require the broker's actual CLOSE_PRODUCER success, including every injected wire request.
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                    connection.completion().isDone() || connection.isDisconnected());
            if (connection.isDisconnected() && !connection.completion().isDone()) {
                // A lost socket can leave writes running after its producer has been removed.
                Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    assertThat(connection.isDestinationDisconnected()).as("retired destination socket").isTrue();
                    assertThat(target.getPendingWriteOps().get()).as("retired destination writes").isZero();
                });
                connection.retireDisconnected().get(10, TimeUnit.SECONDS);
            }
            connection.completion().get(30, TimeUnit.SECONDS);
            connection.checkHealthy().get(10, TimeUnit.SECONDS);
        }
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(target.getPendingWriteOps().get()).as("destination pending writes").isZero();
            assertThat(target.getProducers()).as("destination replication producers").isEmpty();
            assertThat(connections.stream().filter(ReplicationDrainClientCnx::hasPublished).toList())
                    .as("no publishing connection appeared while closing replication")
                    .containsExactlyElementsOf(publishingConnections);
        });
    }

    protected static void assertNoLateMessages(Consumer<?> consumer) throws PulsarClientException {
        assertThat(consumer.receive(10, TimeUnit.SECONDS))
                .as("no late duplicate while replication remains active").isNull();
    }

    protected static void assertReplicatedEntryCount(PersistentTopic target, int expectedCount) {
        // Count successful appends even when normal consumer ACKs allow old ledgers to be trimmed.
        assertThat(((ManagedLedgerImpl) target.getManagedLedger()).getEntriesAddedCounter())
                .as("persisted replicated entries").isEqualTo(expectedCount);
    }

}
