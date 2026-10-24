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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.GeoReplicationProducerImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class ReplicationDrainClientCnxTest {
    private static final String TOPIC = "persistent://public/default/replication-drain";

    @Test
    public void testOrdinaryPersistenceFailureIsRejected() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.sendError(1, "Producer is closed");
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
            assertThatThrownBy(() -> fixture.connection.completion().join())
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("Replication send failed: PersistenceError: Producer is closed");
        }
    }

    @Test
    public void testExpectedInFlightDuplicateErrorIsAccepted() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(1)).getMessage());
            fixture.closeProducer(1, 10);
            fixture.connection.handleSuccess(new CommandSuccess().setRequestId(10));
            assertThat(fixture.connection.completion()).isCompleted();
        }
    }

    @Test
    public void testEachProducerMayReturnItsOwnInFlightDuplicateError() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.send(2);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(1)).getMessage());
            fixture.sendError(2, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(2)).getMessage());
            fixture.closeProducer(1, 10);
            fixture.closeProducer(2, 11);
            fixture.connection.handleSuccess(new CommandSuccess().setRequestId(10));
            assertThat(fixture.connection.completion()).isNotDone();
            fixture.connection.handleSuccess(new CommandSuccess().setRequestId(11));
            assertThat(fixture.connection.completion()).isCompleted();
        }
    }

    @Test
    public void testDuplicateErrorFromAnotherProducerIsRejected() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(2)).getMessage());
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
        }
    }

    @Test
    public void testDuplicateErrorFromAnotherTopicIsRejected() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC + "-other", producerName(1)).getMessage());
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
        }
    }

    @Test
    public void testAnotherProducerReceiptCannotHideMissingReceipt() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.send(2);
            fixture.receipt(1);
            fixture.receipt(1);
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
            assertThatThrownBy(() -> fixture.connection.completion().join())
                    .hasRootCauseMessage("Received an unaccounted replication receipt");
        }
    }

    @Test
    public void testCloseSuccessWaitsForEveryReceipt() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.closeProducer(1, 10);
            fixture.connection.handleSuccess(new CommandSuccess().setRequestId(10));
            assertThat(fixture.connection.completion()).isNotDone();
            fixture.receipt(1);
            assertThat(fixture.connection.completion()).isCompleted();
        }
    }

    @Test
    public void testFailedCloseResponseFailsTheBarrier() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.receipt(1);
            fixture.closeProducer(1, 10);
            fixture.connection.handleError(new CommandError().setRequestId(10)
                    .setError(ServerError.PersistenceError).setMessage("close failed"));
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
            assertThatThrownBy(() -> fixture.connection.completion().join())
                    .hasRootCauseMessage("Replication producer close failed: PersistenceError");
        }
    }

    @Test
    public void testUnexpectedDisconnectCannotRetireMissingReceipts() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            fixture.channel.close();
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
            fixture.assertUnexpectedClose();
            assertThatThrownBy(() -> fixture.execute(fixture.connection.retireDisconnected()).join())
                    .hasRootCauseInstanceOf(ClosedChannelException.class);
        }
    }

    @Test
    public void testDisconnectRetirementRequiresAnAcceptedDelayedDuplicate() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.send(1);
            assertThatThrownBy(() -> fixture.execute(fixture.connection.allowDisconnectedRetirement()).join())
                    .hasRootCauseMessage("No accepted delayed duplicate is available for disconnect retirement");
        }
    }

    @Test
    public void testKnownRecoveryCanCloseAfterDuplicateReceptionDespiteLostReceipt() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.send(1);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(1)).getMessage());
            ByteBufPair pair = ReplicationPublishTrackerTest.command("1:2");
            try {
                fixture.tracker.recordInjectedDuplicate(pair, fixture.channel.localAddress());
                ReplicationPublishTrackerTest.publish(fixture.tracker, pair, TOPIC, fixture.channel.localAddress());
                ReplicationPublishTrackerTest.publish(fixture.tracker, pair, TOPIC, fixture.channel.localAddress());
            } finally {
                pair.release();
            }
            fixture.closeProducer(1, 10);
            fixture.connection.handleSuccess(new CommandSuccess().setRequestId(10));
            assertThat(fixture.connection.completion()).isCompleted();
        }
    }

    @Test
    public void testKnownRecoveryCannotRetireAnUnreceivedDuplicate() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.send(1);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(1)).getMessage());
            ByteBufPair pair = ReplicationPublishTrackerTest.command("1:2");
            try {
                fixture.tracker.recordInjectedDuplicate(pair, fixture.channel.localAddress());
                ReplicationPublishTrackerTest.publish(fixture.tracker, pair, TOPIC, fixture.channel.localAddress());
            } finally {
                pair.release();
            }
            fixture.closeProducer(1, 10);
            fixture.channel.close();
            assertThat(fixture.connection.completion()).isNotDone();
            assertThatThrownBy(() -> fixture.execute(fixture.connection.retireDisconnected()).join())
                    .hasRootCauseMessage(
                            "Cannot retire replication recovery before injected duplicates reached the destination");
            assertThat(fixture.connection.completion()).isNotDone();
        }
    }

    @Test
    public void testKnownRecoveryWithoutInjectedDuplicatesCanRetire() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.send(1);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(1)).getMessage());
            fixture.execute(fixture.connection.seal()).get(10, TimeUnit.SECONDS);
            fixture.channel.close();
            fixture.execute(fixture.connection.retireDisconnected()).get(10, TimeUnit.SECONDS);
            assertThat(fixture.connection.completion()).isCompleted();
        }
    }

    @Test
    public void testTrackerDoesNotAllowOrdinaryPersistenceFailure() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.sendError(1, "Producer is closed");
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
        }
    }

    @Test
    public void testDuplicateReceiptCanCauseLocalRecoveryBeforeInactiveCallback() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.send(1);
            fixture.recordReceivedDuplicate();
            fixture.connection.closeDuringReceipt = true;
            fixture.duplicateReceipt(1);
            assertThat(fixture.connection.completion()).isNotDone();
            fixture.execute(fixture.connection.seal()).get(10, TimeUnit.SECONDS);
            fixture.execute(fixture.connection.retireDisconnected()).get(10, TimeUnit.SECONDS);
            assertThat(fixture.connection.completion()).isCompleted();
        }
    }

    @Test
    public void testDuplicateReceiptRecoveryStillRequiresInjectedReception() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.send(1);
            ByteBufPair pair = ReplicationPublishTrackerTest.command("1:2");
            try {
                fixture.tracker.recordInjectedDuplicate(pair, fixture.channel.localAddress());
            } finally {
                pair.release();
            }
            fixture.connection.closeDuringReceipt = true;
            fixture.duplicateReceipt(1);
            fixture.execute(fixture.connection.seal()).get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> fixture.execute(fixture.connection.retireDisconnected()).join())
                    .hasRootCauseMessage(
                            "Cannot retire replication recovery before injected duplicates reached the destination");
        }
    }

    @Test
    public void testDuplicateReceiptDoesNotAuthorizeALaterExternalDisconnect() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.send(1);
            fixture.recordReceivedDuplicate();
            fixture.duplicateReceipt(1);
            fixture.channel.close();
            fixture.assertUnexpectedClose();
        }
    }

    @Test
    public void testOrdinaryReceiptCannotAuthorizeLocalRecovery() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.connection.closeDuringReceipt = true;
            CommandSendReceipt receipt = new CommandSendReceipt().setProducerId(1).setSequenceId(0)
                    .setHighestSequenceId(0);
            receipt.setMessageId().setLedgerId(1).setEntryId(2);
            fixture.connection.handleSendReceipt(receipt);
            fixture.assertUnexpectedClose();
        }
    }

    @Test
    public void testMissingMessageIdCannotAuthorizeLocalRecovery() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.connection.closeDuringReceipt = true;
            fixture.receipt(1);
            fixture.assertUnexpectedClose();
        }
    }

    @Test
    public void testAnotherProducerDuplicateCannotAuthorizeLocalRecovery() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.connection.closeDuringReceipt = true;
            fixture.duplicateReceipt(2);
            fixture.assertUnexpectedClose();
        }
    }

    @Test
    public void testAlreadyAccountedDuplicateCannotAuthorizeLocalRecovery() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.receipt(1);
            fixture.connection.closeDuringReceipt = true;
            fixture.duplicateReceipt(1);
            fixture.assertUnexpectedClose();
        }
    }

    @Test
    public void testPrototypeDuplicateCannotAuthorizeLocalRecovery() throws Exception {
        try (Fixture fixture = new Fixture(false, true)) {
            fixture.send(1);
            fixture.connection.closeDuringReceipt = true;
            fixture.duplicateReceipt(1);
            fixture.assertUnexpectedClose();
        }
    }

    @Test
    public void testRetiredSocketProducerRequestStillFailsItsClientPromise() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.enterRecovery();
            ChannelFuture request = fixture.failWrite(BaseCommand.Type.PRODUCER, true, new ClosedChannelException());
            assertThat(request.isDone()).isTrue();
            assertThat(request.cause()).isInstanceOf(ClosedChannelException.class);
            fixture.execute(fixture.connection.checkHealthy()).get(10, TimeUnit.SECONDS);
            assertThat(fixture.connection.completion()).isNotDone();
        }
    }

    @DataProvider(name = "fatalWriteFailures")
    public Object[][] fatalWriteFailures() {
        return new Object[][] {
                {BaseCommand.Type.PRODUCER, false, false, true, new ClosedChannelException()},
                {BaseCommand.Type.PRODUCER, true, true, true, new ClosedChannelException()},
                {BaseCommand.Type.PRODUCER, true, false, false, new ClosedChannelException()},
                {BaseCommand.Type.PRODUCER, true, false, true, new IOException("ordinary IO failure")},
                {BaseCommand.Type.SEND, true, false, true, new ClosedChannelException()},
                {BaseCommand.Type.CLOSE_PRODUCER, true, true, true, new ClosedChannelException()}
        };
    }

    @Test(dataProvider = "fatalWriteFailures")
    public void testOtherWriteFailuresRemainFatal(BaseCommand.Type command, boolean recovery, boolean sealed,
                                                boolean disconnect, Exception error) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            if (recovery) {
                fixture.enterRecovery();
            } else {
                fixture.send(1);
            }
            if (sealed) {
                fixture.execute(fixture.connection.seal()).get(10, TimeUnit.SECONDS);
            }
            ChannelFuture request = fixture.failWrite(command, disconnect, error);
            assertThat(request.isDone()).isTrue();
            assertThat(request.cause()).isSameAs(error);
            assertThat(fixture.connection.completion()).isCompletedExceptionally();
            assertThatThrownBy(() -> fixture.connection.completion().join())
                    .hasRootCauseInstanceOf(error.getClass());
            if (command == BaseCommand.Type.SEND) {
                assertThat(fixture.tracker.injectedDuplicateCount()).isOne();
                assertThatThrownBy(fixture.tracker::assertDuplicatesReceived).isInstanceOf(AssertionError.class);
            }
        }
    }

    @Test
    public void testTailDuplicatesAreRegisteredWhenActuallyFlushed() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            List<ByteBufPair> cached = new ArrayList<>();
            cached.add(ReplicationPublishTrackerTest.command("1:2"));
            assertThat(fixture.tracker.injectedDuplicateCount()).isZero();
            fixture.execute(fixture.connection.flushTailDuplicates(cached)).get(10, TimeUnit.SECONDS);
            assertThat(cached).isEmpty();
            assertThat(fixture.tracker.injectedDuplicateCount()).isOne();
            fixture.releaseOutbound();
            fixture.execute(fixture.connection.flushTailDuplicates(cached)).get(10, TimeUnit.SECONDS);
            assertThat(fixture.tracker.injectedDuplicateCount()).isOne();
        }
    }

    @Test
    public void testActualNormalRetriesCannotHideMissingInjectedDuplicate() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            fixture.send(1);
            fixture.send(1);
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(1)).getMessage());
            ByteBufPair pair = ReplicationPublishTrackerTest.command("1:2");
            try {
                ReplicationPublishTrackerTest.publish(fixture.tracker, pair, TOPIC, fixture.channel.localAddress());
                ReplicationPublishTrackerTest.publish(fixture.tracker, pair, TOPIC, fixture.channel.localAddress());
                fixture.tracker.recordInjectedDuplicate(pair, fixture.channel.localAddress());
            } finally {
                pair.release();
            }
            fixture.closeProducer(1, 10);
            fixture.connection.handleSuccess(new CommandSuccess().setRequestId(10));
            assertThat(fixture.connection.completion()).isNotDone();
        }
    }

    @Test
    public void testRecoveryDropsOnlyUnsentTailWhenSocketCloses() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.send(1);
            ByteBufPair submitted = ReplicationPublishTrackerTest.command("1:2");
            fixture.tracker.recordInjectedDuplicate(submitted, fixture.channel.localAddress());
            fixture.channel.writeOutbound(submitted);
            fixture.releaseOutbound();
            fixture.sendError(1, new MessageDeduplication.MessageDupUnknownException(
                    TOPIC, producerName(1)).getMessage());
            ByteBufPair unsent = ReplicationPublishTrackerTest.command("1:2");
            List<ByteBufPair> cached = new ArrayList<>(List.of(unsent));
            fixture.channel.close();
            fixture.execute(fixture.connection.flushTailDuplicates(cached)).get(10, TimeUnit.SECONDS);
            assertThat(cached).isEmpty();
            assertThat(unsent.refCnt()).isZero();
            assertThat(fixture.tracker.injectedDuplicateCount()).as("submitted duplicate remains tracked").isOne();
            assertThatThrownBy(fixture.tracker::assertDuplicatesReceived).isInstanceOf(AssertionError.class);
        }
    }

    private static String producerName(long producerId) {
        return "pulsar.repl.r1-" + producerId;
    }

    private static class Fixture implements AutoCloseable {
        private final EventLoopGroup eventLoops = new DefaultEventLoopGroup(1);
        private final PersistentTopic destination = mock(PersistentTopic.class);
        private final Map<String, Producer> producers = new HashMap<>();
        private final ReplicationPublishTracker tracker;
        private final TestConnection connection;
        private final EmbeddedChannel channel;

        Fixture() {
            this(false);
        }

        Fixture(boolean allowRecovery) {
            this(allowRecovery, false);
        }

        Fixture(boolean allowRecovery, boolean prototype) {
            when(destination.getName()).thenReturn(TOPIC);
            when(destination.getProducers()).thenReturn(producers);
            when(destination.getPendingWriteOps()).thenReturn(new AtomicLong());
            ClientConfigurationData configuration = new ClientConfigurationData();
            configuration.setKeepAliveIntervalSeconds(0);
            tracker = allowRecovery ? new ReplicationPublishTracker(TOPIC) : null;
            connection = prototype
                    ? new TestConnection(configuration, eventLoops, destination, new CompletableFuture<>())
                    : new TestConnection(configuration, eventLoops, destination, tracker);
            channel = new EmbeddedChannel(connection);
            connection.connected();
            releaseOutbound();
        }

        void send(long producerId) {
            Producer producer = mock(Producer.class);
            TransportCnx serverConnection = mock(TransportCnx.class);
            when(serverConnection.clientAddress()).thenReturn(channel.localAddress());
            when(producer.getProducerId()).thenReturn(producerId);
            when(producer.getProducerName()).thenReturn(producerName(producerId));
            when(producer.getCnx()).thenReturn(serverConnection);
            producers.put(producerName(producerId), producer);
            MessageMetadata metadata = new MessageMetadata().setProducerName("original").setSequenceId(0)
                    .setPublishTime(0);
            metadata.addProperty().setKey(GeoReplicationProducerImpl.MSG_PROP_REPL_SOURCE_POSITION).setValue("1:2");
            channel.writeOutbound(Commands.newSend(producerId, 0, 1, Commands.ChecksumType.None, metadata,
                    Unpooled.wrappedBuffer(new byte[] {1})));
            releaseOutbound();
        }

        void receipt(long producerId) {
            connection.handleSendReceipt(new CommandSendReceipt().setProducerId(producerId)
                    .setSequenceId(0).setHighestSequenceId(0));
        }

        void duplicateReceipt(long producerId) {
            CommandSendReceipt receipt = new CommandSendReceipt().setProducerId(producerId).setSequenceId(0)
                    .setHighestSequenceId(0);
            receipt.setMessageId().setLedgerId(-1).setEntryId(-1);
            connection.handleSendReceipt(receipt);
        }

        void recordReceivedDuplicate() {
            ByteBufPair pair = ReplicationPublishTrackerTest.command("1:2");
            try {
                tracker.recordInjectedDuplicate(pair, channel.localAddress());
                ReplicationPublishTrackerTest.publish(tracker, pair, TOPIC, channel.localAddress());
                ReplicationPublishTrackerTest.publish(tracker, pair, TOPIC, channel.localAddress());
            } finally {
                pair.release();
            }
        }

        void assertUnexpectedClose() {
            assertThat(connection.completion()).isCompletedExceptionally();
            assertThatThrownBy(() -> connection.completion().join()).hasCauseInstanceOf(ClosedChannelException.class);
        }

        void enterRecovery() {
            send(1);
            send(1);
            sendError(1, new MessageDeduplication.MessageDupUnknownException(TOPIC, producerName(1)).getMessage());
        }

        ChannelFuture failWrite(BaseCommand.Type commandType, boolean disconnect, Exception error) {
            Object command;
            if (commandType == BaseCommand.Type.SEND) {
                ByteBufPair duplicate = ReplicationPublishTrackerTest.command("1:2");
                tracker.recordInjectedDuplicate(duplicate, channel.localAddress());
                command = duplicate;
            } else if (commandType == BaseCommand.Type.CLOSE_PRODUCER) {
                command = Commands.newCloseProducer(1, 10);
            } else {
                BaseCommand producer = new BaseCommand().setType(BaseCommand.Type.PRODUCER);
                producer.setProducer().setProducerId(1).setRequestId(10).setTopic(TOPIC);
                command = Commands.serializeWithSize(producer);
            }
            channel.pipeline().addBefore("replication-drain", "fail-write", new ChannelOutboundHandlerAdapter() {
                @Override
                public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
                    if (disconnect) {
                        context.close();
                    }
                    ReferenceCountUtil.release(message);
                    promise.setFailure(error);
                }
            });
            return channel.writeAndFlush(command);
        }

        void sendError(long producerId, String message) {
            connection.handleSendError(new CommandSendError().setProducerId(producerId).setSequenceId(0)
                    .setError(ServerError.PersistenceError).setMessage(message));
        }

        void closeProducer(long producerId, long requestId) throws Exception {
            execute(connection.seal()).get(10, TimeUnit.SECONDS);
            channel.writeOutbound(Commands.newCloseProducer(producerId, requestId));
            releaseOutbound();
        }

        CompletableFuture<Void> execute(CompletableFuture<Void> action) {
            channel.runPendingTasks();
            return action;
        }

        private void releaseOutbound() {
            Object outbound;
            while ((outbound = channel.readOutbound()) != null) {
                ReferenceCountUtil.release(outbound);
            }
        }

        @Override
        public void close() throws Exception {
            try {
                channel.finishAndReleaseAll();
            } finally {
                eventLoops.shutdownGracefully(0, 0, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static class TestConnection extends ReplicationDrainClientCnx {
        private boolean closeDuringReceipt;

        TestConnection(ClientConfigurationData configuration, EventLoopGroup eventLoops, PersistentTopic destination,
                       ReplicationPublishTracker tracker) {
            super(configuration, eventLoops, () -> destination, tracker);
        }

        TestConnection(ClientConfigurationData configuration, EventLoopGroup eventLoops, PersistentTopic destination,
                       CompletableFuture<ReplicationDrainClientCnx> publishingConnection) {
            super(configuration, eventLoops, publishingConnection, () -> destination);
        }

        @Override
        void processReceipt(CommandSendReceipt receipt) {
            if (closeDuringReceipt) {
                // ProducerImpl.ackReceived requests this close before its receipt callback returns.
                channel().close();
            } else {
                super.processReceipt(receipt);
            }
        }

        void connected() {
            handleConnected(new CommandConnected().setServerVersion("test")
                    .setProtocolVersion(Commands.getCurrentProtocolVersion()));
        }
    }
}
