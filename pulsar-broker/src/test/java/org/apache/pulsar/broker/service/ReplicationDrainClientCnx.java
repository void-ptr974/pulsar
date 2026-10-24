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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.ByteBufPair;

/** Tracks replication requests through their terminal responses and a graceful broker close. */
class ReplicationDrainClientCnx extends ClientCnx {
    private final CompletableFuture<ReplicationDrainClientCnx> publishingConnection;
    private final Supplier<PersistentTopic> destination;
    private final ReplicationPublishTracker publishTracker;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    // The counters and retained buffer are accessed only on this channel's event loop.
    private volatile ChannelHandlerContext outboundContext;
    private volatile boolean published;
    private volatile boolean disconnected;
    private volatile TransportCnx destinationConnection;
    private volatile Throwable failure;
    private final Set<Long> producerIds = new HashSet<>();
    private final Set<Long> closingProducerIds = new HashSet<>();
    private final Set<Long> closedProducerIds = new HashSet<>();
    private final Map<Long, Long> closeRequests = new HashMap<>();
    private final Map<Long, Integer> pendingReceipts = new HashMap<>();
    private final Map<Long, String> expectedDeduplicationErrors = new HashMap<>();
    private PersistentTopic publishingDestination;
    private ByteBufPair delayedDuplicate;
    private int expectedReceipts;
    private int receivedReceipts;
    private boolean sealed;
    private boolean allowDisconnectedRetirement;
    private boolean handlingDuplicateReceipt;
    private volatile boolean knownRecovery;

    ReplicationDrainClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) {
        this(conf, eventLoopGroup, null, null, null);
    }

    ReplicationDrainClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                             Supplier<PersistentTopic> destination) {
        this(conf, eventLoopGroup, null, destination, null);
    }

    ReplicationDrainClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                             Supplier<PersistentTopic> destination, ReplicationPublishTracker publishTracker) {
        this(conf, eventLoopGroup, null, destination, publishTracker);
    }

    ReplicationDrainClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                             CompletableFuture<ReplicationDrainClientCnx> publishingConnection) {
        this(conf, eventLoopGroup, publishingConnection, null, null);
    }

    ReplicationDrainClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                             CompletableFuture<ReplicationDrainClientCnx> publishingConnection,
                             Supplier<PersistentTopic> destination) {
        this(conf, eventLoopGroup, publishingConnection, destination, null);
    }

    private ReplicationDrainClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                                     CompletableFuture<ReplicationDrainClientCnx> publishingConnection,
                                     Supplier<PersistentTopic> destination, ReplicationPublishTracker publishTracker) {
        super(InstrumentProvider.NOOP, conf, eventLoopGroup);
        this.publishingConnection = publishingConnection;
        this.destination = destination;
        this.publishTracker = publishTracker;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.pipeline().addBefore(ctx.name(), "replication-drain", new ChannelOutboundHandlerAdapter() {
            @Override
            public void handlerAdded(ChannelHandlerContext context) {
                outboundContext = context;
            }

            @Override
            public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) throws Exception {
                boolean producerRequest;
                try {
                    BaseCommand command = decodeCommand(message);
                    producerRequest = command != null && command.getType() == BaseCommand.Type.PRODUCER;
                    if (command != null && command.getType() == BaseCommand.Type.SEND) {
                        recordSend((ByteBufPair) message, command.getSend().getProducerId());
                    } else if (command != null && command.getType() == BaseCommand.Type.CLOSE_PRODUCER && sealed) {
                        long producerId = command.getCloseProducer().getProducerId();
                        if (producerIds.contains(producerId)) {
                            closingProducerIds.add(producerId);
                            closeRequests.put(command.getCloseProducer().getRequestId(), producerId);
                        }
                    }
                } catch (Exception error) {
                    ReferenceCountUtil.safeRelease(message);
                    promise.tryFailure(error);
                    fail(error);
                    return;
                }
                context.write(message, promise.unvoid()).addListener(future -> {
                    // A late reconnect callback can try creating its producer on the socket being retired.
                    // Preserve the failed promise for the client's retry without treating it as a lost publish.
                    boolean retryingProducer = producerRequest && canRecover() && !sealed
                            && !context.channel().isActive() && future.cause() instanceof ClosedChannelException;
                    if (!future.isSuccess() && !retryingProducer) {
                        fail(future.cause());
                    }
                });
            }

            @Override
            public void close(ChannelHandlerContext context, ChannelPromise promise) throws Exception {
                if (handlingDuplicateReceipt) {
                    // Compatibility producers can close while processing an out-of-order duplicate ACK.
                    // Observe the close request here because channelInactive can run after receipt handling returns.
                    knownRecovery = true;
                }
                context.close(promise);
            }
        });
        super.channelActive(ctx);
    }

    private static BaseCommand decodeCommand(Object message) {
        ByteBuf header;
        if (message instanceof ByteBufPair pair) {
            header = pair.getFirst().duplicate();
        } else if (message instanceof ByteBuf buffer) {
            header = buffer.duplicate();
        } else {
            return null;
        }
        header.readInt(); // Total frame size.
        int commandSize = header.readInt();
        BaseCommand command = new BaseCommand();
        command.parseFrom(header, commandSize);
        return command;
    }

    private void recordSend(ByteBufPair pair, long producerId) {
        published = true;
        requireHealthy();
        if (completion.isDone() || closingProducerIds.contains(producerId)
                || (sealed && publishingConnection != null)) {
            throw new IllegalStateException("Replication sent another command after the drain boundary");
        }
        if (destination != null && !expectedDeduplicationErrors.containsKey(producerId)) {
            if (publishingDestination == null) {
                publishingDestination = destination.get();
            }
            Producer publishingProducer = publishingDestination.getProducers().values().stream()
                    .filter(producer -> producer.getProducerId() == producerId
                            && producer.getCnx().clientAddress().equals(outboundContext.channel().localAddress()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Cannot find the destination publishing connection"));
            destinationConnection = publishingProducer.getCnx();
            expectedDeduplicationErrors.put(producerId, new MessageDeduplication.MessageDupUnknownException(
                    publishingDestination.getName(), publishingProducer.getProducerName()).getMessage());
        }
        producerIds.add(producerId);
        if (publishTracker != null) {
            publishTracker.recordOutboundSend(pair, outboundContext.channel().localAddress());
        }
        if (expectedReceipts == 0 && publishingConnection != null) {
            if (!publishingConnection.complete(this)) {
                ReplicationDrainClientCnx previous = publishingConnection.getNow(null);
                IllegalStateException error =
                        new IllegalStateException("Replication changed publishing connections");
                if (previous != null) {
                    previous.runOnEventLoop(result -> {
                        previous.fail(error);
                        result.complete(null);
                    });
                }
                throw error;
            }
            delayedDuplicate = ByteBufPair.get(pair.getFirst().retainedDuplicate(),
                    pair.getSecond().retainedDuplicate());
            expectedReceipts++; // Reserve the prototype duplicate before it can be released.
            pendingReceipts.merge(producerId, 1, Integer::sum);
        }
        expectedReceipts++;
        pendingReceipts.merge(producerId, 1, Integer::sum);
    }

    @Override
    protected void handleSendReceipt(CommandSendReceipt receipt) {
        handlingDuplicateReceipt = publishTracker != null && publishingConnection == null
                && pendingReceipts.getOrDefault(receipt.getProducerId(), 0) > 0
                && expectedDeduplicationErrors.containsKey(receipt.getProducerId())
                && receipt.hasMessageId() && receipt.getMessageId().getLedgerId() == -1
                && receipt.getMessageId().getEntryId() == -1;
        try {
            processReceipt(receipt);
        } finally {
            handlingDuplicateReceipt = false;
        }
        recordTerminalResponse(receipt.getProducerId());
    }

    // Keep the producer callback separate so EmbeddedChannel tests can exercise its local close path.
    void processReceipt(CommandSendReceipt receipt) {
        super.handleSendReceipt(receipt);
    }

    private void recordTerminalResponse(long producerId) {
        Integer pending = pendingReceipts.get(producerId);
        if (pending == null || pending == 0) {
            fail(new IllegalStateException("Received an unaccounted replication receipt"));
            return;
        }
        pendingReceipts.put(producerId, pending - 1);
        receivedReceipts++;
        maybeComplete();
    }

    @Override
    protected void handleSendError(CommandSendError error) {
        if (publishingConnection != null || error.getError() != ServerError.PersistenceError
                || !error.getMessage().equals(expectedDeduplicationErrors.get(error.getProducerId()))) {
            super.handleSendError(error);
            fail(new IllegalStateException("Replication send failed: " + error.getError() + ": " + error.getMessage()));
            return;
        }
        knownRecovery = true;
        super.handleSendError(error);
        recordTerminalResponse(error.getProducerId());
    }

    @Override
    protected void handleSuccess(CommandSuccess success) {
        super.handleSuccess(success);
        Long producerId = closeRequests.remove(success.getRequestId());
        if (producerId != null) {
            closedProducerIds.add(producerId);
            maybeComplete();
        }
    }

    @Override
    protected void handleError(CommandError error) {
        if (closeRequests.containsKey(error.getRequestId())) {
            fail(new IllegalStateException("Replication producer close failed: " + error.getError()));
        }
        super.handleError(error);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        disconnected = true;
        if (!allowDisconnectedRetirement && !canRecover() && published
                && (!completion.isDone() || completion.isCompletedExceptionally())) {
            fail(new ClosedChannelException());
        }
        super.channelInactive(ctx);
    }

    boolean hasPublished() {
        return published;
    }

    boolean isDisconnected() {
        return disconnected;
    }

    boolean isDestinationDisconnected() {
        TransportCnx connection = destinationConnection;
        return connection != null && !connection.isActive();
    }

    CompletableFuture<Void> retireDisconnected() {
        return runOnEventLoop(result -> {
            requireHealthy();
            if ((!allowDisconnectedRetirement && !canRecover()) || !sealed || !disconnected
                    || !isDestinationDisconnected()
                    || publishingDestination.getPendingWriteOps().get() != 0 || delayedDuplicate != null) {
                result.completeExceptionally(new IllegalStateException(
                        "Disconnected replication connection has not drained on the destination"));
                return;
            }
            if (canRecover() && !publishTracker.duplicatesReceived()) {
                result.completeExceptionally(new IllegalStateException(
                        "Cannot retire replication recovery before injected duplicates reached the destination"));
                return;
            }
            // Recovery may lose receipts, but the destination must have executed every injected duplicate.
            // The prototype's explicit disconnect instead proves its delayed write was accepted before closing.
            completion.complete(null);
            result.complete(null);
        });
    }

    CompletableFuture<Void> allowDisconnectedRetirement() {
        return runOnEventLoop(result -> {
            requireHealthy();
            if (publishingConnection == null || publishingDestination == null || delayedDuplicate != null
                    || expectedReceipts - receivedReceipts != 1
                    || publishingDestination.getPendingWriteOps().get() == 0) {
                throw new IllegalStateException("No accepted delayed duplicate is available for disconnect retirement");
            }
            allowDisconnectedRetirement = true;
            result.complete(null);
        });
    }

    CompletableFuture<Void> checkHealthy() {
        return runOnEventLoop(result -> {
            requireHealthy();
            result.complete(null);
        });
    }

    CompletableFuture<Void> seal() {
        return runOnEventLoop(result -> {
            requireHealthy();
            if (sealed) {
                result.complete(null);
                return;
            }
            if (publishingConnection != null && (expectedReceipts == 0 || delayedDuplicate == null)) {
                throw new IllegalStateException("No delayed duplicate is available to seal");
            }
            sealed = true;
            maybeComplete();
            result.complete(null);
        });
    }

    CompletableFuture<Void> completion() {
        if (canRecover() && !completion.isDone()) {
            // The last close ACK can precede persistence callbacks from a retired producer's connection.
            runOnEventLoop(result -> {
                maybeComplete();
                result.complete(null);
            });
        }
        return completion;
    }

    CompletableFuture<Void> flushTailDuplicates(List<ByteBufPair> cachedDuplicates) {
        return runOnEventLoop(result -> {
            requireHealthy();
            List<CompletableFuture<Void>> writes = new ArrayList<>();
            List<ByteBufPair> pendingDuplicates;
            synchronized (cachedDuplicates) {
                pendingDuplicates = new ArrayList<>(cachedDuplicates);
                cachedDuplicates.clear();
            }
            if (canRecover() && (disconnected || !outboundContext.channel().isActive())) {
                // These cached frames were never submitted, so they have not entered the injected-send ledger.
                // A recovered producer will use its new socket; old producer frames must remain on this socket.
                pendingDuplicates.forEach(ReferenceCountUtil::safeRelease);
                result.complete(null);
                return;
            }
            int submitted = 0;
            try {
                while (submitted < pendingDuplicates.size()) {
                    if (publishTracker != null) {
                        publishTracker.recordInjectedDuplicate(pendingDuplicates.get(submitted),
                                outboundContext.channel().localAddress());
                    }
                    ChannelFuture writeFuture = outboundContext.channel()
                            .writeAndFlush(pendingDuplicates.get(submitted));
                    submitted++;
                    CompletableFuture<Void> write = new CompletableFuture<>();
                    writes.add(write);
                    writeFuture.addListener(future -> {
                        if (future.isSuccess()) {
                            write.complete(null);
                        } else {
                            fail(future.cause());
                            write.completeExceptionally(future.cause());
                        }
                    });
                }
            } finally {
                while (submitted < pendingDuplicates.size()) {
                    ReferenceCountUtil.safeRelease(pendingDuplicates.get(submitted++));
                }
            }
            CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).whenComplete((ignored, error) -> {
                if (error == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(error);
                }
            });
        });
    }

    CompletableFuture<Void> releaseDelayedDuplicate() {
        return runOnEventLoop(result -> {
            if (!sealed || delayedDuplicate == null || completion.isDone()) {
                throw new IllegalStateException("No sealed delayed duplicate is available to release");
            }
            ByteBufPair duplicate = delayedDuplicate;
            delayedDuplicate = null;
            outboundContext.writeAndFlush(duplicate).addListener(future -> {
                if (future.isSuccess()) {
                    result.complete(null);
                } else {
                    fail(future.cause());
                    result.completeExceptionally(future.cause());
                }
            });
            maybeComplete();
        });
    }

    CompletableFuture<Void> discardDelayedDuplicate() {
        return runOnEventLoop(result -> {
            fail(new IllegalStateException("Replication drain prototype cleaned up"));
            result.complete(null);
        });
    }

    private CompletableFuture<Void> runOnEventLoop(Consumer<CompletableFuture<Void>> action) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ChannelHandlerContext context = outboundContext;
        if (context == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Connection is not active"));
        }
        try {
            context.executor().execute(() -> {
                try {
                    action.accept(result);
                } catch (Exception error) {
                    fail(error);
                    result.completeExceptionally(error);
                }
            });
        } catch (RuntimeException error) {
            failure = error;
            completion.completeExceptionally(error);
            result.completeExceptionally(error);
        }
        return result;
    }

    private void maybeComplete() {
        boolean closed = closedProducerIds.containsAll(producerIds);
        boolean recovered = canRecover() && publishTracker.duplicatesReceived()
                && publishingDestination.getPendingWriteOps().get() == 0;
        if (sealed && closed && delayedDuplicate == null && (receivedReceipts == expectedReceipts || recovered)) {
            completion.complete(null);
        }
    }

    private boolean canRecover() {
        return publishTracker != null && knownRecovery;
    }

    private void fail(Throwable error) {
        if (failure == null) {
            failure = error;
        }
        completion.completeExceptionally(failure);
        ReferenceCountUtil.safeRelease(delayedDuplicate);
        delayedDuplicate = null;
    }

    private void requireHealthy() {
        if (failure != null) {
            throw new CompletionException(failure);
        }
    }
}
