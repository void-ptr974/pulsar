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
import io.netty.buffer.ByteBuf;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.intercept.MockBrokerInterceptor;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;

/** Proves that injected duplicates reached the destination's publish and deduplication path. */
class ReplicationPublishTracker extends MockBrokerInterceptor {
    private final String topicName;
    private final Map<ConnectionPosition, AtomicLong> injectedDuplicates = new ConcurrentHashMap<>();
    private final Map<ConnectionPosition, AtomicLong> outbound = new ConcurrentHashMap<>();
    private final Map<ConnectionPosition, AtomicLong> received = new ConcurrentHashMap<>();
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

    ReplicationPublishTracker(String topicName) {
        this.topicName = topicName;
    }

    void recordInjectedDuplicate(ByteBufPair duplicate, SocketAddress clientAddress) {
        ConnectionPosition position = outboundPosition(duplicate, clientAddress);
        injectedDuplicates.computeIfAbsent(position, ignored -> new AtomicLong()).incrementAndGet();
    }

    void recordOutboundSend(ByteBufPair command, SocketAddress clientAddress) {
        ConnectionPosition position = outboundPosition(command, clientAddress);
        outbound.computeIfAbsent(position, ignored -> new AtomicLong()).incrementAndGet();
    }

    private static ConnectionPosition outboundPosition(ByteBufPair command, SocketAddress clientAddress) {
        ByteBuf header = command.getFirst().duplicate();
        header.readInt(); // Total frame size.
        header.skipBytes(header.readInt()); // Command size and command.
        return new ConnectionPosition(clientAddress, sourcePosition(header));
    }

    @Override
    public void onMessagePublish(Producer producer, ByteBuf headersAndPayload, Topic.PublishContext publishContext) {
        if (!topicName.equals(producer.getTopic().getName())) {
            return;
        }
        try {
            ConnectionPosition position = new ConnectionPosition(producer.getCnx().clientAddress(),
                    sourcePosition(headersAndPayload.duplicate()));
            received.computeIfAbsent(position, ignored -> new AtomicLong()).incrementAndGet();
        } catch (RuntimeException error) {
            // Report decoder failures to the test thread instead of losing them in an IO callback.
            failure.compareAndSet(null, error);
        }
    }

    boolean duplicatesReceived() {
        return failure.get() == null && injectedDuplicates.entrySet().stream().allMatch(entry -> {
            AtomicLong count = received.get(entry.getKey());
            return count != null && count.get() >= expectedPublishes(entry.getKey(), entry.getValue().get());
        });
    }

    long injectedDuplicateCount() {
        return injectedDuplicates.values().stream().mapToLong(AtomicLong::get).sum();
    }

    void assertDuplicatesReceived() {
        if (failure.get() != null) {
            throw new AssertionError("Cannot verify replicated duplicate reception for " + topicName, failure.get());
        }
        List<String> missing = new ArrayList<>();
        injectedDuplicates.forEach((position, injected) -> {
            AtomicLong observed = received.get(position);
            long actual = observed == null ? 0 : observed.get();
            long expected = expectedPublishes(position, injected.get());
            if (actual < expected) {
                missing.add(position + ": expected at least " + expected + " publishes, received " + actual);
            }
        });
        if (!missing.isEmpty()) {
            missing.sort(String::compareTo);
            throw new AssertionError("Injected replication duplicates were not received for " + topicName + ": "
                    + missing);
        }
    }

    private long expectedPublishes(ConnectionPosition position, long injected) {
        AtomicLong sent = outbound.get(position);
        // Every normal retry on this socket also raises the requirement, so it cannot mask a lost duplicate.
        return Math.max(injected + 1, sent == null ? 0 : sent.get());
    }

    private static SourcePosition sourcePosition(ByteBuf headersAndPayload) {
        MessageMetadata metadata = Commands.parseMessageMetadata(headersAndPayload);
        for (var property : metadata.getPropertiesList()) {
            if (MSG_PROP_REPL_SOURCE_POSITION.equals(property.getKey())) {
                String[] components = property.getValue().split(":", -1);
                if (components.length == 2) {
                    long ledgerId = Long.parseLong(components[0]);
                    long entryId = Long.parseLong(components[1]);
                    if (ledgerId >= 0 && entryId >= 0) {
                        return new SourcePosition(ledgerId, entryId);
                    }
                }
                throw new IllegalStateException("Invalid replication source position: " + property.getValue());
            }
        }
        throw new IllegalStateException("Replicated publish is missing its source position");
    }

    private record SourcePosition(long ledgerId, long entryId) {
    }

    private record ConnectionPosition(SocketAddress clientAddress, SourcePosition position) {
    }
}
