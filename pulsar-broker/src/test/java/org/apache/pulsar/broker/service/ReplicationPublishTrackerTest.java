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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class ReplicationPublishTrackerTest {
    private static final SocketAddress ADDRESS = new InetSocketAddress("127.0.0.1", 12345);
    private static final String TOPIC = "persistent://public/default/replication-publish-tracker";

    @Test
    public void testMissingDuplicateCannotPass() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair pair = command("1:2");
        try {
            tracker.recordInjectedDuplicate(pair, ADDRESS);
            publish(tracker, pair, TOPIC);
            assertThat(tracker.injectedDuplicateCount()).isOne();
            assertThat(tracker.duplicatesReceived()).isFalse();
            assertThatThrownBy(tracker::assertDuplicatesReceived).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("expected at least 2 publishes, received 1");
            publish(tracker, pair, TOPIC);
            tracker.assertDuplicatesReceived();
        } finally {
            pair.release();
        }
    }

    @Test
    public void testAnotherSourcePositionCannotReplaceMissingDuplicate() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair expected = command("1:2");
        ByteBufPair other = command("1:3");
        try {
            tracker.recordInjectedDuplicate(expected, ADDRESS);
            publish(tracker, expected, TOPIC);
            publish(tracker, other, TOPIC);
            publish(tracker, other, TOPIC);
            assertThat(tracker.duplicatesReceived()).isFalse();
            assertThatThrownBy(tracker::assertDuplicatesReceived).isInstanceOf(AssertionError.class);
        } finally {
            expected.release();
            other.release();
        }
    }

    @Test
    public void testAnotherTopicCannotReplaceMissingDuplicate() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair pair = command("1:2");
        try {
            tracker.recordInjectedDuplicate(pair, ADDRESS);
            publish(tracker, pair, TOPIC);
            publish(tracker, pair, TOPIC + "-other");
            assertThat(tracker.duplicatesReceived()).isFalse();
        } finally {
            pair.release();
        }
    }

    @Test
    public void testEveryInjectedCopyRequiresItsOwnPublish() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair pair = command("1:2");
        try {
            tracker.recordInjectedDuplicate(pair, ADDRESS);
            tracker.recordInjectedDuplicate(pair, ADDRESS);
            publish(tracker, pair, TOPIC);
            publish(tracker, pair, TOPIC);
            assertThat(tracker.duplicatesReceived()).isFalse();
            publish(tracker, pair, TOPIC);
            tracker.assertDuplicatesReceived();
            assertThat(tracker.injectedDuplicateCount()).isEqualTo(2);
        } finally {
            pair.release();
        }
    }

    @Test
    public void testMissingSourcePositionFailsClosed() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair pair = command(null);
        try {
            assertThatThrownBy(() -> tracker.recordInjectedDuplicate(pair, ADDRESS))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("missing its source position");
            publish(tracker, pair, TOPIC);
            assertThat(tracker.duplicatesReceived()).isFalse();
            assertThatThrownBy(tracker::assertDuplicatesReceived).isInstanceOf(AssertionError.class)
                    .hasRootCauseMessage("Replicated publish is missing its source position");
        } finally {
            pair.release();
        }
    }

    @Test
    public void testObservationDoesNotConsumeOrRetainBuffers() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair pair = command("1:2");
        try {
            int index = pair.getFirst().readerIndex();
            int references = pair.refCnt();
            tracker.recordInjectedDuplicate(pair, ADDRESS);
            publish(tracker, pair, TOPIC);
            publish(tracker, pair, TOPIC);
            tracker.assertDuplicatesReceived();
            assertThat(pair.getFirst().readerIndex()).isEqualTo(index);
            assertThat(pair.refCnt()).isEqualTo(references);
        } finally {
            pair.release();
        }
    }

    @Test
    public void testExtraNormalRetryCannotReplaceDroppedInjectedDuplicate() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair pair = command("1:2");
        try {
            // The original and a normal retry reached the destination before the injected copy was lost.
            tracker.recordOutboundSend(pair, ADDRESS);
            publish(tracker, pair, TOPIC);
            tracker.recordOutboundSend(pair, ADDRESS);
            publish(tracker, pair, TOPIC);
            tracker.recordInjectedDuplicate(pair, ADDRESS);
            tracker.recordOutboundSend(pair, ADDRESS);
            assertThat(tracker.duplicatesReceived()).isFalse();
            assertThatThrownBy(tracker::assertDuplicatesReceived).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("expected at least 3 publishes, received 2");
        } finally {
            pair.release();
        }
    }

    @Test
    public void testAnotherSocketCannotReplaceMissingDuplicate() {
        ReplicationPublishTracker tracker = new ReplicationPublishTracker(TOPIC);
        ByteBufPair pair = command("1:2");
        try {
            tracker.recordInjectedDuplicate(pair, ADDRESS);
            publish(tracker, pair, TOPIC);
            publish(tracker, pair, TOPIC, new InetSocketAddress("127.0.0.1", 54321));
            assertThat(tracker.duplicatesReceived()).isFalse();
        } finally {
            pair.release();
        }
    }

    static ByteBufPair command(String sourcePosition) {
        MessageMetadata metadata = new MessageMetadata().setProducerName("original").setSequenceId(0)
                .setPublishTime(0);
        if (sourcePosition != null) {
            metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue(sourcePosition);
        }
        return Commands.newSend(1, 0, 1, Commands.ChecksumType.Crc32c, metadata,
                Unpooled.wrappedBuffer(new byte[] {1}));
    }

    static void publish(ReplicationPublishTracker tracker, ByteBufPair pair, String topicName) {
        publish(tracker, pair, topicName, ADDRESS);
    }

    static void publish(ReplicationPublishTracker tracker, ByteBufPair pair, String topicName,
                        SocketAddress clientAddress) {
        Topic topic = mock(Topic.class);
        when(topic.getName()).thenReturn(topicName);
        Producer producer = mock(Producer.class);
        when(producer.getTopic()).thenReturn(topic);
        TransportCnx connection = mock(TransportCnx.class);
        when(connection.clientAddress()).thenReturn(clientAddress);
        when(producer.getCnx()).thenReturn(connection);
        ByteBuf header = pair.getFirst().duplicate();
        header.readInt();
        header.skipBytes(header.readInt());
        int readerIndex = header.readerIndex();
        tracker.onMessagePublish(producer, header, null);
        assertThat(header.readerIndex()).isEqualTo(readerIndex);
    }
}
