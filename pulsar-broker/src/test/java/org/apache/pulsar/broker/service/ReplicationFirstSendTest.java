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
import io.netty.buffer.Unpooled;
import org.apache.pulsar.broker.service.OneWayReplicatorDeduplicationTestBase.FirstReplicationSend;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class ReplicationFirstSendTest {
    @Test
    public void testFirstSendFromSchemaRecoveryRemainsEligible() {
        FirstReplicationSend sends = new FirstReplicationSend();
        ByteBufPair message = command(1, 0, "1:2");
        try {
            assertThat(resendMessages(sends, message)).as("first wire send after schema buffering").isTrue();
            assertThat(resendMessages(sends, message)).as("actual replay of the same source entry").isFalse();
        } finally {
            message.release();
        }
    }

    private static boolean resendMessages(FirstReplicationSend sends, ByteBufPair message) {
        return sends.isFirstSend(message);
    }

    @Test
    public void testReplayAcrossProducerEpochsUsesSourcePosition() {
        FirstReplicationSend sends = new FirstReplicationSend();
        ByteBufPair original = command(1, 0, "1:2");
        ByteBufPair newProducerEpoch = command(9, 100, "1:2");
        ByteBufPair nextEntry = command(9, 101, "1:3");
        ByteBufPair nextLedger = command(9, 102, "2:2");
        try {
            assertThat(sends.isFirstSend(original)).isTrue();
            assertThat(sends.isFirstSend(newProducerEpoch)).isFalse();
            assertThat(sends.isFirstSend(nextEntry)).isTrue();
            assertThat(sends.isFirstSend(nextLedger)).isTrue();
        } finally {
            original.release();
            newProducerEpoch.release();
            nextEntry.release();
            nextLedger.release();
        }
    }

    @Test
    public void testDecodePreservesBothBufferIndexes() {
        FirstReplicationSend sends = new FirstReplicationSend();
        ByteBufPair message = command(1, 0, "1:2");
        try {
            int headerReaderIndex = message.getFirst().readerIndex();
            int headerWriterIndex = message.getFirst().writerIndex();
            int payloadReaderIndex = message.getSecond().readerIndex();
            int payloadWriterIndex = message.getSecond().writerIndex();
            assertThat(sends.isFirstSend(message)).isTrue();
            assertThat(sends.isFirstSend(message)).isFalse();
            assertThat(message.getFirst().readerIndex()).isEqualTo(headerReaderIndex);
            assertThat(message.getFirst().writerIndex()).isEqualTo(headerWriterIndex);
            assertThat(message.getSecond().readerIndex()).isEqualTo(payloadReaderIndex);
            assertThat(message.getSecond().writerIndex()).isEqualTo(payloadWriterIndex);
        } finally {
            message.release();
        }
    }

    @DataProvider
    public Object[][] invalidPositions() {
        return new Object[][] {{null}, {""}, {"1"}, {"1:2:3"}, {"-1:2"}, {"1:-2"}, {"a:2"}, {"1:"}};
    }

    @Test(dataProvider = "invalidPositions")
    public void testInvalidPositionCannotSilentlySkipInjection(String position) {
        FirstReplicationSend sends = new FirstReplicationSend();
        ByteBufPair invalid = command(1, 0, position);
        ByteBufPair valid = command(1, 0, "1:2");
        try {
            assertThatThrownBy(() -> sends.isFirstSend(invalid)).isInstanceOf(RuntimeException.class);
            assertThat(sends.isFirstSend(valid)).as("invalid input must not poison the seen set").isTrue();
        } finally {
            invalid.release();
            valid.release();
        }
    }

    @Test
    public void testConflictingSourcePositionsFailClosed() {
        FirstReplicationSend sends = new FirstReplicationSend();
        MessageMetadata metadata = metadata("1:2");
        metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("1:3");
        ByteBufPair message = Commands.newSend(1, 0, 1, Commands.ChecksumType.Crc32c, metadata,
                Unpooled.wrappedBuffer(new byte[] {1}));
        try {
            assertThatThrownBy(() -> sends.isFirstSend(message)).isInstanceOf(IllegalStateException.class)
                    .hasMessage("Conflicting replication source positions");
        } finally {
            message.release();
        }
    }

    private static ByteBufPair command(long producerId, long sequenceId, String position) {
        return Commands.newSend(producerId, sequenceId, 1, Commands.ChecksumType.Crc32c, metadata(position),
                Unpooled.wrappedBuffer(new byte[] {1}));
    }

    private static MessageMetadata metadata(String position) {
        MessageMetadata metadata = new MessageMetadata().setProducerName("original").setSequenceId(0)
                .setPublishTime(0);
        if (position != null) {
            metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue(position);
        }
        return metadata;
    }
}
