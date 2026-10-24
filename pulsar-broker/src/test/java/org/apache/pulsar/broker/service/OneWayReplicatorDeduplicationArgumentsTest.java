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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class OneWayReplicatorDeduplicationArgumentsTest {

    @Test
    public void testOnlyMarkersForwardedToTheTargetContributeToTheUnacknowledgedBacklog() {
        MessageMetadata request = new MessageMetadata()
                .setMarkerType(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST.getValue());
        assertThat(OneWayReplicatorDeduplicationTest.isOutboundReplicationMarker(request, "r2")).isTrue();
        request.setReplicatedFrom("r2");
        assertThat(OneWayReplicatorDeduplicationTest.isOutboundReplicationMarker(request, "r2")).isFalse();

        MessageMetadata localSnapshot = new MessageMetadata()
                .setMarkerType(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT.getValue());
        localSnapshot.addReplicateTo("r1");
        assertThat(OneWayReplicatorDeduplicationTest.isOutboundReplicationMarker(localSnapshot, "r2")).isFalse();
        localSnapshot.addReplicateTo("r2");
        assertThat(OneWayReplicatorDeduplicationTest.isOutboundReplicationMarker(localSnapshot, "r2")).isTrue();
        assertThat(OneWayReplicatorDeduplicationTest.isOutboundReplicationMarker(new MessageMetadata(), "r2"))
                .isFalse();
    }

    @Test
    public void testAllArgumentsRunInExactlyOneGroup() {
        Object[][] singleSchema = new OneWayReplicatorDeduplicationTest().deduplicationArgs();
        Object[][] multiSchema = new OneWayReplicatorDeduplicationMultiSchemaTest().deduplicationArgs();
        Object[][] compatibility = new OneWayReplicatorDeduplicationCompatibilityTest().deduplicationArgs();

        assertThat(singleSchema.length).isEqualTo(6);
        assertThat(multiSchema.length).isEqualTo(6);
        assertThat(compatibility.length).isEqualTo(3);
        List<List<Object>> partitioned = Stream.of(singleSchema, multiSchema, compatibility)
                .flatMap(Arrays::stream).map(Arrays::asList).toList();
        assertThat(partitioned).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(
                Arrays.stream(OneWayReplicatorDeduplicationTestBase.allDeduplicationArgs())
                        .map(Arrays::asList).toList());
    }

    @Test
    public void testCleanupPreservesOriginalFailureAndRunsEveryAction() throws Exception {
        AssertionError original = new AssertionError("test failed");
        Exception cleanupFailure = new Exception("cleanup failed");
        AtomicInteger completed = new AtomicInteger();
        List<AutoCloseable> actions = new ArrayList<>();
        actions.add(() -> {
            throw cleanupFailure;
        });
        actions.add(completed::incrementAndGet);

        OneWayReplicatorDeduplicationTestBase.closeAfterTest(actions, original);

        assertThat(completed).hasValue(1);
        assertThat(original.getSuppressed()).containsExactly(cleanupFailure);
    }

    @Test
    public void testCleanupFailureDoesNotSkipRemainingActions() {
        Exception cleanupFailure = new Exception("cleanup failed");
        AtomicInteger completed = new AtomicInteger();
        List<AutoCloseable> actions = new ArrayList<>();
        actions.add(() -> {
            throw cleanupFailure;
        });
        actions.add(completed::incrementAndGet);

        assertThatThrownBy(() -> OneWayReplicatorDeduplicationTestBase.closeAfterTest(actions, null))
                .isSameAs(cleanupFailure);
        assertThat(completed).hasValue(1);
    }

    @Test
    public void testCleanupDoesNotSuppressTheOriginalFailureOntoItself() throws Exception {
        AssertionError original = new AssertionError("test and cleanup failed");
        AtomicInteger completed = new AtomicInteger();
        List<AutoCloseable> actions = new ArrayList<>();
        actions.add(() -> {
            throw original;
        });
        actions.add(completed::incrementAndGet);

        OneWayReplicatorDeduplicationTestBase.closeAfterTest(actions, original);

        assertThat(completed).hasValue(1);
        assertThat(original.getSuppressed()).isEmpty();
    }
}
