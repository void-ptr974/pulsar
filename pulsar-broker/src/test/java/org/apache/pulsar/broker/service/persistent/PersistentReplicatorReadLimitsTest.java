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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.persistent.PersistentReplicator.calculateReadLimits;
import static org.testng.Assert.assertEquals;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.ReadLimits;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentReplicatorReadLimitsTest {

    @DataProvider(name = "readLimits")
    public Object[][] readLimits() {
        return new Object[][] {
                {0, true, 100, 1024L, -1L, -1L, 0, 0L, false},
                {1000, true, 100, 1024L, -1L, -1L, 100, 1024L, true},
                {20, true, 100, 1024L, -1L, -1L, 20, 1024L, true},
                {1000, true, 100, 1024L, 10L, -1L, 10, 1024L, true},
                {1000, true, 100, 1024L, -1L, 512L, 100, 512L, true},
                {1000, true, 100, 1024L, 0L, -1L, 0, 0L, false},
                {1000, true, 100, 1024L, -1L, 0L, 0, 0L, false},
                {1000, false, 100, 1024L, -1L, -1L, 1, 1024L, true},
                {1000, false, 100, 1024L, 10L, -1L, 1, 1024L, true},
                {1000, false, 100, 1024L, 0L, -1L, 0, 0L, false},
                {1000, true, 100, 0L, -1L, -1L, 0, 0L, false}
        };
    }

    @Test(dataProvider = "readLimits")
    public void testCalculateReadLimits(int permits, boolean writable, int readBatchSize, long readMaxSizeBytes,
                                        long dispatchRateLimitOnMsg, long dispatchRateLimitOnBytes,
                                        int expectedEntries, long expectedMaxBytes, boolean expectedReadable) {
        ReadLimits readLimits = calculateReadLimits(permits, writable, readBatchSize, readMaxSizeBytes,
                dispatchRateLimitOnMsg, dispatchRateLimitOnBytes);

        assertEquals(readLimits.entriesToRead(), expectedEntries);
        assertEquals(readLimits.maxBytesToRead(), expectedMaxBytes);
        assertEquals(readLimits.isReadable(), expectedReadable);
    }
}
