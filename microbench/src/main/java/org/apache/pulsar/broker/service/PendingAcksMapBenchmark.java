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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.pulsar.common.util.collections.IntIntPair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares the current {@link PendingAcksMap} implementation with the previous nested boxed
 * {@code TreeMap<Long, TreeMap<Long, IntIntPair>>} shape.
 *
 * <pre>
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar \
 *   "org.apache.pulsar.broker.service.PendingAcksMapBenchmark.*" \
 *   -p implementation=oldProduction,production -p dataset=64kEntries1kLedgers,1mEntries16kLedgers \
 *   -wi 2 -i 3 -w 1s -r 1s -f 1 -prof gc
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class PendingAcksMapBenchmark {
    private static final int PENDING_ACK_NOT_FOUND = -1;

    private interface PendingAcks {
        boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);

        boolean contains(long ledgerId, long entryId);

        int getRemainingUnacked(long ledgerId, long entryId);

        boolean remove(long ledgerId, long entryId);

        int removeAndGetRemainingUnacked(long ledgerId, long entryId);

        boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta);

        void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId,
                           PendingAcksMap.PendingAcksConsumer removedEntryCallback);

        void forEach(PendingAcksMap.PendingAcksConsumer processor);
    }

    private abstract static class BaseState {
        String implementation;
        String dataset;
        PendingAcks pendingAcks;
        long[] ledgerIds;
        long[] entryIds;
        int entries;
        int ledgers;
        int cursor;

        void setupDataset() {
            switch (dataset) {
                case "64kEntries1kLedgers":
                    entries = 65_536;
                    ledgers = 1_024;
                    break;
                case "1mEntries16kLedgers":
                    entries = 1_048_576;
                    ledgers = 16_384;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown dataset " + dataset);
            }
            ledgerIds = new long[entries];
            entryIds = new long[entries];
            for (int i = 0; i < entries; i++) {
                ledgerIds[i] = i % ledgers;
                entryIds[i] = i / ledgers;
            }
        }

        void resetMap() {
            pendingAcks = newMap();
            for (int i = 0; i < entries; i++) {
                pendingAcks.addPendingAckIfAllowed(ledgerIds[i], entryIds[i], remainingUnacked(i),
                        stickyKeyHash(i));
            }
            cursor = 0;
        }

        int nextIndex() {
            int index = cursor;
            cursor++;
            if (cursor == entries) {
                cursor = 0;
            }
            return index;
        }

        private PendingAcks newMap() {
            switch (implementation) {
                case "oldProduction":
                    return new OldPendingAcksMap();
                case "production":
                    return new ProductionPendingAcksMap();
                default:
                    throw new IllegalArgumentException("Unknown implementation " + implementation);
            }
        }
    }

    @State(Scope.Thread)
    public static class OperationState extends BaseState {
        @Param({"oldProduction", "production"})
        public String implementationParam;

        @Param({"64kEntries1kLedgers", "1mEntries16kLedgers"})
        public String datasetParam;

        @Setup(Level.Trial)
        public void setup() {
            implementation = implementationParam;
            dataset = datasetParam;
            setupDataset();
            resetMap();
        }
    }

    @State(Scope.Thread)
    public static class RangeRemoveState extends BaseState {
        @Param({"oldProduction", "production"})
        public String implementationParam;

        @Param({"64kEntries1kLedgers", "1mEntries16kLedgers"})
        public String datasetParam;

        long markDeleteLedgerId;
        long markDeleteEntryId;

        @Setup(Level.Trial)
        public void setupTrial() {
            implementation = implementationParam;
            dataset = datasetParam;
            setupDataset();
            markDeleteLedgerId = ledgers / 2L;
            markDeleteEntryId = entries / ledgers - 1L;
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            resetMap();
        }
    }

    @Benchmark
    public void getRemainingUnackedHit(OperationState state, Blackhole blackhole) {
        int index = state.nextIndex();
        blackhole.consume(state.pendingAcks.getRemainingUnacked(state.ledgerIds[index], state.entryIds[index]));
    }

    @Benchmark
    public void containsHit(OperationState state, Blackhole blackhole) {
        int index = state.nextIndex();
        blackhole.consume(state.pendingAcks.contains(state.ledgerIds[index], state.entryIds[index]));
    }

    @Benchmark
    public void addOrReplace(OperationState state, Blackhole blackhole) {
        int index = state.nextIndex();
        blackhole.consume(state.pendingAcks.addPendingAckIfAllowed(state.ledgerIds[index], state.entryIds[index],
                remainingUnacked(index), stickyKeyHash(index)));
    }

    @Benchmark
    public void updateRemainingUnacked(OperationState state, Blackhole blackhole) {
        int index = state.nextIndex();
        blackhole.consume(state.pendingAcks.updateRemainingUnacked(state.ledgerIds[index], state.entryIds[index], 0));
    }

    @Benchmark
    public void removeAndAddRemaining(OperationState state, Blackhole blackhole) {
        int index = state.nextIndex();
        long ledgerId = state.ledgerIds[index];
        long entryId = state.entryIds[index];
        int removed = state.pendingAcks.removeAndGetRemainingUnacked(ledgerId, entryId);
        if (removed == PENDING_ACK_NOT_FOUND) {
            throw new IllegalStateException("Missing pending ack");
        }
        blackhole.consume(removed);
        blackhole.consume(state.pendingAcks.addPendingAckIfAllowed(ledgerId, entryId, removed, stickyKeyHash(index)));
    }

    @Benchmark
    public void forEachAll(OperationState state, Blackhole blackhole) {
        state.pendingAcks.forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> {
            blackhole.consume(ledgerId);
            blackhole.consume(entryId);
            blackhole.consume(batchSize);
            blackhole.consume(stickyKeyHash);
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void removeAllUpTo(RangeRemoveState state, Blackhole blackhole) {
        state.pendingAcks.removeAllUpTo(state.markDeleteLedgerId, state.markDeleteEntryId,
                (ledgerId, entryId, batchSize, stickyKeyHash) -> {
                    blackhole.consume(ledgerId);
                    blackhole.consume(entryId);
                    blackhole.consume(batchSize);
                    blackhole.consume(stickyKeyHash);
                });
    }

    private static int remainingUnacked(int index) {
        return (index & 1023) + 1;
    }

    private static int stickyKeyHash(int index) {
        return (int) (0x9e3779b9L * index);
    }

    private static class ProductionPendingAcksMap implements PendingAcks {
        private final PendingAcksMap pendingAcksMap = new PendingAcksMap(null, () -> null, () -> null);

        @Override
        public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            return pendingAcksMap.addPendingAckIfAllowed(ledgerId, entryId, remainingUnacked, stickyKeyHash);
        }

        @Override
        public boolean contains(long ledgerId, long entryId) {
            return pendingAcksMap.contains(ledgerId, entryId);
        }

        @Override
        public int getRemainingUnacked(long ledgerId, long entryId) {
            return pendingAcksMap.getRemainingUnacked(ledgerId, entryId);
        }

        @Override
        public boolean remove(long ledgerId, long entryId) {
            return pendingAcksMap.remove(ledgerId, entryId);
        }

        @Override
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            return pendingAcksMap.removeAndGetRemainingUnacked(ledgerId, entryId);
        }

        @Override
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            return pendingAcksMap.updateRemainingUnacked(ledgerId, entryId, ackedDelta);
        }

        @Override
        public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId,
                                  PendingAcksMap.PendingAcksConsumer removedEntryCallback) {
            pendingAcksMap.removeAllUpTo(markDeleteLedgerId, markDeleteEntryId, removedEntryCallback);
        }

        @Override
        public void forEach(PendingAcksMap.PendingAcksConsumer processor) {
            pendingAcksMap.forEach(processor);
        }
    }

    private static class OldPendingAcksMap implements PendingAcks {
        private final TreeMap<Long, TreeMap<Long, IntIntPair>> pendingAcks = new TreeMap<>();
        private final Supplier<PendingAcksMap.PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier = () -> null;
        private final Lock readLock;
        private final Lock writeLock;

        OldPendingAcksMap() {
            ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
            this.writeLock = readWriteLock.writeLock();
            this.readLock = readWriteLock.readLock();
        }

        @Override
        public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            try {
                writeLock.lock();
                TreeMap<Long, IntIntPair> ledgerPendingAcks =
                        pendingAcks.computeIfAbsent(ledgerId, ignored -> new TreeMap<>());
                ledgerPendingAcks.put(entryId, IntIntPair.of(remainingUnacked, stickyKeyHash));
                return true;
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public boolean contains(long ledgerId, long entryId) {
            try {
                readLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                return ledgerMap != null && ledgerMap.containsKey(entryId);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public int getRemainingUnacked(long ledgerId, long entryId) {
            try {
                readLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                if (ledgerMap == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                IntIntPair pendingAck = ledgerMap.get(entryId);
                if (pendingAck == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                return pendingAck.leftInt();
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public boolean remove(long ledgerId, long entryId) {
            try {
                writeLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                if (ledgerMap == null) {
                    return false;
                }
                IntIntPair removedEntry = ledgerMap.remove(entryId);
                boolean removed = removedEntry != null;
                if (removed && ledgerMap.isEmpty()) {
                    pendingAcks.remove(ledgerId);
                }
                return removed;
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            try {
                writeLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                if (ledgerMap == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                IntIntPair removedEntry = ledgerMap.remove(entryId);
                if (removedEntry == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                if (ledgerMap.isEmpty()) {
                    pendingAcks.remove(ledgerId);
                }
                return removedEntry.leftInt();
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            try {
                writeLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                if (ledgerMap == null) {
                    return false;
                }
                IntIntPair current = ledgerMap.get(entryId);
                if (current == null) {
                    return false;
                }
                int newRemaining = current.leftInt() - ackedDelta;
                ledgerMap.put(entryId, IntIntPair.of(newRemaining, current.rightInt()));
                return true;
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId,
                                  PendingAcksMap.PendingAcksConsumer removedEntryCallback) {
            internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, false, removedEntryCallback);
        }

        @Override
        public void forEach(PendingAcksMap.PendingAcksConsumer processor) {
            try {
                readLock.lock();
                processPendingAcks(processor);
            } finally {
                readLock.unlock();
            }
        }

        private void processPendingAcks(PendingAcksMap.PendingAcksConsumer processor) {
            for (Map.Entry<Long, TreeMap<Long, IntIntPair>> entry : pendingAcks.entrySet()) {
                long ledgerId = entry.getKey();
                TreeMap<Long, IntIntPair> ledgerPendingAcks = entry.getValue();
                for (Map.Entry<Long, IntIntPair> e : ledgerPendingAcks.entrySet()) {
                    long entryId = e.getKey();
                    IntIntPair batchSizeAndStickyKeyHash = e.getValue();
                    processor.accept(ledgerId, entryId, batchSizeAndStickyKeyHash.leftInt(),
                            batchSizeAndStickyKeyHash.rightInt());
                }
            }
        }

        private void internalRemoveAllUpTo(long markDeleteLedgerId, long markDeleteEntryId, boolean useWriteLock,
                                           PendingAcksMap.PendingAcksConsumer removedEntryCallback) {
            PendingAcksMap.PendingAcksRemoveHandler pendingAcksRemoveHandler =
                    pendingAcksRemoveHandlerSupplier.get();
            boolean acquiredWriteLock = false;
            boolean batchStarted = false;
            boolean retryWithWriteLock = false;
            try {
                if (useWriteLock) {
                    writeLock.lock();
                    acquiredWriteLock = true;
                } else {
                    readLock.lock();
                }
                Iterator<Map.Entry<Long, TreeMap<Long, IntIntPair>>> ledgerMapIterator =
                        pendingAcks.headMap(markDeleteLedgerId + 1).entrySet().iterator();
                while (ledgerMapIterator.hasNext()) {
                    Map.Entry<Long, TreeMap<Long, IntIntPair>> entry = ledgerMapIterator.next();
                    long ledgerId = entry.getKey();
                    TreeMap<Long, IntIntPair> ledgerMap = entry.getValue();
                    TreeMap<Long, IntIntPair> ledgerMapHead;
                    if (ledgerId == markDeleteLedgerId) {
                        ledgerMapHead = new TreeMap<>(ledgerMap.headMap(markDeleteEntryId + 1));
                    } else {
                        ledgerMapHead = ledgerMap;
                    }
                    Iterator<Map.Entry<Long, IntIntPair>> entryMapIterator = ledgerMapHead.entrySet().iterator();
                    while (entryMapIterator.hasNext()) {
                        Map.Entry<Long, IntIntPair> intIntPairEntry = entryMapIterator.next();
                        long entryId = intIntPairEntry.getKey();
                        if (!acquiredWriteLock) {
                            retryWithWriteLock = true;
                            return;
                        }
                        IntIntPair value = intIntPairEntry.getValue();
                        int batchSize = value.leftInt();
                        int stickyKeyHash = value.rightInt();
                        if (pendingAcksRemoveHandler != null) {
                            if (!batchStarted) {
                                pendingAcksRemoveHandler.startBatch();
                                batchStarted = true;
                            }
                            pendingAcksRemoveHandler.handleRemoving(null, ledgerId, entryId, stickyKeyHash, false);
                        }
                        if (removedEntryCallback != null) {
                            removedEntryCallback.accept(ledgerId, entryId, batchSize, stickyKeyHash);
                        }
                        entryMapIterator.remove();
                        if (ledgerId == markDeleteLedgerId) {
                            ledgerMap.remove(entryId);
                        }
                    }
                    if (ledgerMap.isEmpty()) {
                        if (!acquiredWriteLock) {
                            retryWithWriteLock = true;
                            return;
                        }
                        ledgerMapIterator.remove();
                    }
                }
            } finally {
                if (batchStarted) {
                    pendingAcksRemoveHandler.endBatch();
                }
                if (acquiredWriteLock) {
                    writeLock.unlock();
                } else {
                    readLock.unlock();
                    if (retryWithWriteLock) {
                        internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, true, removedEntryCallback);
                    }
                }
            }
        }
    }
}
