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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.pulsar.common.util.collections.IntIntPair;
import org.apache.pulsar.common.util.collections.Long2LongOpenHashMap;

/**
 * A thread-safe map to store pending acks in the consumer.
 *
 * The locking solution is used for the draining hashes solution
 * to ensure that there's a consistent view of the pending acks. This is needed in the DrainingHashesTracker
 * to ensure that the reference counts are consistent at all times.
 * Calling forEachAndClose will ensure that no more entries can be added,
 * therefore no other thread cannot send out entries while the forEachAndClose is being called.
 * remove is also locked to ensure that there aren't races in the removal of entries while forEachAndClose is
 * running.
 */
public class PendingAcksMap {
    static final int PENDING_ACK_NOT_FOUND = -1;
    private static final long STICKY_KEY_HASH_MASK = 0xFFFF_FFFFL;

    /**
     * Callback interface for handling the addition of pending acknowledgments.
     */
    public interface PendingAcksAddHandler {
        /**
         * Handle the addition of a pending acknowledgment.
         *
         * @param consumer      the consumer
         * @param ledgerId      the ledger ID
         * @param entryId       the entry ID
         * @param stickyKeyHash the sticky key hash
         * @return true if the addition is allowed, false otherwise
         */
        boolean handleAdding(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash);
    }

    /**
     * Callback interface for handling the removal of pending acknowledgments.
     */
    public interface PendingAcksRemoveHandler {
        /**
         * Handle the removal of a pending acknowledgment.
         *
         * @param consumer      the consumer
         * @param ledgerId      the ledger ID
         * @param entryId       the entry ID
         * @param stickyKeyHash the sticky key hash
         * @param closing       true if the pending ack is being removed because the map is being closed, false
         *                      otherwise
         */
        void handleRemoving(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash, boolean closing);
        /**
         * Start a batch of pending acknowledgment removals.
         */
        void startBatch();
        /**
         * End a batch of pending acknowledgment removals.
         */
        void endBatch();
    }

    /**
     * Callback interface for processing pending acknowledgments.
     */
    public interface PendingAcksConsumer {
        /**
         * Accept a pending acknowledgment.
         *
         * @param ledgerId          the ledger ID
         * @param entryId           the entry ID
         * @param remainingUnacked  the number of remaining unacked messages in this entry
         *                          (accounts for batch index level acknowledgments)
         * @param stickyKeyHash     the sticky key hash
         */
        void accept(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);
    }

    private final Consumer consumer;
    private final TreeMap<Long, Long2LongOpenHashMap> pendingAcks;
    private final Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier;
    private final Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier;
    private final Lock readLock;
    private final Lock writeLock;
    private boolean closed = false;

    PendingAcksMap(Consumer consumer, Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier,
                   Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier) {
        this.consumer = consumer;
        this.pendingAcks = new TreeMap<>();
        this.pendingAcksAddHandlerSupplier = pendingAcksAddHandlerSupplier;
        this.pendingAcksRemoveHandlerSupplier = pendingAcksRemoveHandlerSupplier;
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.writeLock = readWriteLock.writeLock();
        this.readLock = readWriteLock.readLock();
    }

    /**
     * Add a pending ack to the map if it's allowed to send a message with the given sticky key hash.
     * If this method returns false, it means that the pending ack was not added, and it's not allowed to send a
     * message. In that case, the caller should not send a message and skip the entry.
     * The sending could be disallowed if the sticky key hash is blocked in the Key_Shared subscription.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param remainingUnacked the number of remaining unacked messages in this entry
     *                         (for batch entries with some indexes already acked, this may be less than batchSize)
     * @param stickyKeyHash the sticky key hash
     * @return true if the pending ack was added, and it's allowed to send a message, false otherwise
     */
    public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
        try {
            writeLock.lock();
            // prevent adding sticky hash to pending acks if the PendingAcksMap has already been closed
            // and there's a race condition between closing the consumer and sending new messages
            if (closed) {
                return false;
            }
            // prevent adding sticky hash to pending acks if it's already in draining hashes
            // to avoid any race conditions that would break consistency
            PendingAcksAddHandler pendingAcksAddHandler = pendingAcksAddHandlerSupplier.get();
            if (pendingAcksAddHandler != null
                    && !pendingAcksAddHandler.handleAdding(consumer, ledgerId, entryId, stickyKeyHash)) {
                return false;
            }
            Long2LongOpenHashMap ledgerPendingAcks =
                    pendingAcks.computeIfAbsent(ledgerId, k -> new Long2LongOpenHashMap());
            ledgerPendingAcks.put(entryId, packPendingAckValue(remainingUnacked, stickyKeyHash));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Get the size of the pending acks map.
     *
     * @return the size of the pending acks map
     */
    public long size() {
        try {
            readLock.lock();
            return pendingAcks.values().stream().mapToInt(Long2LongOpenHashMap::size).sum();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Iterate over all the pending acks and process them using the given processor.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEach(PendingAcksConsumer processor) {
        try {
            readLock.lock();
            processPendingAcks(processor);
        } finally {
            readLock.unlock();
        }
    }

    // iterate all pending acks and process them
    private void processPendingAcks(PendingAcksConsumer processor) {
        // this code uses for loops intentionally, don't refactor to use forEach
        // iterate the outer map
        for (Map.Entry<Long, Long2LongOpenHashMap> entry : pendingAcks.entrySet()) {
            long ledgerId = entry.getKey();
            Long2LongOpenHashMap ledgerPendingAcks = entry.getValue();
            // iterate the inner map
            ledgerPendingAcks.forEach((entryId, packedValue) ->
                processor.accept(ledgerId, entryId, unpackRemainingUnacked(packedValue),
                        unpackStickyKeyHash(packedValue)));
        }
    }

    /**
     * Iterate over all the pending acks and close the map so that no more entries can be added.
     * All entries are removed.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEachAndClose(PendingAcksConsumer processor) {
        internalForEachAndClear(processor, true);
    }

    /**
     * Iterate over all the pending acks and clear the map.
     * Unlike {@link #forEachAndClose(PendingAcksConsumer)}, this method does not close the map,
     * so new entries can still be added after this method returns.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEachAndClear(PendingAcksConsumer processor) {
        internalForEachAndClear(processor, false);
    }

    private void internalForEachAndClear(PendingAcksConsumer processor, boolean close) {
        try {
            writeLock.lock();
            if (close) {
                closed = true;
            }
            PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
            if (pendingAcksRemoveHandler != null) {
                try {
                    pendingAcksRemoveHandler.startBatch();
                    processPendingAcks((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                        processor.accept(ledgerId, entryId, batchSize, stickyKeyHash);
                        pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
                    });
                } finally {
                    pendingAcksRemoveHandler.endBatch();
                }
            } else {
                processPendingAcks(processor);
            }
            pendingAcks.clear();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Check if the map contains a pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return true if the map contains the pending ack, false otherwise
     */
    public boolean contains(long ledgerId, long entryId) {
        try {
            readLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            return ledgerMap.containsKey(entryId);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the pending ack, or null if not found
     */
    public IntIntPair get(long ledgerId, long entryId) {
        try {
            readLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null || !ledgerMap.containsKey(entryId)) {
                return null;
            }
            return unpackPendingAckValue(ledgerMap.get(entryId));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the remaining unacked count for the given ledger ID and entry ID.
     *
     * @return the remaining unacked count, or {@link #PENDING_ACK_NOT_FOUND} if not found
     */
    int getRemainingUnacked(long ledgerId, long entryId) {
        try {
            readLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null || !ledgerMap.containsKey(entryId)) {
                return PENDING_ACK_NOT_FOUND;
            }
            return unpackRemainingUnacked(ledgerMap.get(entryId));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Remove the pending ack for the given ledger ID, entry ID, batch size, and sticky key hash.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param batchSize the batch size
     * @param stickyKeyHash the sticky key hash
     * @return true if the pending ack was removed, false otherwise
     */
    public boolean remove(long ledgerId, long entryId, int batchSize, int stickyKeyHash) {
        try {
            writeLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null || !ledgerMap.containsKey(entryId)) {
                return false;
            }
            long packedValue = ledgerMap.get(entryId);
            boolean removed = packedValue == packPendingAckValue(batchSize, stickyKeyHash);
            if (removed) {
                ledgerMap.remove(entryId);
                handleRemovePendingAck(ledgerId, entryId, stickyKeyHash);
            }
            if (removed && ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically update the remaining unacked count for a pending ack entry by subtracting the given delta.
     * Called from the ack handler after computing the number of batch indexes acknowledged in a partial ack.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param ackedDelta the number of batch indexes that were just acknowledged
     * @return true if the entry was found and updated, false otherwise
     */
    public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
        try {
            writeLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null || !ledgerMap.containsKey(entryId)) {
                return false;
            }
            long packedValue = ledgerMap.get(entryId);
            int newRemaining = unpackRemainingUnacked(packedValue) - ackedDelta;
            ledgerMap.put(entryId, packPendingAckValue(newRemaining, unpackStickyKeyHash(packedValue)));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Remove the pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return true if the pending ack was removed, false otherwise
     */
    public boolean remove(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null || !ledgerMap.containsKey(entryId)) {
                return false;
            }
            long removedEntry = ledgerMap.remove(entryId);
            handleRemovePendingAck(ledgerId, entryId, unpackStickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically remove and return the pending ack for the given ledger ID and entry ID.
     * Unlike {@link #remove(long, long)}, this method returns the removed entry so the caller
     * can access the batch size and sticky key hash without a separate get operation.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the removed entry as an IntIntPair (batchSize, stickyKeyHash), or null if not found
     */
    public IntIntPair removeAndGet(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null || !ledgerMap.containsKey(entryId)) {
                return null;
            }
            long removedEntry = ledgerMap.remove(entryId);
            handleRemovePendingAck(ledgerId, entryId, unpackStickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return unpackPendingAckValue(removedEntry);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically remove and return the remaining unacked count for the given ledger ID and entry ID.
     *
     * @return the remaining unacked count, or {@link #PENDING_ACK_NOT_FOUND} if not found
     */
    int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            Long2LongOpenHashMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null || !ledgerMap.containsKey(entryId)) {
                return PENDING_ACK_NOT_FOUND;
            }
            long removedEntry = ledgerMap.remove(entryId);
            handleRemovePendingAck(ledgerId, entryId, unpackStickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return unpackRemainingUnacked(removedEntry);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Remove all pending acks up to the given ledger ID and entry ID, invoking a callback for each removed entry.
     *
     * @param markDeleteLedgerId the ledger ID up to which to remove pending acks
     * @param markDeleteEntryId the entry ID up to which to remove pending acks
     * @param removedEntryCallback optional callback invoked for each removed entry (within the write lock),
     *                             receiving ledgerId, entryId, batchSize, and stickyKeyHash
     */
    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId,
                             PendingAcksConsumer removedEntryCallback) {
        internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, false, removedEntryCallback);
    }

    /**
     * Removes all pending acknowledgments up to the specified ledger ID and entry ID.
     *
     * ReadWriteLock doesn't support upgrading from read lock to write lock.
     * This method first checks if there's anything to remove using a read lock and if there is, exits
     * and retries with a write lock to make the removals.
     *
     * @param markDeleteLedgerId the ledger ID up to which to remove pending acks
     * @param markDeleteEntryId the entry ID up to which to remove pending acks
     * @param useWriteLock true if the method should use a write lock, false otherwise
     * @param removedEntryCallback optional callback invoked for each removed entry (within the write lock)
     */
    private void internalRemoveAllUpTo(long markDeleteLedgerId, long markDeleteEntryId, boolean useWriteLock,
                                      PendingAcksConsumer removedEntryCallback) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        // track if the write lock was acquired
        boolean acquiredWriteLock = false;
        RemovedEntryProcessor removedEntryProcessor =
                new RemovedEntryProcessor(pendingAcksRemoveHandler, removedEntryCallback);
        // track if the method should retry with a write lock
        boolean retryWithWriteLock = false;
        try {
            if (useWriteLock) {
                writeLock.lock();
                acquiredWriteLock = true;
            } else {
                readLock.lock();
            }
            Iterator<Map.Entry<Long, Long2LongOpenHashMap>> ledgerMapIterator =
                    pendingAcks.headMap(markDeleteLedgerId, true).entrySet().iterator();
            while (ledgerMapIterator.hasNext()) {
                Map.Entry<Long, Long2LongOpenHashMap> entry = ledgerMapIterator.next();
                long ledgerId = entry.getKey();
                Long2LongOpenHashMap ledgerMap = entry.getValue();
                if (!acquiredWriteLock) {
                    if (hasEntryToRemove(ledgerId, ledgerMap, markDeleteLedgerId, markDeleteEntryId)) {
                        retryWithWriteLock = true;
                        return;
                    }
                    continue;
                }
                if (ledgerId < markDeleteLedgerId) {
                    ledgerMap.forEach((entryId, packedValue) ->
                            removedEntryProcessor.accept(ledgerId, entryId, packedValue));
                    ledgerMapIterator.remove();
                } else {
                    int removed = ledgerMap.removeIf((entryId, packedValue) -> {
                        if (entryId > markDeleteEntryId) {
                            return false;
                        }
                        removedEntryProcessor.accept(ledgerId, entryId, packedValue);
                        return true;
                    });
                    if (removed > 0 && ledgerMap.isEmpty()) {
                        ledgerMapIterator.remove();
                    }
                }
            }
        } finally {
            removedEntryProcessor.endBatchIfNeeded();
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

    private static boolean hasEntryToRemove(long ledgerId, Long2LongOpenHashMap ledgerMap,
                                            long markDeleteLedgerId, long markDeleteEntryId) {
        if (ledgerId < markDeleteLedgerId) {
            return !ledgerMap.isEmpty();
        }
        EntryUpToFinder finder = new EntryUpToFinder(markDeleteEntryId);
        ledgerMap.forEach(finder::accept);
        return finder.found;
    }

    private static long packPendingAckValue(int remainingUnacked, int stickyKeyHash) {
        return ((long) remainingUnacked << Integer.SIZE) | (stickyKeyHash & STICKY_KEY_HASH_MASK);
    }

    private static IntIntPair unpackPendingAckValue(long packedValue) {
        return IntIntPair.of(unpackRemainingUnacked(packedValue), unpackStickyKeyHash(packedValue));
    }

    private static int unpackRemainingUnacked(long packedValue) {
        return (int) (packedValue >> Integer.SIZE);
    }

    // Casting back to int restores the original signed sticky key hash.
    private static int unpackStickyKeyHash(long packedValue) {
        return (int) packedValue;
    }

    private void handleRemovePendingAck(long ledgerId, long entryId, int stickyKeyHash) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        if (pendingAcksRemoveHandler != null) {
            pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
        }
    }

    private static final class EntryUpToFinder {
        private final long markDeleteEntryId;
        private boolean found;

        private EntryUpToFinder(long markDeleteEntryId) {
            this.markDeleteEntryId = markDeleteEntryId;
        }

        private void accept(long entryId, long ignoredValue) {
            if (entryId <= markDeleteEntryId) {
                found = true;
            }
        }
    }

    private final class RemovedEntryProcessor {
        private final PendingAcksRemoveHandler pendingAcksRemoveHandler;
        private final PendingAcksConsumer removedEntryCallback;
        private boolean batchStarted;

        private RemovedEntryProcessor(PendingAcksRemoveHandler pendingAcksRemoveHandler,
                                      PendingAcksConsumer removedEntryCallback) {
            this.pendingAcksRemoveHandler = pendingAcksRemoveHandler;
            this.removedEntryCallback = removedEntryCallback;
        }

        private void accept(long ledgerId, long entryId, long packedValue) {
            int stickyKeyHash = unpackStickyKeyHash(packedValue);
            if (pendingAcksRemoveHandler != null) {
                if (!batchStarted) {
                    pendingAcksRemoveHandler.startBatch();
                    batchStarted = true;
                }
                pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
            }
            if (removedEntryCallback != null) {
                removedEntryCallback.accept(ledgerId, entryId, unpackRemainingUnacked(packedValue), stickyKeyHash);
            }
        }

        private void endBatchIfNeeded() {
            if (batchStarted) {
                pendingAcksRemoveHandler.endBatch();
            }
        }
    }
}
