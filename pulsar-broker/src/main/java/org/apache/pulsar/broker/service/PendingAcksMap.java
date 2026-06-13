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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongSortedMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.pulsar.common.util.collections.IntIntPair;

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
    private static final long PENDING_ACK_NOT_FOUND = Long.MIN_VALUE;

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
    private final Long2ObjectRBTreeMap<Long2LongRBTreeMap> pendingAcks;
    private final Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier;
    private final Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier;
    private final Lock readLock;
    private final Lock writeLock;
    private boolean closed = false;

    PendingAcksMap(Consumer consumer, Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier,
                   Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier) {
        this.consumer = consumer;
        this.pendingAcks = new Long2ObjectRBTreeMap<>();
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
            Long2LongRBTreeMap ledgerPendingAcks = pendingAcks.get(ledgerId);
            if (ledgerPendingAcks == null) {
                ledgerPendingAcks = newLedgerPendingAcks();
                pendingAcks.put(ledgerId, ledgerPendingAcks);
            }
            ledgerPendingAcks.put(entryId, pack(remainingUnacked, stickyKeyHash));
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
            return pendingAcks.values().stream().mapToInt(Long2LongRBTreeMap::size).sum();
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
        for (Long2ObjectMap.Entry<Long2LongRBTreeMap> entry : pendingAcks.long2ObjectEntrySet()) {
            long ledgerId = entry.getLongKey();
            Long2LongRBTreeMap ledgerPendingAcks = entry.getValue();
            // iterate the inner map
            for (Long2LongMap.Entry e : ledgerPendingAcks.long2LongEntrySet()) {
                long entryId = e.getLongKey();
                long batchSizeAndStickyKeyHash = e.getLongValue();
                processor.accept(ledgerId, entryId, remainingUnacked(batchSizeAndStickyKeyHash),
                        stickyKeyHash(batchSizeAndStickyKeyHash));
            }
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
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
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
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return null;
            }
            long pendingAck = ledgerMap.get(entryId);
            if (isMissingEntry(ledgerMap, entryId, pendingAck)) {
                return null;
            }
            return unpack(pendingAck);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the remaining unacked count for the given ledger ID and entry ID without allocating an {@link IntIntPair}.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the remaining unacked count, or {@code -1} if not found
     */
    public int getRemainingUnacked(long ledgerId, long entryId) {
        try {
            readLock.lock();
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return -1;
            }
            long pendingAck = ledgerMap.get(entryId);
            if (isMissingEntry(ledgerMap, entryId, pendingAck)) {
                return -1;
            }
            return remainingUnacked(pendingAck);
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
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long current = ledgerMap.get(entryId);
            if (isMissingEntry(ledgerMap, entryId, current)) {
                return false;
            }
            long expected = pack(batchSize, stickyKeyHash);
            if (current != expected) {
                return false;
            }
            ledgerMap.remove(entryId);
            handleRemovePendingAck(ledgerId, entryId, stickyKeyHash);
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return true;
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
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long current = ledgerMap.get(entryId);
            if (isMissingEntry(ledgerMap, entryId, current)) {
                return false;
            }
            int newRemaining = remainingUnacked(current) - ackedDelta;
            ledgerMap.put(entryId, pack(newRemaining, stickyKeyHash(current)));
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
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long removedEntry = ledgerMap.get(entryId);
            if (isMissingEntry(ledgerMap, entryId, removedEntry)) {
                return false;
            }
            ledgerMap.remove(entryId);
            handleRemovePendingAck(ledgerId, entryId, stickyKeyHash(removedEntry));
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
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return null;
            }
            long removedEntry = ledgerMap.get(entryId);
            if (isMissingEntry(ledgerMap, entryId, removedEntry)) {
                return null;
            }
            ledgerMap.remove(entryId);
            handleRemovePendingAck(ledgerId, entryId, stickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return unpack(removedEntry);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically remove and return the remaining unacked count for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the remaining unacked count, or {@code -1} if not found
     */
    public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            Long2LongRBTreeMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return -1;
            }
            long removedEntry = ledgerMap.get(entryId);
            if (isMissingEntry(ledgerMap, entryId, removedEntry)) {
                return -1;
            }
            ledgerMap.remove(entryId);
            handleRemovePendingAck(ledgerId, entryId, stickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return remainingUnacked(removedEntry);
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
        // track if a batch was started
        boolean batchStarted = false;
        // track if the method should retry with a write lock
        boolean retryWithWriteLock = false;
        try {
            if (useWriteLock) {
                writeLock.lock();
                acquiredWriteLock = true;
            } else {
                readLock.lock();
            }
            Iterator<Long2ObjectMap.Entry<Long2LongRBTreeMap>> ledgerMapIterator =
                    pendingAcks.headMap(markDeleteLedgerId + 1).long2ObjectEntrySet().iterator();
            while (ledgerMapIterator.hasNext()) {
                Long2ObjectMap.Entry<Long2LongRBTreeMap> entry = ledgerMapIterator.next();
                long ledgerId = entry.getLongKey();
                Long2LongRBTreeMap ledgerMap = entry.getValue();
                boolean removeLedger = ledgerId < markDeleteLedgerId;
                Long2LongSortedMap ledgerMapHead;
                if (removeLedger) {
                    ledgerMapHead = ledgerMap;
                } else {
                    ledgerMapHead = ledgerMap.headMap(markDeleteEntryId + 1);
                }
                Iterator<Long2LongMap.Entry> entryMapIterator = ledgerMapHead.long2LongEntrySet().iterator();
                while (entryMapIterator.hasNext()) {
                    Long2LongMap.Entry intIntPairEntry = entryMapIterator.next();
                    long entryId = intIntPairEntry.getLongKey();
                    if (!acquiredWriteLock) {
                        retryWithWriteLock = true;
                        return;
                    }
                    long value = intIntPairEntry.getLongValue();
                    int batchSize = remainingUnacked(value);
                    int stickyKeyHash = stickyKeyHash(value);
                    if (pendingAcksRemoveHandler != null) {
                        if (!batchStarted) {
                            pendingAcksRemoveHandler.startBatch();
                            batchStarted = true;
                        }
                        pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
                    }
                    if (removedEntryCallback != null) {
                        removedEntryCallback.accept(ledgerId, entryId, batchSize, stickyKeyHash);
                    }
                    if (!removeLedger) {
                        entryMapIterator.remove();
                    }
                }
                if (removeLedger || ledgerMap.isEmpty()) {
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

    private void handleRemovePendingAck(long ledgerId, long entryId, int stickyKeyHash) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        if (pendingAcksRemoveHandler != null) {
            pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
        }
    }

    private static long pack(int remainingUnacked, int stickyKeyHash) {
        return ((long) remainingUnacked << Integer.SIZE) | (stickyKeyHash & 0xffffffffL);
    }

    private static Long2LongRBTreeMap newLedgerPendingAcks() {
        Long2LongRBTreeMap ledgerPendingAcks = new Long2LongRBTreeMap();
        ledgerPendingAcks.defaultReturnValue(PENDING_ACK_NOT_FOUND);
        return ledgerPendingAcks;
    }

    private static boolean isMissingEntry(Long2LongRBTreeMap ledgerMap, long entryId, long pendingAck) {
        return pendingAck == PENDING_ACK_NOT_FOUND && !ledgerMap.containsKey(entryId);
    }

    private static IntIntPair unpack(long packed) {
        return IntIntPair.of(remainingUnacked(packed), stickyKeyHash(packed));
    }

    private static int remainingUnacked(long packed) {
        return (int) (packed >> Integer.SIZE);
    }

    private static int stickyKeyHash(long packed) {
        return (int) packed;
    }
}
