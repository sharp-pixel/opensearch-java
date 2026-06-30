/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.transport.httpclient5.internal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A shared, thread-safe budget that bounds the total number of bytes buffered in heap memory across all in-flight
 * response consumers created by the same factory.
 * <p>
 * The per-response buffer limit only bounds a <em>single</em> response. Under high concurrency (or a load/DDoS spike)
 * many responses can be buffered at once and collectively exhaust the heap, which - if it surfaces as an
 * {@link OutOfMemoryError} on an I/O reactor thread - can permanently shut the reactor down (see
 * <a href="https://github.com/opensearch-project/opensearch-java/issues/1969">opensearch-java#1969</a>). This budget
 * caps the aggregate so that, once exhausted, further buffering fails fast with a recoverable error instead of
 * allocating more memory.
 * <p>
 * A non-positive {@code maxBytes} disables the budget (unlimited), preserving legacy behavior.
 */
public final class ResponseMemoryBudget {

    /**
     * A budget that imposes no limit. {@link #tryReserve(long)} always succeeds and {@link #release(long)} is a no-op.
     */
    public static final ResponseMemoryBudget UNLIMITED = new ResponseMemoryBudget(0L);

    private final long maxBytes;
    private final AtomicLong usedBytes = new AtomicLong(0L);
    private final AtomicLong generation = new AtomicLong(0L);

    /**
     * Creates a budget.
     *
     * @param maxBytes the maximum number of bytes that may be reserved at once; a value {@code <= 0} means unlimited
     */
    public ResponseMemoryBudget(final long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /**
     * @return {@code true} if this budget enforces a finite limit
     */
    public boolean isLimited() {
        return maxBytes > 0L;
    }

    /**
     * @return the configured maximum number of bytes, or a value {@code <= 0} if unlimited
     */
    public long maxBytes() {
        return maxBytes;
    }

    /**
     * @return the number of bytes currently reserved
     */
    public long usedBytes() {
        return usedBytes.get();
    }

    /**
     * @return the current accounting generation
     */
    long generation() {
        return generation.get();
    }

    /**
     * Attempts to reserve {@code bytes} from the budget.
     *
     * @param bytes the number of bytes to reserve
     * @return {@code true} if the bytes were reserved (always {@code true} for an unlimited budget or a non-positive
     *         request); {@code false} if reserving would exceed the budget
     */
    public boolean tryReserve(final long bytes) {
        return tryReserve(generation(), bytes);
    }

    /**
     * Attempts to reserve {@code bytes} from the budget for a specific accounting generation.
     *
     * @param generation the generation captured when the consumer was created
     * @param bytes the number of bytes to reserve
     * @return {@code true} if the bytes were reserved; {@code false} if reserving would exceed the budget or the
     *         generation is stale
     */
    synchronized boolean tryReserve(final long generation, final long bytes) {
        if (bytes <= 0L || maxBytes <= 0L) {
            return true;
        }
        if (generation != this.generation.get()) {
            return false;
        }
        final long current = usedBytes.get();
        final long next = current + bytes;
        // Reject when the budget would be exceeded or the counter would overflow.
        if (next > maxBytes || next < 0L) {
            return false;
        }
        usedBytes.set(next);
        return true;
    }

    /**
     * Returns previously reserved bytes to the budget.
     *
     * @param bytes the number of bytes to release
     */
    public void release(final long bytes) {
        release(generation(), bytes);
    }

    /**
     * Returns previously reserved bytes to the budget for a specific accounting generation.
     *
     * @param generation the generation captured when the consumer was created
     * @param bytes the number of bytes to release
     */
    synchronized void release(final long generation, final long bytes) {
        if (bytes <= 0L || maxBytes <= 0L) {
            return;
        }
        if (generation != this.generation.get()) {
            return;
        }
        usedBytes.addAndGet(-bytes);
    }

    /**
     * Clears all outstanding reservations, returning the budget to its empty state.
     * <p>
     * This is used when the owning transport rebuilds its underlying client after an I/O reactor shutdown: consumers
     * that were buffering responses on the dead client are orphaned and never call {@link #release(long)}, so their
     * reservations would otherwise leak permanently and eventually reject all further requests. At rebuild time every
     * outstanding reservation belongs to the now-dead client, so resetting is safe.
     */
    public synchronized void reset() {
        usedBytes.set(0L);
        generation.incrementAndGet();
    }

    /**
     * Records bytes that have already been allocated, without enforcing the limit. Used to reconcile the budget with
     * the actual buffer capacity after an allocation that may have overshot the requested size (e.g.
     * {@link org.apache.hc.core5.util.ByteArrayBuffer} doubling its backing array). This may push {@link #usedBytes()}
     * above {@link #maxBytes()}; subsequent {@link #tryReserve(long)} calls then reject until enough is released, which
     * keeps the budget honest about real heap usage.
     *
     * @param bytes the number of already-committed bytes to account
     */
    public void reserveUnchecked(final long bytes) {
        reserveUnchecked(generation(), bytes);
    }

    /**
     * Records bytes that have already been allocated for a specific accounting generation.
     *
     * @param generation the generation captured when the consumer was created
     * @param bytes the number of already-committed bytes to account
     */
    synchronized void reserveUnchecked(final long generation, final long bytes) {
        if (bytes <= 0L || maxBytes <= 0L) {
            return;
        }
        if (generation != this.generation.get()) {
            return;
        }
        usedBytes.addAndGet(bytes);
    }
}
