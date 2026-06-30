/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.transport.httpclient5.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.core5.http.ContentTooLongException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.nio.AsyncEntityConsumer;
import org.apache.hc.core5.http.nio.entity.AbstractBinAsyncEntityConsumer;
import org.apache.hc.core5.util.ByteArrayBuffer;
import org.opensearch.client.transport.httpclient5.ResponseBufferBudgetExceededException;

/**
 * Default implementation of {@link AsyncEntityConsumer}. Buffers the whole
 * response content in heap memory, meaning that the size of the buffer is equal to the content-length of the response.
 * Limits the size of responses that can be read based on a configurable argument. Throws an exception in case the entity is longer
 * than the configured buffer limit.
 */
public class HeapBufferedAsyncEntityConsumer extends AbstractBinAsyncEntityConsumer<byte[]> {

    /**
     * Upper bound for the flow-control window advertised to the producer per {@code updateCapacity} cycle. Bounding
     * this (instead of advertising {@link Integer#MAX_VALUE}) applies backpressure so a single response cannot pull an
     * unbounded amount of data into transport buffers at once. The window is replenished each cycle, so throughput is
     * preserved.
     */
    static final int MAX_CAPACITY_INCREMENT = 8 * 1024 * 1024 /* 8Mb */;

    private final int bufferLimitBytes;
    private final ResponseMemoryBudget memoryBudget;
    private final long memoryBudgetGeneration;
    private final AtomicLong reservedBytes = new AtomicLong(0L);
    private AtomicReference<ByteArrayBuffer> bufferRef = new AtomicReference<>();

    /**
     * Creates a new instance of this consumer with the provided buffer limit and no shared memory budget.
     *
     * @param bufferLimit the buffer limit. Must be greater than 0.
     * @throws IllegalArgumentException if {@code bufferLimit} is less than or equal to 0.
     */
    public HeapBufferedAsyncEntityConsumer(int bufferLimit) {
        this(bufferLimit, ResponseMemoryBudget.UNLIMITED);
    }

    /**
     * Creates a new instance of this consumer with the provided per-response buffer limit and a shared memory budget.
     *
     * @param bufferLimit the per-response buffer limit. Must be greater than 0.
     * @param memoryBudget the budget shared across all consumers, bounding aggregate heap usage; {@code null} is
     *                     treated as {@link ResponseMemoryBudget#UNLIMITED}.
     * @throws IllegalArgumentException if {@code bufferLimit} is less than or equal to 0.
     */
    public HeapBufferedAsyncEntityConsumer(int bufferLimit, ResponseMemoryBudget memoryBudget) {
        if (bufferLimit <= 0) {
            throw new IllegalArgumentException("bufferLimit must be greater than 0");
        }
        this.bufferLimitBytes = bufferLimit;
        this.memoryBudget = memoryBudget == null ? ResponseMemoryBudget.UNLIMITED : memoryBudget;
        this.memoryBudgetGeneration = this.memoryBudget.generation();
    }

    /**
     * Get the limit of the buffer.
     */
    public int getBufferLimit() {
        return bufferLimitBytes;
    }

    /**
     * Triggered to signal beginning of entity content stream.
     *
     * @param contentType the entity content type
     */
    @Override
    protected void streamStart(final ContentType contentType) throws HttpException, IOException {}

    /**
     * Triggered to obtain the capacity increment.
     *
     * @return the number of bytes this consumer is prepared to process.
     */
    @Override
    protected int capacityIncrement() {
        // Preserve the historical default (no flow control) unless a memory budget is active. When a budget is in
        // effect, advertise a bounded window that HttpCore replenishes each updateCapacity cycle, applying backpressure
        // without stalling and never exceeding the configured per-response buffer limit.
        if (!memoryBudget.isLimited()) {
            return Integer.MAX_VALUE;
        }
        return Math.min(bufferLimitBytes, MAX_CAPACITY_INCREMENT);
    }

    /**
     * Triggered to pass incoming data packet to the data consumer.
     *
     * @param src the data packet.
     * @param endOfStream flag indicating whether this data packet is the last in the data stream.
     *
     */
    @Override
    protected void data(final ByteBuffer src, final boolean endOfStream) throws IOException {
        if (src == null) {
            return;
        }

        int len = src.limit();
        if (len < 0) {
            len = 4096;
        } else if (len > bufferLimitBytes) {
            throw contentTooLong(len);
        }

        final int toAppend = src.remaining();

        ByteArrayBuffer buffer = bufferRef.get();
        if (buffer == null) {
            // Reserve the initial buffer capacity BEFORE allocating it, so the very first chunk is gated by the
            // budget too (fail fast under load rather than allocating and rejecting afterwards).
            if (!memoryBudget.tryReserve(memoryBudgetGeneration, len)) {
                throw budgetExceeded(len);
            }
            final ByteArrayBuffer created;
            try {
                created = createBuffer(len);
            } catch (final OutOfMemoryError oome) {
                memoryBudget.release(memoryBudgetGeneration, len);
                throw outOfMemory(oome);
            }
            if (bufferRef.compareAndSet(null, created)) {
                reservedBytes.addAndGet(len); // invariant: reservedBytes == buffer.capacity()
                buffer = created;
            } else {
                // Lost a race (defensive; a consumer is normally driven by a single reactor thread). Drop our reservation.
                memoryBudget.release(memoryBudgetGeneration, len);
                buffer = bufferRef.get();
            }
        }

        if (buffer.length() + len > bufferLimitBytes) {
            throw contentTooLong(len);
        }

        // Reserve only the new memory this append actually requires beyond the current capacity (fail fast). Content
        // that fits into existing spare capacity needs no new allocation and is therefore not reserved.
        final long oldCapacity = buffer.capacity();
        final long minGrowth = Math.max(0L, (long) buffer.length() + toAppend - oldCapacity);
        if (minGrowth > 0L && !memoryBudget.tryReserve(memoryBudgetGeneration, minGrowth)) {
            throw budgetExceeded(minGrowth);
        }

        try {
            if (src.hasArray()) {
                buffer.append(src.array(), src.arrayOffset() + src.position(), src.remaining());
            } else {
                while (src.hasRemaining()) {
                    buffer.append(src.get());
                }
            }
        } catch (final OutOfMemoryError oome) {
            if (minGrowth > 0L) {
                memoryBudget.release(memoryBudgetGeneration, minGrowth);
            }
            throw outOfMemory(oome);
        }

        // Reconcile with the actual capacity growth: ByteArrayBuffer may have doubled its backing array beyond the
        // minimum we reserved. Account the extra so the budget exactly reflects the buffer's real capacity, keeping
        // the reservedBytes == capacity invariant.
        final long actualGrowth = (long) buffer.capacity() - oldCapacity;
        final long extra = actualGrowth - minGrowth;
        if (extra > 0L) {
            memoryBudget.reserveUnchecked(memoryBudgetGeneration, extra);
        }
        if (actualGrowth > 0L) {
            reservedBytes.addAndGet(actualGrowth);
        }
    }

    private ContentTooLongException contentTooLong(final int len) {
        return new ContentTooLongException(
            "entity content is too long [" + len + "] for the configured buffer limit [" + bufferLimitBytes + "]"
        );
    }

    private ResponseBufferBudgetExceededException budgetExceeded(final long requestedBytes) {
        return new ResponseBufferBudgetExceededException(
            "response buffer memory budget exceeded: cannot reserve ["
                + requestedBytes
                + "] bytes (used ["
                + memoryBudget.usedBytes()
                + "] of ["
                + memoryBudget.maxBytes()
                + "] bytes)"
        );
    }

    /**
     * Allocates the backing buffer that accumulates the response content. Extracted so that allocation failures
     * (i.e. {@link OutOfMemoryError}) can be exercised deterministically in tests.
     *
     * @param initialCapacity the initial capacity of the buffer
     * @return a new {@link ByteArrayBuffer}
     */
    protected ByteArrayBuffer createBuffer(final int initialCapacity) {
        return new ByteArrayBuffer(initialCapacity);
    }

    /**
     * Releases the (potentially very large) response buffer and converts a fatal {@link OutOfMemoryError} into a
     * recoverable {@link IOException}.
     * <p>
     * Response content is consumed on the Apache HttpCore I/O reactor thread. An {@link Error} escaping a consumer
     * callback shuts the reactor down permanently, leaving the transport unable to serve any further request until it
     * is recreated (see
     * <a href="https://github.com/opensearch-project/opensearch-java/issues/1969">opensearch-java#1969</a>). By
     * releasing the buffer first and rethrowing as an {@link IOException}, the offending request fails cleanly while
     * the reactor - and therefore the transport - stays alive.
     *
     * @param oome the error raised while allocating or growing the buffer
     * @return an {@link IOException} wrapping the original error
     */
    private IOException outOfMemory(final OutOfMemoryError oome) {
        // Free the partially-filled buffer before allocating anything else, to relieve memory pressure.
        releaseResources();
        return new IOException(
            "Ran out of memory while buffering the HTTP response; the response may be too large for the available heap",
            oome
        );
    }

    /**
     * Triggered to generate entity representation.
     *
     * @return the entity content
     */
    @Override
    protected byte[] generateContent() throws IOException {
        final ByteArrayBuffer buffer = bufferRef.get();
        if (buffer == null) {
            return new byte[0];
        }
        try {
            return buffer.toByteArray();
        } catch (final OutOfMemoryError oome) {
            throw outOfMemory(oome);
        }
    }

    /**
     * Release resources being held
     */
    @Override
    public void releaseResources() {
        ByteArrayBuffer buffer = bufferRef.getAndSet(null);
        if (buffer != null) {
            buffer.clear();
            buffer = null;
        }
        // Return any reserved bytes to the shared budget. getAndSet(0) keeps this idempotent across repeated calls.
        final long reserved = reservedBytes.getAndSet(0L);
        if (reserved > 0L) {
            memoryBudget.release(memoryBudgetGeneration, reserved);
        }
    }

    /**
     * Gets current byte buffer instance
     * @return byte buffer instance
     */
    ByteArrayBuffer getBuffer() {
        return bufferRef.get();
    }
}
