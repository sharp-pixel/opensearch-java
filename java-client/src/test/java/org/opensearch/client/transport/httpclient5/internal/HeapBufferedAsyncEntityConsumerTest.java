/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.transport.httpclient5.internal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hc.core5.http.ContentTooLongException;
import org.apache.hc.core5.util.ByteArrayBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.transport.httpclient5.ResponseBufferBudgetExceededException;

public class HeapBufferedAsyncEntityConsumerTest extends RandomizedTest {
    private static final int BUFFER_LIMIT = 100 * 1024 * 1024 /* 100Mb */;
    private HeapBufferedAsyncEntityConsumer consumer;

    @Before
    public void setUp() {
        consumer = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT);
    }

    @After
    public void tearDown() {
        consumer.releaseResources();
    }

    @Test
    public void testConsumerAllocatesBufferLimit() throws IOException {
        consumer.consume((ByteBuffer) randomByteBufferOfLength(1000).flip());
        assertThat(consumer.getBuffer().capacity(), equalTo(1000));
    }

    @Test
    public void testConsumerAllocatesEmptyBuffer() throws IOException {
        consumer.consume((ByteBuffer) ByteBuffer.allocate(0).flip());
        assertThat(consumer.getBuffer().capacity(), equalTo(0));
    }

    @Test
    public void testConsumerExpandsBufferLimits() throws IOException {
        consumer.consume((ByteBuffer) randomByteBufferOfLength(1000).flip());
        consumer.consume((ByteBuffer) randomByteBufferOfLength(2000).flip());
        consumer.consume((ByteBuffer) randomByteBufferOfLength(3000).flip());
        assertThat(consumer.getBuffer().capacity(), equalTo(6000));
    }

    @Test
    public void testConsumerAllocatesLimit() throws IOException {
        consumer.consume((ByteBuffer) randomByteBufferOfLength(BUFFER_LIMIT).flip());
        assertThat(consumer.getBuffer().capacity(), equalTo(BUFFER_LIMIT));
    }

    @Test
    public void testConsumerFailsToAllocateOverLimit() throws IOException {
        assertThrows(ContentTooLongException.class, () -> consumer.consume((ByteBuffer) randomByteBufferOfLength(BUFFER_LIMIT + 1).flip()));
    }

    @Test
    public void testConsumerFailsToExpandOverLimit() throws IOException {
        consumer.consume((ByteBuffer) randomByteBufferOfLength(BUFFER_LIMIT).flip());
        assertThrows(ContentTooLongException.class, () -> consumer.consume((ByteBuffer) randomByteBufferOfLength(1).flip()));
    }

    @Test
    public void testSharedBudgetIsEnforcedAcrossConsumers() throws IOException {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(1000);
        HeapBufferedAsyncEntityConsumer c1 = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT, budget);
        HeapBufferedAsyncEntityConsumer c2 = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT, budget);

        c1.consume((ByteBuffer) randomByteBufferOfLength(600).flip());
        assertThat(budget.usedBytes(), equalTo(600L));

        // The second consumer cannot reserve another 600 bytes from the shared budget (600 + 600 > 1000). It must
        // fail with the dedicated overload exception (not ContentTooLongException, which means "single response too
        // big" and is non-retryable).
        assertThrows(ResponseBufferBudgetExceededException.class, () -> c2.consume((ByteBuffer) randomByteBufferOfLength(600).flip()));
        assertThat(budget.usedBytes(), equalTo(600L));

        // Releasing the first consumer returns its bytes to the shared budget...
        c1.releaseResources();
        assertThat(budget.usedBytes(), equalTo(0L));

        // ...so the second consumer can now buffer.
        c2.consume((ByteBuffer) randomByteBufferOfLength(600).flip());
        assertThat(budget.usedBytes(), equalTo(600L));

        c2.releaseResources();
        assertThat(budget.usedBytes(), equalTo(0L));
    }

    @Test
    public void testBudgetTracksActualCapacityNotContent() throws IOException {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(10_000_000);
        HeapBufferedAsyncEntityConsumer c = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT, budget);

        // Multiple packets force ByteArrayBuffer to grow, leaving spare capacity after doubling. The budget must
        // reflect the buffer's real capacity exactly — neither under-counting (content only) nor over-counting
        // (content appended into spare capacity on top of capacity).
        c.consume((ByteBuffer) randomByteBufferOfLength(1000).flip());
        c.consume((ByteBuffer) randomByteBufferOfLength(1000).flip());
        c.consume((ByteBuffer) randomByteBufferOfLength(1000).flip());
        c.consume((ByteBuffer) randomByteBufferOfLength(1000).flip());

        assertThat(budget.usedBytes(), equalTo((long) c.getBuffer().capacity()));

        c.releaseResources();
        assertThat(budget.usedBytes(), equalTo(0L));
    }

    @Test
    public void testReleaseResourcesReturnsReservationToBudget() throws IOException {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(1_000_000);
        HeapBufferedAsyncEntityConsumer c = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT, budget);
        c.consume((ByteBuffer) randomByteBufferOfLength(2000).flip());
        assertThat(budget.usedBytes(), equalTo((long) c.getBuffer().capacity()));

        // releaseResources() is the single release path HttpCore invokes on any terminal event (completed, failed or
        // cancelled), so a cancelled in-flight response also returns its reservation.
        c.releaseResources();
        assertThat(budget.usedBytes(), equalTo(0L));

        // Idempotent: a second release must not double-release.
        c.releaseResources();
        assertThat(budget.usedBytes(), equalTo(0L));
    }

    @Test
    public void testCapacityIncrementBoundedOnlyWhenBudgetActive() {
        // Default (no budget): preserve historical behavior of an unbounded window.
        HeapBufferedAsyncEntityConsumer noBudget = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT);
        assertThat(noBudget.capacityIncrement(), equalTo(Integer.MAX_VALUE));

        // With a budget active: backpressure via a bounded window, capped at the per-response limit.
        HeapBufferedAsyncEntityConsumer withBudget = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT, new ResponseMemoryBudget(1_000_000));
        assertThat(withBudget.capacityIncrement(), equalTo(HeapBufferedAsyncEntityConsumer.MAX_CAPACITY_INCREMENT));

        HeapBufferedAsyncEntityConsumer smallLimit = new HeapBufferedAsyncEntityConsumer(4096, new ResponseMemoryBudget(1_000_000));
        assertThat(smallLimit.capacityIncrement(), equalTo(4096));
    }

    @Test
    public void testConsumerConvertsOutOfMemoryErrorToIOException() {
        // Simulate the heap being exhausted while allocating the response buffer (e.g. under load / DDoS).
        HeapBufferedAsyncEntityConsumer oomConsumer = new HeapBufferedAsyncEntityConsumer(BUFFER_LIMIT) {
            @Override
            protected ByteArrayBuffer createBuffer(int initialCapacity) {
                throw new OutOfMemoryError("simulated OOM while buffering response");
            }
        };

        IOException ex = assertThrows(IOException.class, () -> oomConsumer.consume((ByteBuffer) randomByteBufferOfLength(1000).flip()));

        // The fatal error must be converted to a recoverable IOException so it cannot escape onto and kill the
        // I/O reactor thread (GH-1969)...
        assertThat(ex.getCause() instanceof OutOfMemoryError, equalTo(true));
        // ...and the (partial) buffer must be released to relieve memory pressure.
        assertThat(oomConsumer.getBuffer(), equalTo(null));
    }

    private static ByteBuffer randomByteBufferOfLength(int length) {
        return ByteBuffer.allocate(length).put(randomBytesOfLength(length));
    }
}
