/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.transport.httpclient5.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ResponseMemoryBudgetTest {

    @Test
    public void testReserveUpToLimitThenReject() {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(1000);

        assertTrue(budget.isLimited());
        assertTrue(budget.tryReserve(600));
        assertEquals(600L, budget.usedBytes());

        // Would exceed the budget (600 + 500 > 1000).
        assertFalse(budget.tryReserve(500));
        assertEquals(600L, budget.usedBytes());

        // Exactly fills the budget.
        assertTrue(budget.tryReserve(400));
        assertEquals(1000L, budget.usedBytes());

        // Nothing more fits.
        assertFalse(budget.tryReserve(1));
    }

    @Test
    public void testReleaseFreesBudget() {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(1000);
        assertTrue(budget.tryReserve(1000));
        assertFalse(budget.tryReserve(1));

        budget.release(400);
        assertEquals(600L, budget.usedBytes());
        assertTrue(budget.tryReserve(400));
        assertEquals(1000L, budget.usedBytes());
    }

    @Test
    public void testUnlimitedBudgetAlwaysReserves() {
        assertFalse(ResponseMemoryBudget.UNLIMITED.isLimited());
        assertTrue(ResponseMemoryBudget.UNLIMITED.tryReserve(Long.MAX_VALUE));
        assertEquals(0L, ResponseMemoryBudget.UNLIMITED.usedBytes());

        // A non-positive maxBytes also means unlimited.
        ResponseMemoryBudget disabled = new ResponseMemoryBudget(0);
        assertFalse(disabled.isLimited());
        assertTrue(disabled.tryReserve(Long.MAX_VALUE));
    }

    @Test
    public void testNonPositiveReserveIsNoOp() {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(10);
        assertTrue(budget.tryReserve(0));
        assertTrue(budget.tryReserve(-5));
        assertEquals(0L, budget.usedBytes());
    }

    @Test
    public void testOverflowIsRejected() {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(Long.MAX_VALUE);
        assertTrue(budget.tryReserve(Long.MAX_VALUE - 1));
        // Reserving more would overflow the counter; it must be rejected, not wrap negative.
        assertFalse(budget.tryReserve(100));
    }

    @Test
    public void testResetIgnoresReleasesFromStaleGeneration() {
        ResponseMemoryBudget budget = new ResponseMemoryBudget(1000);
        long originalGeneration = budget.generation();

        assertTrue(budget.tryReserve(originalGeneration, 500));
        assertEquals(500L, budget.usedBytes());

        budget.reset();
        assertEquals(0L, budget.usedBytes());

        // Consumers created before reset can be released later. Their stale reservations must not subtract from the
        // new generation, otherwise the budget can go negative and admit more than the configured max.
        budget.release(originalGeneration, 500);
        assertEquals(0L, budget.usedBytes());
        assertFalse(budget.tryReserve(originalGeneration, 1));

        assertTrue(budget.tryReserve(1000));
        assertEquals(1000L, budget.usedBytes());
        assertFalse(budget.tryReserve(1));
    }
}
