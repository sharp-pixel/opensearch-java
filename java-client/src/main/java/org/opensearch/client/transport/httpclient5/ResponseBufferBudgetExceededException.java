/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.transport.httpclient5;

import java.io.IOException;

/**
 * Thrown when buffering a response would exceed the shared response-buffer memory budget (configured via
 * {@link ApacheHttpClient5TransportBuilder#setMaxTotalResponseBufferBytes(long)}).
 * <p>
 * Unlike {@link org.apache.hc.core5.http.ContentTooLongException} - which means a <em>single</em> response is larger
 * than the per-response buffer limit and is therefore not retryable - this exception signals a transient
 * <em>overload</em>: too much response data is being buffered concurrently right now. It is typically retryable after
 * a back-off once in-flight responses drain. Distinguishing the two lets callers apply load-shedding/retry policies
 * appropriately.
 */
public class ResponseBufferBudgetExceededException extends IOException {

    /**
     * Creates a new instance with the provided message.
     *
     * @param message the detail message
     */
    public ResponseBufferBudgetExceededException(String message) {
        super(message);
    }
}
