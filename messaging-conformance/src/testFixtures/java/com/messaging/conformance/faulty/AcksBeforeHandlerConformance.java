package com.messaging.conformance.faulty;

import com.messaging.conformance.BusSettings;
import com.messaging.conformance.InMemoryConformance;

/**
 * Runs the full conformance suite against {@link AcksBeforeHandlerTransport}. Lives in
 * testFixtures, so {@code ./gradlew test} never discovers it; {@code MetaConformanceTest}
 * and the sample app run it, expecting {@link #EXPECTED_FAILURE} to FAIL.
 */
public class AcksBeforeHandlerConformance extends InMemoryConformance {

    /** The scenario this fault must break. */
    public static final String EXPECTED_FAILURE = "redeliveryOnExceptionalFuture";

    @Override
    protected InMemoryTransport createTransport(BusSettings settings) {
        return new AcksBeforeHandlerTransport(settings.listener(), settings.closeTimeout(), settings.concurrency());
    }
}
