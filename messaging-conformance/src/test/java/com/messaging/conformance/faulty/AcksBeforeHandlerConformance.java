package com.messaging.conformance.faulty;

/**
 * Runs the full conformance suite against {@link AcksBeforeHandlerTransport}. Excluded
 * from normal test discovery (see messaging-conformance/build.gradle); only
 * {@code MetaConformanceTest} runs it, expecting specific methods to FAIL.
 */
public class AcksBeforeHandlerConformance extends AbstractFaultyConformanceTest {

    @Override
    protected InMemoryTransport createFaultyTransport() { return new AcksBeforeHandlerTransport(); }

    @Override
    protected String variantUrl() { return "faulty://acks-before-handler"; }
}
