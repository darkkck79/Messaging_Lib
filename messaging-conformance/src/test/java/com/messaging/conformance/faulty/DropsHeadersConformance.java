package com.messaging.conformance.faulty;

/**
 * Runs the full conformance suite against {@link DropsHeadersTransport}. Excluded from
 * normal test discovery (see messaging-conformance/build.gradle); only
 * {@code MetaConformanceTest} runs it, expecting specific methods to FAIL.
 */
public class DropsHeadersConformance extends AbstractFaultyConformanceTest {

    @Override
    protected InMemoryTransport createFaultyTransport() { return new DropsHeadersTransport(); }

    @Override
    protected String variantUrl() { return "faulty://drops-headers"; }
}
