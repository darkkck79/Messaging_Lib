package com.messaging.conformance.faulty;

/**
 * Runs the full conformance suite against {@link QueueFansOutTransport}. Excluded from
 * normal test discovery (see messaging-conformance/build.gradle); only
 * {@code MetaConformanceTest} runs it, expecting specific methods to FAIL.
 */
public class QueueFansOutConformance extends AbstractFaultyConformanceTest {

    @Override
    protected InMemoryTransport createFaultyTransport() { return new QueueFansOutTransport(); }

    @Override
    protected String variantUrl() { return "faulty://queue-fans-out"; }
}
