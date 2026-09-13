package com.messaging.conformance.faulty;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.internal.DefaultMessageBus;

/**
 * Shared wiring for the three faulty conformance harnesses: construct the deliberately
 * broken {@link InMemoryTransport} variant directly (rather than via
 * {@code Messaging.connect}) so tests can provision destinations straight against it.
 * {@link FaultyTransportProvider} still registers the same variants under the
 * {@code faulty://} scheme for real SPI resolution outside this suite.
 */
abstract class AbstractFaultyConformanceTest extends AbstractMessagingConformanceTest {

    private InMemoryTransport transport;

    /** The deliberately broken transport this harness exercises. */
    protected abstract InMemoryTransport createFaultyTransport();

    /** The {@code faulty://} URL naming this variant, for parity with {@link FaultyTransportProvider}. */
    protected abstract String variantUrl();

    @Override
    protected final MessageBus createBus() {
        transport = createFaultyTransport();
        return new DefaultMessageBus(transport,
            MessagingConfig.builder().url(variantUrl()).build(),
            MessagingListener.noOp());
    }

    @Override
    protected final Destination provisionTopic(String name) {
        Topic topic = Topic.of(name);
        transport.provision(topic);
        return topic;
    }

    @Override
    protected final Destination provisionQueue(String name) {
        Queue queue = Queue.of(name);
        transport.provision(queue);
        return queue;
    }
}
