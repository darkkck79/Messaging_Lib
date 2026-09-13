package com.messaging.conformance;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.conformance.faulty.InMemoryTransport;
import com.messaging.internal.DefaultMessageBus;

/**
 * Exercises the shared conformance suite against an in-memory transport. As-is this is
 * the "control" run proving the suite passes against a transport with no deliberate
 * faults; the faulty variants override {@link #createTransport()}. Run by
 * {@code InMemoryConformanceTest}, {@code MetaConformanceTest}, and the sample app.
 */
public class InMemoryConformance extends AbstractMessagingConformanceTest {

    private InMemoryTransport transport;

    /** The transport under test; faulty variants return a deliberately broken one. */
    protected InMemoryTransport createTransport() {
        return new InMemoryTransport();
    }

    @Override
    protected final MessageBus createBus() {
        transport = createTransport();
        return new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(),
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
