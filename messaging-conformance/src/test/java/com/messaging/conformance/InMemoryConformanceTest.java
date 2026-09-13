package com.messaging.conformance;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.conformance.faulty.InMemoryTransport;
import com.messaging.internal.DefaultMessageBus;

/**
 * Exercises the shared conformance suite against a correct, minimal in-memory
 * transport. This is the "control" run proving the suite passes against a
 * transport with no deliberate faults.
 */
class InMemoryConformanceTest extends AbstractMessagingConformanceTest {

    private InMemoryTransport transport;

    @Override
    protected MessageBus createBus() {
        transport = new InMemoryTransport();
        return new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(),
            MessagingListener.noOp());
    }

    @Override
    protected Destination provisionTopic(String name) {
        Topic topic = Topic.of(name);
        transport.provision(topic);
        return topic;
    }

    @Override
    protected Destination provisionQueue(String name) {
        Queue queue = Queue.of(name);
        transport.provision(queue);
        return queue;
    }
}
