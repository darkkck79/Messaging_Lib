package com.messaging.conformance;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.conformance.faulty.InMemoryTransport;
import com.messaging.internal.DefaultMessageBus;
import com.messaging.internal.SafeListener;

public class InMemoryConformance extends AbstractMessagingConformanceTest {

    private InMemoryTransport transport;

    /** The transport under test; faulty variants return a deliberately broken one. */
    protected InMemoryTransport createTransport(BusSettings settings) {
        return new InMemoryTransport(SafeListener.wrap(settings.listener()), settings.closeTimeout(), settings.concurrency());
    }

    @Override
    protected final MessageBus createBus(BusSettings settings) {
        transport = createTransport(settings);
        return new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(),
            settings.listener());
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
