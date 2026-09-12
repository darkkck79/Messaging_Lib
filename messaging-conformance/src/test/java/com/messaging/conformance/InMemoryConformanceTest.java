package com.messaging.conformance;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.internal.DefaultMessageBus;
import com.messaging.spi.Transport;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Exercises the shared conformance suite against a correct, minimal in-memory
 * transport. This is the "control" run proving the suite passes against a
 * transport with no deliberate faults.
 */
class InMemoryConformanceTest extends AbstractMessagingConformanceTest {

    @Override
    protected MessageBus createBus() {
        return new DefaultMessageBus(new CorrectInMemoryTransport(),
            MessagingConfig.builder().url("test://localhost").build(),
            MessagingListener.noOp());
    }

    @Override
    protected void provisionTopic(String name) { /* no-op: in-memory, always available */ }

    @Override
    protected void provisionQueue(String name) { /* no-op: in-memory, always available */ }

    private static final class CorrectInMemoryTransport implements Transport {
        private final Set<String> known = ConcurrentHashMap.newKeySet();
        private final Map<Destination, CopyOnWriteArrayList<MessageHandler>> subscribers = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Void> publish(Destination destination, Message message) {
            var handlers = subscribers.get(destination);
            if (handlers == null || handlers.isEmpty()) {
                return CompletableFuture.failedFuture(
                    new MessagingException("No subscribers for " + destination));
            }
            for (MessageHandler handler : handlers) {
                handler.handle(message);
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
            subscribers.computeIfAbsent(destination, d -> new CopyOnWriteArrayList<>()).add(handler);
            return CompletableFuture.completedFuture(() ->
                subscribers.getOrDefault(destination, new CopyOnWriteArrayList<>()).remove(handler));
        }

        @Override
        public void close(Duration timeout) {
            subscribers.clear();
        }
    }
}
