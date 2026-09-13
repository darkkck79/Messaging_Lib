package com.messaging.conformance.faulty;

import com.messaging.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Faulty variant: delivers every Queue message to ALL subscribers instead of exactly one
 * (fan-out instead of competing-consumers). Violates the Queue semantics in the scope-of
 * -the-guarantee table (§ Scope of the guarantee). Used only by
 * {@code QueueFansOutConformance}, excluded from normal test discovery, to prove
 * {@code MetaConformanceTest} catches it.
 */
public final class QueueFansOutTransport extends InMemoryTransport {

    private final Map<Destination, List<LinkedBlockingDeque<Message>>> subscriberDeques = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
        LinkedBlockingDeque<Message> myDeque = new LinkedBlockingDeque<>();
        subscriberDeques.computeIfAbsent(destination, d -> new CopyOnWriteArrayList<>()).add(myDeque);

        ConsumerUnit unit = createConsumerUnit(myDeque, handler);
        unit.start();

        return CompletableFuture.completedFuture(() -> {
            unit.stop();
            subscriberDeques.get(destination).remove(myDeque);
        });
    }

    @Override
    protected void deliver(Destination destination, Message message) {
        // Bug under test: broadcast a Queue message to every subscriber, exactly like a
        // Topic would, instead of exactly one competing consumer.
        for (var deque : subscriberDeques.getOrDefault(destination, List.of())) deque.add(message);
    }
}
