package com.messaging.conformance.faulty;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

/**
 * Faulty transport that acks before the handler completes.
 * This violates at-least-once delivery semantics.
 */
public final class AcksBeforeHandlerTransport implements Transport {
    private static final Logger log = LoggerFactory.getLogger(AcksBeforeHandlerTransport.class);
    private final Map<Destination, ExecutorService> threadPools = new ConcurrentHashMap<>();
    private final Map<Destination, Queue<Message>> queues = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> publish(Destination destination, Message message) {
        queues.computeIfAbsent(destination, d -> new ConcurrentLinkedQueue<>()).add(message);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
        ExecutorService executor = threadPools.computeIfAbsent(destination, d ->
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "consumer-" + destination);
                t.setDaemon(true);
                return t;
            }));

        Queue<Message> q = queues.computeIfAbsent(destination, d -> new ConcurrentLinkedQueue<>());

        // Get next message
        Message msg = q.poll();
        if (msg == null) {
            // Wait for a message
            final Queue<Message> finalQ = q;
            msg = new CompletableFuture<Message>().whenComplete((m, e) -> {}).join();
            msg = q.poll();
        }

        // ACK immediately (before handler completes) - this is the bug
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        ackFuture.complete(null);

        CompletableFuture<Void> handlerFuture = executor.submit(() -> {
            try {
                handler.handle(msg);
            } catch (Exception e) {
                // Message is lost - acked already
            }
            return null;
        });

        return CompletableFuture.completedFuture(() -> {
            q.poll();
        });
    }

    @Override
    public void close(Duration timeout) {
        threadPools.values().forEach(ExecutorService::shutdownNow);
        queues.clear();
    }
}
