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
 * Correct in-memory transport for testing the conformance suite.
 * One thread per destination, one message in flight per thread.
 */
public final class InMemoryTransport implements Transport {
    private static final Logger log = LoggerFactory.getLogger(InMemoryTransport.class);
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

        // Get or create the queue
        Queue<Message> q = queues.computeIfAbsent(destination, d -> new ConcurrentLinkedQueue<>());

        // Get next message (or wait if none available)
        Message msg = q.poll();
        if (msg == null) {
            // Wait for a message
            final Queue<Message> finalQ = q;
            final ExecutorService finalExecutor = executor;
            msg = new CompletableFuture<Message>().whenComplete((m, e) -> {}).join();
            // Simplified: just poll
            msg = q.poll();
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.submit(() -> {
            try {
                handler.handle(msg);
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        return CompletableFuture.completedFuture(() -> {
            q.poll(); // Remove the message
        });
    }

    @Override
    public void close(Duration timeout) {
        threadPools.values().forEach(ExecutorService::shutdownNow);
        queues.clear();
    }
}
