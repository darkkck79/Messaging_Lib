package com.messaging.conformance.faulty;

import com.messaging.*;
import com.messaging.spi.Transport;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A correct in-memory transport, used both directly (InMemoryConformance) and as the
 * base class for deliberately faulty variants that prove the conformance suite catches
 * real violations of the delivery contract (§A).
 *
 * <p>One dedicated consumer thread per subscriber ("consumer unit", §B/§C), one message
 * in flight per unit. Queue destinations are competing-consumers over a single shared
 * deque; topic destinations fan out to a private deque per subscriber. A failed,
 * null, cancelled, or throwing handler outcome re-offers the message to the front of its
 * deque for redelivery (§A) — never silently dropped.
 */
public class InMemoryTransport implements Transport {

    private final Set<Destination> provisioned = ConcurrentHashMap.newKeySet();
    private final Map<Destination, LinkedBlockingDeque<Message>> queueBacking = new ConcurrentHashMap<>();
    private final Map<Destination, List<LinkedBlockingDeque<Message>>> fanOutDeques = new ConcurrentHashMap<>();
    private final List<ConsumerUnit> consumerUnits = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    public final void provision(Destination destination) {
        provisioned.add(destination);
        if (destination instanceof Queue) queueBacking.putIfAbsent(destination, new LinkedBlockingDeque<>());
    }

    @Override
    public CompletableFuture<Void> publish(Destination destination, Message message) {
        Exception unusable = unusable(destination);
        if (unusable != null) return CompletableFuture.failedFuture(unusable);
        deliver(destination, message);
        return CompletableFuture.completedFuture(null);
    }

    /** Fan out to every subscriber's private deque, or hand to the shared competing-consumers deque. */
    private void deliver(Destination destination, Message message) {
        if (fansOut(destination)) {
            for (var q : fanOutDeques.getOrDefault(destination, List.of())) q.add(message);
        } else {
            queueBacking.get(destination).add(message);
        }
    }

    @Override
    public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
        Exception unusable = unusable(destination);
        if (unusable != null) return CompletableFuture.failedFuture(unusable);

        LinkedBlockingDeque<Message> myDeque;
        Runnable onClose;
        if (fansOut(destination)) {
            myDeque = new LinkedBlockingDeque<>();
            fanOutDeques.computeIfAbsent(destination, d -> new CopyOnWriteArrayList<>()).add(myDeque);
            onClose = () -> fanOutDeques.get(destination).remove(myDeque);
        } else {
            myDeque = queueBacking.get(destination);
            onClose = () -> {};
        }

        ConsumerUnit unit = createConsumerUnit(myDeque, handler);
        consumerUnits.add(unit);
        unit.start();

        return CompletableFuture.completedFuture(() -> {
            unit.stop();
            onClose.run();
            consumerUnits.remove(unit);
        });
    }

    /** Whether every subscriber gets its own copy (Topic) rather than competing (Queue). Faulty variants override this. */
    protected boolean fansOut(Destination destination) {
        return destination instanceof Topic;
    }

    /** Extension point for faulty variants. */
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler) {
        return new ConsumerUnit(deque, handler);
    }

    @Override
    public void close(Duration timeout) {
        closed = true;
        consumerUnits.forEach(ConsumerUnit::stop);
        consumerUnits.clear();
        queueBacking.clear();
        fanOutDeques.clear();
    }

    /** Why {@code destination} cannot be published to or subscribed to right now, or null if it can. */
    private Exception unusable(Destination destination) {
        if (closed) return new IllegalStateException("Transport is closed");
        if (!provisioned.contains(destination)) {
            return new MessagingException("No such destination (not provisioned): " + destination);
        }
        return null;
    }

    /** One thread, one message in flight, per §B/§C. Consumes → runs handler → settles or redelivers. */
    protected static class ConsumerUnit {
        private final LinkedBlockingDeque<Message> deque;
        private final MessageHandler handler;
        private volatile Thread thread;
        private volatile boolean stopped;

        ConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler) {
            this.deque = deque;
            this.handler = handler;
        }

        void start() {
            thread = new Thread(this::run, "in-memory-consumer");
            thread.setDaemon(true);
            thread.start();
        }

        void stop() {
            stopped = true;
            if (thread != null) thread.interrupt();
        }

        private void run() {
            while (!stopped) {
                Message message;
                try {
                    message = deque.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                processOneMessage(message);
            }
        }

        protected void processOneMessage(Message message) {
            CompletableFuture<Void> outcome;
            try {
                outcome = handler.handle(message);
                if (outcome == null) outcome = CompletableFuture.failedFuture(
                    new MessagingException("Handler returned null future"));
            } catch (Throwable synchronousThrow) {
                outcome = CompletableFuture.failedFuture(synchronousThrow);
            }
            try {
                outcome.get();
                // settled successfully; do not redeliver
            } catch (Exception failedOrCancelled) {
                deque.offerFirst(message);
            }
        }
    }
}
