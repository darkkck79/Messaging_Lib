package com.messaging.conformance.faulty;

import com.messaging.*;
import com.messaging.spi.Transport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A correct in-memory transport, used both directly (InMemoryConformance) and as the
 * base class for deliberately faulty variants that prove the conformance suite catches
 * real violations of the delivery contract (§A, §C, §J).
 *
 * <p>{@code concurrency} consumer units per subscription share one deque: a private one
 * per subscriber for a Topic, the destination's single backing deque for a Queue (so
 * separate {@code subscribe} calls on the same queue still compete). Each unit runs its
 * own thread with at most one message in flight. A failed, null, cancelled, or throwing
 * handler outcome re-offers the message to the front of its deque for redelivery (§A).
 * {@code close} (§J) stops accepting new work, waits for in-flight outcomes against one
 * shared deadline, then cancels whatever is still running.
 */
public class InMemoryTransport implements Transport {

    private final MessagingListener listener;
    private final Duration closeTimeout;
    private final int concurrency;

    private final Set<Destination> provisioned = ConcurrentHashMap.newKeySet();
    private final Map<Destination, LinkedBlockingDeque<Message>> queueBacking = new ConcurrentHashMap<>();
    private final Map<Destination, List<LinkedBlockingDeque<Message>>> fanOutDeques = new ConcurrentHashMap<>();
    private final List<ConsumerUnit> consumerUnits = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    public InMemoryTransport(MessagingListener listener, Duration closeTimeout, int concurrency) {
        this.listener = listener;
        this.closeTimeout = closeTimeout;
        this.concurrency = concurrency;
    }

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

        LinkedBlockingDeque<Message> sharedDeque;
        Runnable onClose;
        if (fansOut(destination)) {
            sharedDeque = new LinkedBlockingDeque<>();
            fanOutDeques.computeIfAbsent(destination, d -> new CopyOnWriteArrayList<>()).add(sharedDeque);
            onClose = () -> fanOutDeques.get(destination).remove(sharedDeque);
        } else {
            sharedDeque = queueBacking.get(destination);
            onClose = () -> {};
        }

        List<ConsumerUnit> units = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            ConsumerUnit unit = createConsumerUnit(sharedDeque, handler, listener, destination);
            units.add(unit);
            consumerUnits.add(unit);
            unit.start();
        }

        return CompletableFuture.completedFuture(() -> {
            long deadline = System.nanoTime() + closeTimeout.toNanos();
            units.forEach(ConsumerUnit::requestStop);
            for (ConsumerUnit unit : units) {
                unit.awaitStop(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
                consumerUnits.remove(unit);
            }
            onClose.run();
        });
    }

    /** Whether every subscriber gets its own copy (Topic) rather than competing (Queue). Faulty variants override this. */
    protected boolean fansOut(Destination destination) {
        return destination instanceof Topic;
    }

    /** Extension point for faulty variants that need a different handler or a different settle rule. */
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler,
                                               MessagingListener listener, Destination destination) {
        return new ConsumerUnit(deque, handler, listener, destination);
    }

    @Override
    public void close(Duration timeout) {
        closed = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        consumerUnits.forEach(ConsumerUnit::requestStop);
        for (ConsumerUnit unit : consumerUnits) {
            unit.awaitStop(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        }
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

    /** One thread, one message in flight, per §B/§C. Consumes, runs the handler on this
     * thread, then settles — success stops redelivery, failure re-offers the message. */
    protected static class ConsumerUnit {
        private final LinkedBlockingDeque<Message> deque;
        private final MessageHandler handler;
        private final MessagingListener listener;
        private final Destination destination;
        private volatile Thread thread;
        private volatile boolean stopping;
        private volatile CompletableFuture<Void> currentOutcome;

        ConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler,
                     MessagingListener listener, Destination destination) {
            this.deque = deque;
            this.handler = handler;
            this.listener = listener;
            this.destination = destination;
        }

        void start() {
            thread = new Thread(this::run, "in-memory-consumer");
            thread.setDaemon(true);
            thread.start();
        }

        /** Signal this unit to stop taking new messages; does not wait. */
        void requestStop() {
            stopping = true;
        }

        /** §J: wait up to {@code remaining} for the in-flight handler. If it's still
         * running when time is up, cancel its outcome (whose failure path re-offers the
         * message via {@link #settle}) and give the thread a short grace period to exit. */
        void awaitStop(Duration remaining) {
            if (thread == null) return;
            try {
                thread.join(Math.max(0, remaining.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (thread.isAlive()) {
                CompletableFuture<Void> outcome = currentOutcome;
                if (outcome != null) outcome.cancel(true);
                try {
                    thread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void run() {
            while (!stopping) {
                Message message;
                try {
                    message = deque.poll(200, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (message == null) continue;
                processOneMessage(message);
            }
        }

        private void processOneMessage(Message message) {
            CompletableFuture<Void> outcome;
            try {
                outcome = handler.handle(message);
                if (outcome == null) outcome = CompletableFuture.failedFuture(
                    new MessagingException("Handler returned null future"));
            } catch (Throwable synchronousThrow) {
                outcome = CompletableFuture.failedFuture(synchronousThrow);
            }
            currentOutcome = outcome;
            boolean succeeded;
            try {
                outcome.get();
                succeeded = true;
            } catch (Exception failedOrCancelled) {
                succeeded = false;
            }
            currentOutcome = null;
            settle(message, succeeded);
        }

        /** Success: report {@code onConsumed}, do not redeliver. Failure: re-offer the
         * message to the front of the deque and report {@code onError}. Faulty variants
         * override this single method to break settlement deliberately. */
        protected void settle(Message message, boolean succeeded) {
            if (succeeded) {
                listener.onConsumed(destination);
            } else {
                deque.offerFirst(message);
                listener.onError(destination, new MessagingException("Handler did not settle: " + message));
            }
        }
    }
}
