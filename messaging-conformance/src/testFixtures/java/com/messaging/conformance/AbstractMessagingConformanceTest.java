package com.messaging.conformance;

import com.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

/**
 * Behavioural contract for every transport, per the design spec (§A, §D, §F, §G, §H, §J).
 * Concrete subclasses supply a bus and destination provisioning; faulty in-memory
 * transports extend this to prove the suite itself catches real violations.
 */
public abstract class AbstractMessagingConformanceTest {

    protected MessageBus bus;

    private static final BusSettings DEFAULT_SETTINGS =
        new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(1), 1);

    @BeforeEach
    void setUp() {
        bus = createBus(DEFAULT_SETTINGS);
    }

    @AfterEach
    void tearDown() {
        if (bus != null) bus.close();
    }

    /** Create the MessageBus under test with the given settings. */
    protected abstract MessageBus createBus(BusSettings settings);

    /** Provision a topic out-of-band and return its Destination. */
    protected abstract Destination provisionTopic(String name);

    /** Provision a queue out-of-band and return its Destination. */
    protected abstract Destination provisionQueue(String name);

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    // ==================== Payload & headers ====================

    @Test
    void bodyRoundTrip() {
        Destination topic = provisionTopic("body-rt");
        var received = new CopyOnWriteArrayList<byte[]>();
        awaitSubscribed(topic, message -> {
            received.add(message.body());
            return CompletableFuture.completedFuture(null);
        });

        publish(topic, "Hello, World!".getBytes());

        await().atMost(TIMEOUT).untilAsserted(() ->
            assertThat(received).hasSize(1));
        assertThat(new String(received.get(0))).isEqualTo("Hello, World!");
    }

    @Test
    void emptyBodyRoundTrip() {
        Destination topic = provisionTopic("empty-body");
        var received = new CopyOnWriteArrayList<byte[]>();
        awaitSubscribed(topic, message -> {
            received.add(message.body());
            return CompletableFuture.completedFuture(null);
        });

        publish(topic, new byte[0]);

        await().atMost(TIMEOUT).untilAsserted(() ->
            assertThat(received).hasSize(1));
        assertThat(received.get(0)).isEmpty();
    }

    @Test
    void headersRoundTrip() {
        Destination topic = provisionTopic("headers-rt");
        var received = new CopyOnWriteArrayList<Map<String, String>>();
        awaitSubscribed(topic, message -> {
            received.add(message.headers());
            return CompletableFuture.completedFuture(null);
        });

        var headers = Map.of("key1", "value1", "key2", "value2");
        bus.publish(topic, new Message("body".getBytes(), headers)).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).hasSize(1));
        assertThat(received.get(0)).containsEntry("key1", "value1").containsEntry("key2", "value2");
    }

    @Test
    void invalidHeaderNameRejectedOnPublish() {
        Destination topic = provisionTopic("invalid-header");
        assertThatThrownBy(() ->
            bus.publish(topic, new Message(new byte[0], Map.of("bad header!", "value"))))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("bad header!");
    }

    @Test
    void reservedHeaderPrefixRejectedOnPublish() {
        Destination topic = provisionTopic("reserved-jms");
        assertThatThrownBy(() ->
            bus.publish(topic, new Message(new byte[0], Map.of("JMSCorrelationID", "value"))))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("JMS");
    }

    @Test
    void reservedMessagingPrefixRejectedOnPublish() {
        Destination topic = provisionTopic("reserved-messaging");
        assertThatThrownBy(() ->
            bus.publish(topic, new Message(new byte[0], Map.of("messaging.internal", "value"))))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("messaging.");
    }

    // ==================== Patterns ====================

    @Test
    void topicFanOut() {
        Destination topic = provisionTopic("fan-out");
        var receivedA = new CopyOnWriteArrayList<String>();
        var receivedB = new CopyOnWriteArrayList<String>();
        awaitSubscribed(topic, recordingHandler(receivedA));
        awaitSubscribed(topic, recordingHandler(receivedB));

        publish(topic, "fan-out-payload".getBytes());

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(receivedA).containsExactly("fan-out-payload");
            assertThat(receivedB).containsExactly("fan-out-payload");
        });
    }

    @Test
    void queueCompetingConsumers() {
        Destination queue = provisionQueue("competing");
        var received = new CopyOnWriteArrayList<String>();
        awaitSubscribed(queue, recordingHandler(received));
        awaitSubscribed(queue, recordingHandler(received));

        for (int i = 0; i < 4; i++) publish(queue, ("m" + i).getBytes());

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).hasSize(4));
        assertThat(received).containsExactlyInAnyOrder("m0", "m1", "m2", "m3");
    }

    // ==================== Settlement (§A) ====================

    @Test
    void redeliveryOnExceptionalFuture() {
        assertRedelivered(handlerThatFailsOnceThenSucceeds(
            (count, message) -> CompletableFuture.failedFuture(new RuntimeException("handler failed"))));
    }

    @Test
    void redeliveryOnSynchronousThrow() {
        assertRedelivered(handlerThatFailsOnceThenSucceeds((count, message) -> {
            throw new RuntimeException("sync fail");
        }));
    }

    @Test
    void redeliveryOnNullFuture() {
        assertRedelivered(handlerThatFailsOnceThenSucceeds((count, message) -> null));
    }

    @Test
    void redeliveryOnCancelledFuture() {
        assertRedelivered(handlerThatFailsOnceThenSucceeds((count, message) -> {
            var f = new CompletableFuture<Void>();
            f.cancel(true);
            return f;
        }));
    }

    private void assertRedelivered(MessageHandler handler) {
        Destination queue = provisionQueue("redeliver-" + System.nanoTime());
        awaitSubscribed(queue, handler);
        publish(queue, "payload".getBytes());
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(redeliverAttempts.get()).isGreaterThanOrEqualTo(2));
    }

    private final AtomicInteger redeliverAttempts = new AtomicInteger();

    private interface FailOnceBody {
        CompletableFuture<Void> onFirstAttempt(int attemptNumber, Message message);
    }

    private MessageHandler handlerThatFailsOnceThenSucceeds(FailOnceBody firstAttemptBehaviour) {
        redeliverAttempts.set(0);
        return message -> {
            int attempt = redeliverAttempts.incrementAndGet();
            if (attempt == 1) return firstAttemptBehaviour.onFirstAttempt(attempt, message);
            return CompletableFuture.completedFuture(null);
        };
    }

    @Test
    void successfulHandlerNotRedelivered() {
        Destination queue = provisionQueue("success-no-redeliver");
        var count = new AtomicInteger();
        awaitSubscribed(queue, message -> {
            count.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        publish(queue, "payload".getBytes());

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count.get()).isEqualTo(1));
        assertThatCode(() -> Thread.sleep(300)).doesNotThrowAnyException();
        assertThat(count.get()).isEqualTo(1);
    }

    // ==================== Lifecycle (§D, §J) ====================

    @Test
    void subscribeFutureMeansReady() {
        Destination topic = provisionTopic("subscribe-ready");
        var received = new CopyOnWriteArrayList<String>();
        bus.subscribe(topic, recordingHandler(received)).join();

        publish(topic, "immediate".getBytes());

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).containsExactly("immediate"));
    }

    @Test
    void subscriptionCloseStopsDelivery() throws Exception {
        Destination topic = provisionTopic("close-stop");
        var received = new CopyOnWriteArrayList<String>();
        Subscription subscription = bus.subscribe(topic, recordingHandler(received)).join();

        subscription.close();
        publish(topic, "after-close".getBytes());

        Thread.sleep(300);
        assertThat(received).isEmpty();
    }

    @Test
    void repeatedCloseIsIdempotent() {
        Destination topic = provisionTopic("repeated-close");
        Subscription subscription = bus.subscribe(topic,
            message -> CompletableFuture.completedFuture(null)).join();
        assertThatCode(() -> { subscription.close(); subscription.close(); })
            .doesNotThrowAnyException();
    }

    @Test
    void busCloseIsIdempotent() {
        assertThatCode(() -> { bus.close(); bus.close(); }).doesNotThrowAnyException();
    }

    @Test
    void apiCallAfterBusCloseThrows() {
        bus.close();
        assertThatThrownBy(() -> bus.publish(Topic.of("closed"), new byte[0]))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void subscriptionCloseWaitsForInFlightHandler() throws Exception {
        try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(2), 1))) {
            Destination queue = provisionQueue("close-waits-in-flight");
            var received = new CopyOnWriteArrayList<String>();
            var handlerStarted = new CompletableFuture<Void>();
            Subscription subscription = extraBus.subscribe(queue, message -> {
                handlerStarted.complete(null);
                return CompletableFuture.runAsync(() -> {
                    try { Thread.sleep(300); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                    received.add(new String(message.body()));
                });
            }).join();

            extraBus.publish(queue, "in-flight".getBytes()).join();
            handlerStarted.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            long startNanos = System.nanoTime();
            subscription.close();
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertThat(elapsedMs).isGreaterThanOrEqualTo(250);
            assertThat(received).containsExactly("in-flight");

            var redelivered = new CopyOnWriteArrayList<String>();
            extraBus.subscribe(queue, recordingHandler(redelivered)).join();
            Thread.sleep(300);
            assertThat(redelivered).isEmpty();
        }
    }

    @Test
    void subscriptionCloseCancelsStuckHandlerUnsettled() throws Exception {
        try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofMillis(500), 1))) {
            Destination queue = provisionQueue("close-cancels-stuck");
            var neverCompletes = new CompletableFuture<Void>();
            Subscription subscription = extraBus.subscribe(queue, message -> neverCompletes).join();

            extraBus.publish(queue, "stuck".getBytes()).join();
            Thread.sleep(100);

            long startNanos = System.nanoTime();
            subscription.close();
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertThat(elapsedMs).isLessThan(2500);
            assertThat(neverCompletes).isCancelled();

            var redelivered = new CopyOnWriteArrayList<String>();
            extraBus.subscribe(queue, recordingHandler(redelivered)).join();
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(redelivered).containsExactly("stuck"));
        }
    }

    @Test
    void busCloseCancelsStuckHandler() throws Exception {
        MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofMillis(500), 1));
        Destination queue = provisionQueue("bus-close-cancels-stuck");
        var neverCompletes = new CompletableFuture<Void>();
        extraBus.subscribe(queue, message -> neverCompletes).join();
        extraBus.publish(queue, "stuck".getBytes()).join();
        Thread.sleep(100);

        long startNanos = System.nanoTime();
        extraBus.close();
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertThat(elapsedMs).isBetween(400L, 2500L);
        assertThat(neverCompletes).isCancelled();
    }

    // ==================== Failure surfaces ====================

    @Test
    void publishToMissingTopicFails() {
        Topic topic = Topic.of("missing-topic-" + System.nanoTime());
        assertThatThrownBy(() -> bus.publish(topic, new byte[0]).join())
            .hasCauseInstanceOf(MessagingException.class);
    }

    @Test
    void publishToMissingQueueFails() {
        Queue queue = Queue.of("missing-queue-" + System.nanoTime());
        assertThatThrownBy(() -> bus.publish(queue, new byte[0]).join())
            .hasCauseInstanceOf(MessagingException.class);
    }

    @Test
    void subscribeToMissingTopicFails() {
        Topic topic = Topic.of("missing-topic-sub-" + System.nanoTime());
        assertThatThrownBy(() ->
            bus.subscribe(topic, message -> CompletableFuture.completedFuture(null)).join())
            .hasCauseInstanceOf(MessagingException.class);
    }

    @Test
    void subscribeToMissingQueueFails() {
        Queue queue = Queue.of("missing-queue-sub-" + System.nanoTime());
        assertThatThrownBy(() ->
            bus.subscribe(queue, message -> CompletableFuture.completedFuture(null)).join())
            .hasCauseInstanceOf(MessagingException.class);
    }

    @Test
    void listenerThrowDoesNotChangeDelivery() {
        MessagingListener throwing = new MessagingListener() {
            @Override public void onPublished(Destination d) { throw new RuntimeException("boom"); }
            @Override public void onConsumed(Destination d) { throw new RuntimeException("boom"); }
            @Override public void onError(Destination d, Throwable err) { throw new RuntimeException("boom"); }
            @Override public void onConnectionStateChanged(ConnectionState s) { throw new RuntimeException("boom"); }
        };
        try (MessageBus throwingBus = createBus(new BusSettings(throwing, Duration.ofSeconds(1), 1))) {
            Destination queue = provisionQueue("listener-throw");
            var received = new CopyOnWriteArrayList<String>();
            throwingBus.subscribe(queue, recordingHandler(received)).join();

            assertThatCode(() -> throwingBus.publish(queue, "m1".getBytes()).join()).doesNotThrowAnyException();
            assertThatCode(() -> throwingBus.publish(queue, "m2".getBytes()).join()).doesNotThrowAnyException();

            await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(received).containsExactlyInAnyOrder("m1", "m2"));
        }
    }

    // ==================== Ordering ====================

    @Test
    void concurrencyOnePreservesOrder() {
        Destination topic = provisionTopic("order-preserving");
        var received = new CopyOnWriteArrayList<String>();
        awaitSubscribed(topic, recordingHandler(received));

        for (int i = 0; i < 5; i++) publish(topic, ("msg-" + i).getBytes());

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).hasSize(5));
        assertThat(received).containsExactly("msg-0", "msg-1", "msg-2", "msg-3", "msg-4");
    }

    // ==================== Concurrency (§C) ====================

    @Test
    void topicConcurrencyDeliversOncePerSubscription() {
        try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(1), 2))) {
            Destination topic = provisionTopic("topic-concurrency");
            var received = new CopyOnWriteArrayList<String>();
            extraBus.subscribe(topic, recordingHandler(received)).join();

            for (int i = 0; i < 4; i++) extraBus.publish(topic, ("m" + i).getBytes()).join();

            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).hasSize(4));
            assertThat(received).containsExactlyInAnyOrder("m0", "m1", "m2", "m3");
        }
    }

    @Test
    void queueConcurrencyRunsUnitsInParallel() throws Exception {
        try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(2), 2))) {
            Destination queue = provisionQueue("queue-concurrency-parallel");
            var bothStarted = new java.util.concurrent.CountDownLatch(2);
            var releaseHandlers = new CompletableFuture<Void>();
            extraBus.subscribe(queue, message -> {
                bothStarted.countDown();
                return releaseHandlers;
            }).join();

            extraBus.publish(queue, "m1".getBytes()).join();
            extraBus.publish(queue, "m2".getBytes()).join();

            assertThat(bothStarted.await(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            releaseHandlers.complete(null);
        }
    }

    // ==================== Helpers ====================

    protected void publish(Destination destination, byte[] body) {
        bus.publish(destination, body).join();
    }

    protected void awaitSubscribed(Destination destination, MessageHandler handler) {
        bus.subscribe(destination, handler).join();
    }

    /** A handler that decodes the body as UTF-8 and appends it to {@code sink}, then settles. */
    protected static MessageHandler recordingHandler(CopyOnWriteArrayList<String> sink) {
        return message -> {
            sink.add(new String(message.body()));
            return CompletableFuture.completedFuture(null);
        };
    }
}
