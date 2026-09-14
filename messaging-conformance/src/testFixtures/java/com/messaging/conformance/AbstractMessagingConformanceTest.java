package com.messaging.conformance;

import com.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
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

    // ==================== Failure surfaces ====================

    @Test
    void publishToMissingDestinationFails() {
        Topic topic = Topic.of("missing-dest-" + System.nanoTime());
        assertThatThrownBy(() -> bus.publish(topic, new byte[0]).join())
            .hasCauseInstanceOf(MessagingException.class);
    }

    @Test
    void subscribeToMissingDestinationFails() {
        Topic topic = Topic.of("missing-dest-sub-" + System.nanoTime());
        assertThatThrownBy(() ->
            bus.subscribe(topic, message -> CompletableFuture.completedFuture(null)).join())
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
