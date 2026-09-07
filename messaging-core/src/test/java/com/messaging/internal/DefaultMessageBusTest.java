package com.messaging.internal;

import com.messaging.Destination;
import com.messaging.MessageBus;
import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import com.messaging.Topic;
import com.messaging.config.MessagingConfig;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DefaultMessageBus must run outgoing messages through HeaderValidator before handing
 * them to the transport, per §the header-validation contract already proven in
 * HeaderValidatorTest. This exercises that wiring end-to-end through publish().
 */
class DefaultMessageBusTest {

    @Test void publishRejectsHeaderKeyOutsideAllowedCharset() {
        MessagingConfig config = MessagingConfig.builder().url("fake://localhost").build();

        try (MessageBus bus = new DefaultMessageBus(config)) {
            assertThatThrownBy(() ->
                bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of("bad key!", "v")))
                .isInstanceOf(MessagingException.class)
                .hasMessageContaining("bad key!");
        }
    }

    @Test void publishNotifiesConfiguredListenerOnPublished() {
        AtomicReference<Destination> notified = new AtomicReference<>();
        MessagingListener listener = new MessagingListener() {
            @Override public void onPublished(Destination destination) {
                notified.set(destination);
            }
        };
        MessagingConfig config = MessagingConfig.builder()
            .url("fake://localhost")
            .listener(listener)
            .build();

        try (MessageBus bus = new DefaultMessageBus(config)) {
            bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of());
        }

        assertThat(notified.get()).isEqualTo(Topic.of("orders"));
    }

    @Test void handlerFailureIsIsolatedAndReportedToListenerWithoutBlockingOtherSubscribers() {
        AtomicReference<Throwable> reportedError = new AtomicReference<>();
        AtomicReference<Destination> reportedDestination = new AtomicReference<>();
        MessagingListener listener = new MessagingListener() {
            @Override public void onError(Destination destination, Throwable error) {
                reportedDestination.set(destination);
                reportedError.set(error);
            }
        };
        MessagingConfig config = MessagingConfig.builder()
            .url("fake://localhost")
            .listener(listener)
            .build();
        List<String> receivedBySecondHandler = new CopyOnWriteArrayList<>();
        RuntimeException boom = new RuntimeException("boom");

        try (MessageBus bus = new DefaultMessageBus(config)) {
            try (var first = bus.subscribe(Topic.of("orders"), message -> { throw boom; }, null);
                 var second = bus.subscribe(Topic.of("orders"), message -> {
                     receivedBySecondHandler.add(new String(message.body()));
                     return CompletableFuture.completedFuture(null);
                 }, null)) {

                assertThatCode(() -> bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of()))
                    .doesNotThrowAnyException();
            }
        }

        assertThat(reportedDestination.get()).isEqualTo(Topic.of("orders"));
        assertThat(reportedError.get()).isEqualTo(boom);
        assertThat(receivedBySecondHandler).containsExactly("hello");
    }

    /**
     * Per design spec §H: "onConsumed fires after settlement, so it means
     * 'processed and settled', not 'received'." DefaultMessageBus must notify the
     * configured listener once a subscriber's handler successfully completes.
     */
    @Test void successfulHandlerSettlementNotifiesListenerOnConsumed() {
        AtomicReference<Destination> notified = new AtomicReference<>();
        MessagingListener listener = new MessagingListener() {
            @Override public void onConsumed(Destination destination) {
                notified.set(destination);
            }
        };
        MessagingConfig config = MessagingConfig.builder()
            .url("fake://localhost")
            .listener(listener)
            .build();

        try (MessageBus bus = new DefaultMessageBus(config)) {
            try (var subscription = bus.subscribe(Topic.of("orders"), message ->
                    CompletableFuture.completedFuture(null), null)) {
                bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of());
            }
        }

        assertThat(notified.get()).isEqualTo(Topic.of("orders"));
    }
}
