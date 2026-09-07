package com.messaging;

import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

/**
 * Messaging.connect(config) must hand back a MessageBus that is immediately usable —
 * a caller should not have to perform a separate "connect" step before publishing.
 *
 * <p>Registered via META-INF/services/com.messaging.spi.TransportProvider so the SPI
 * lookup in {@link TransportProvider#lookup} resolves {@link FakeTransportProvider}.
 */
class MessagingFactoryTest {

    @Test void connectReturnsAnImmediatelyUsableBus() {
        MessagingConfig config = MessagingConfig.builder().url("fake://localhost").build();

        try (MessageBus bus = Messaging.connect(config)) {
            assertThatCode(() -> bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of()))
                .doesNotThrowAnyException();
        }
    }

    @Test void connectIsolatesExceptionsThrownByTheConfiguredListener() {
        MessagingConfig config = MessagingConfig.builder()
            .url("fake://localhost")
            .listener(new MessagingListener() {
                @Override
                public void onPublished(Destination destination) {
                    throw new RuntimeException("boom");
                }
            })
            .build();

        try (MessageBus bus = Messaging.connect(config)) {
            assertThatCode(() -> bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of()))
                .doesNotThrowAnyException();
        }
    }

    @Test void connectedBusDeliversPublishedMessagesToSubscribers() {
        MessagingConfig config = MessagingConfig.builder().url("fake://localhost").build();
        var received = new CopyOnWriteArrayList<String>();

        try (MessageBus bus = Messaging.connect(config)) {
            try (Subscription ignored = bus.subscribe(Topic.of("orders"), message -> {
                received.add(new String(message.body()));
                return CompletableFuture.completedFuture(null);
            }, null)) {
                bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of());

                await().untilAsserted(() -> assertThat(received).containsExactly("hello"));
            }
        }
    }

    public static final class FakeTransportProvider implements TransportProvider {
        @Override
        public Transport create(String destinationName, MessagingConfig config) {
            return new FakeTransport();
        }

        @Override
        public boolean supports(String providerName) {
            return "fake".equals(providerName);
        }
    }

    private static final class FakeTransport implements Transport {
        private final CopyOnWriteArrayList<MessageHandler> handlers = new CopyOnWriteArrayList<>();

        @Override
        public void publish(String destinationName, byte[] body, Map<String, String> headers) {
            Message message = new Message(body, headers);
            for (MessageHandler handler : handlers) {
                handler.handle(message);
            }
        }

        @Override
        public <T> SubscriptionImpl subscribe(
                String destinationName, MessageHandler handler, TypedHandler<T> typedHandler) {
            handlers.add(handler);
            return () -> handlers.remove(handler);
        }

        @Override
        public void close() {
            handlers.clear();
        }
    }
}
