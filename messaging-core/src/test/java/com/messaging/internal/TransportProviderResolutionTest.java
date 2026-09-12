package com.messaging.internal;

import com.messaging.MessageBus;
import com.messaging.Messaging;
import com.messaging.Queue;
import com.messaging.Topic;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Messaging.connect(config) already resolves the TransportProvider eagerly so that a
 * missing provider fails fast (see MessagingFactoryTest#noProviderForSchemeThrows).
 * DefaultMessageBus must reuse that resolved provider rather than re-running the
 * ServiceLoader classpath scan on every new destination — repeating a full SPI lookup
 * per destination is wasted work the connect()-time lookup was meant to make unnecessary.
 *
 * <p>Registered via META-INF/services/com.messaging.spi.TransportProvider.
 */
class TransportProviderResolutionTest {

    @Test void providerIsResolvedAtMostOnceAcrossConnectAndMultiplePublishes() {
        MessagingConfig config = MessagingConfig.builder().url("counting://localhost").build();
        CountingTransportProvider.INSTANTIATIONS.set(0);

        try (MessageBus bus = Messaging.connect(config)) {
            bus.publish(Topic.of("orders"), "hello".getBytes(), Map.of());
            bus.publish(Queue.of("payments"), "hello".getBytes(), Map.of());
        }

        assertThat(CountingTransportProvider.INSTANTIATIONS.get()).isEqualTo(1);
    }

    public static final class CountingTransportProvider implements TransportProvider {
        static final AtomicInteger INSTANTIATIONS = new AtomicInteger();

        public CountingTransportProvider() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Override public Transport create(String destinationName, MessagingConfig config) {
            return new NoOpTransport();
        }

        @Override public boolean supports(String providerName) {
            return "counting".equals(providerName);
        }
    }

    private static final class NoOpTransport implements Transport {
        @Override public void publish(String destinationName, byte[] body, Map<String, String> headers) {}
        @Override public <T> SubscriptionImpl subscribe(
                String destinationName, com.messaging.MessageHandler handler, com.messaging.TypedHandler<T> typedHandler) {
            return () -> {};
        }
        @Override public void close() {}
    }
}
