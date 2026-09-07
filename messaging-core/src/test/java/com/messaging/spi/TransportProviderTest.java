package com.messaging.spi;

import com.messaging.config.MessagingConfig;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TransportProvider.lookup must select the provider matching the connection URL's
 * scheme (e.g. "kafka://..." selects the provider whose supports("kafka") is true),
 * not merely the first provider ServiceLoader happens to enumerate. This matters as
 * soon as more than one TransportProvider is on the classpath — exactly the situation
 * messaging-jms and messaging-kafka create together.
 *
 * <p>Registered via META-INF/services/com.messaging.spi.TransportProvider, listed
 * *before* {@link SchemeBProvider} so a naive "just take the first one" lookup
 * deterministically returns the wrong provider for a "scheme-b://" URL.
 */
class TransportProviderTest {

    @Test void lookupSelectsProviderMatchingConfigScheme() {
        MessagingConfig config = MessagingConfig.builder().url("scheme-b://localhost").build();

        TransportProvider provider = TransportProvider.lookup(config);

        assertThat(provider).isInstanceOf(SchemeBProvider.class);
    }

    @Test void lookupSelectsOtherProviderForOtherScheme() {
        MessagingConfig config = MessagingConfig.builder().url("scheme-a://localhost").build();

        TransportProvider provider = TransportProvider.lookup(config);

        assertThat(provider).isInstanceOf(SchemeAProvider.class);
    }

    public static final class SchemeAProvider implements TransportProvider {
        @Override public Transport create(String destinationName, MessagingConfig config) {
            return new NoOpTransport();
        }
        @Override public boolean supports(String providerName) {
            return "scheme-a".equals(providerName);
        }
    }

    public static final class SchemeBProvider implements TransportProvider {
        @Override public Transport create(String destinationName, MessagingConfig config) {
            return new NoOpTransport();
        }
        @Override public boolean supports(String providerName) {
            return "scheme-b".equals(providerName);
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
