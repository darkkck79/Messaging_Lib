package com.messaging.config;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

class MessagingConfigTest {

    private static Properties props(String... keyValues) {
        Properties p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            p.setProperty(keyValues[i], keyValues[i + 1]);
        }
        return p;
    }

    @Test void builderExposesUrlAsUri() {
        MessagingConfig config = MessagingConfig.builder().url("kafka://localhost:9092").build();

        assertThat(config.url()).isEqualTo(URI.create("kafka://localhost:9092"));
    }

    @Test void urlIsRequired() {
        assertThatThrownBy(() -> MessagingConfig.builder().build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("messaging.url");
    }

    @Test void urlMustHaveScheme() {
        assertThatThrownBy(() -> MessagingConfig.builder().url("localhost:9092").build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void defaultsMatchSpec() {
        MessagingConfig config = MessagingConfig.builder().url("kafka://localhost:9092").build();

        assertThat(config.concurrency()).isEqualTo(1);
        assertThat(config.connectTimeout()).isEqualTo(Duration.parse("PT10S"));
        assertThat(config.closeTimeout()).isEqualTo(Duration.parse("PT30S"));
        assertThat(config.clientId()).isNotBlank();
    }

    @Test void generatedClientIdsAreUnique() {
        MessagingConfig a = MessagingConfig.builder().url("kafka://h:1").build();
        MessagingConfig b = MessagingConfig.builder().url("kafka://h:1").build();

        assertThat(a.clientId()).isNotEqualTo(b.clientId());
    }

    @Test void builderOverridesDefaults() {
        MessagingConfig config = MessagingConfig.builder()
            .url("kafka://localhost:9092")
            .clientId("orders-app")
            .concurrency(4)
            .connectTimeout(Duration.ofSeconds(5))
            .closeTimeout(Duration.ofSeconds(1))
            .build();

        assertThat(config.clientId()).isEqualTo("orders-app");
        assertThat(config.concurrency()).isEqualTo(4);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test void concurrencyMustBePositive() {
        assertThatThrownBy(() -> MessagingConfig.builder().url("kafka://h:1").concurrency(0).build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void fromPropertiesReadsCoreKeys() {
        MessagingConfig config = MessagingConfig.fromProperties(props(
            "messaging.url", "kafka://localhost:9092",
            "messaging.client-id", "from-props",
            "messaging.consumer.concurrency", "3",
            "messaging.connect-timeout", "PT2S",
            "messaging.close-timeout", "PT4S")).build();

        assertThat(config.url()).isEqualTo(URI.create("kafka://localhost:9092"));
        assertThat(config.clientId()).isEqualTo("from-props");
        assertThat(config.concurrency()).isEqualTo(3);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(4));
    }

    @Test void builderValuesTakePrecedenceOverProperties() {
        MessagingConfig config = MessagingConfig.fromProperties(props(
                "messaging.url", "kafka://localhost:9092",
                "messaging.client-id", "from-props"))
            .clientId("explicit")
            .build();

        assertThat(config.clientId()).isEqualTo("explicit");
    }

    @Test void unknownCoreKeyFailsFast() {
        assertThatThrownBy(() -> MessagingConfig.fromProperties(props(
            "messaging.url", "kafka://localhost:9092",
            "messaging.conurrency", "2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("messaging.conurrency");
    }

    @Test void schemePrefixedKeysPassThroughWithPrefixStripped() {
        MessagingConfig config = MessagingConfig.fromProperties(props(
            "messaging.url", "kafka://localhost:9092",
            "messaging.kafka.acks", "all",
            "messaging.kafka.max.poll.records", "10")).build();

        assertThat(config.transportProperties())
            .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                "acks", "all",
                "max.poll.records", "10"));
    }

    @Test void otherSchemePrefixedKeysAreIgnored() {
        MessagingConfig config = MessagingConfig.fromProperties(props(
            "messaging.url", "kafka://localhost:9092",
            "messaging.jms.some.key", "value")).build();

        assertThat(config.transportProperties()).isEmpty();
    }

    @Test void nonMessagingKeysAreIgnored() {
        MessagingConfig config = MessagingConfig.fromProperties(props(
            "messaging.url", "kafka://localhost:9092",
            "java.version", "21")).build();

        assertThat(config.transportProperties()).isEmpty();
    }

    @Test void builderPropertyAddsTransportProperty() {
        MessagingConfig config = MessagingConfig.builder()
            .url("kafka://localhost:9092")
            .property("acks", "all")
            .build();

        assertThat(config.transportProperties()).containsEntry("acks", "all");
    }

    @Test void transportPropertiesAreUnmodifiable() {
        MessagingConfig config = MessagingConfig.builder().url("kafka://h:1").build();

        assertThatThrownBy(() -> config.transportProperties().put("k", "v"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void malformedDurationIsRejected() {
        assertThatThrownBy(() -> MessagingConfig.fromProperties(props(
            "messaging.url", "kafka://localhost:9092",
            "messaging.connect-timeout", "10s")))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void toStringRedactsUriUserinfo() {
        MessagingConfig config = MessagingConfig.builder()
            .url("jms://admin:s3cret@broker:61616")
            .build();

        assertThat(config.toString())
            .doesNotContain("s3cret")
            .doesNotContain("admin")
            .contains("broker:61616");
    }

    @Test void toStringRedactsPasswordAndSecretProperties() {
        MessagingConfig config = MessagingConfig.builder()
            .url("kafka://localhost:9092")
            .property("sasl.password", "hunter2")
            .property("client.secret.key", "topsecret")
            .property("acks", "all")
            .build();

        assertThat(config.toString())
            .doesNotContain("hunter2")
            .doesNotContain("topsecret")
            .contains("acks=all");
    }
}
