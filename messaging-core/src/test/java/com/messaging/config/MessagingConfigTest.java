package com.messaging.config;

import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Properties;
import static org.assertj.core.api.Assertions.*;

class MessagingConfigTest {

    @Test void builderMinimal() {
        var config = MessagingConfig.builder().url("kafka://localhost:9092").build();
        assertThat(config.scheme()).isEqualTo("kafka");
        assertThat(config.url().getHost()).isEqualTo("localhost");
        assertThat(config.url().getPort()).isEqualTo(9092);
    }

    @Test void builderDefaults() {
        var config = MessagingConfig.builder().url("kafka://localhost:9092").build();
        assertThat(config.concurrency()).isEqualTo(1);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.clientId()).isNotBlank();
        assertThat(config.listener()).isNotNull();
    }

    @Test void builderCustomValues() {
        var listener = new MessagingListener() {};
        var config = MessagingConfig.builder()
            .url("jms://broker:61616").clientId("my-app").concurrency(4)
            .connectTimeout(Duration.ofSeconds(5)).closeTimeout(Duration.ofSeconds(60))
            .listener(listener).build();
        assertThat(config.scheme()).isEqualTo("jms");
        assertThat(config.clientId()).isEqualTo("my-app");
        assertThat(config.concurrency()).isEqualTo(4);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(config.listener()).isSameAs(listener);
    }

    @Test void builderRequiresUrl() {
        assertThatThrownBy(() -> MessagingConfig.builder().build())
            .isInstanceOf(MessagingException.class).hasMessageContaining("url");
    }

    @Test void fromProperties() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.client-id", "test-app");
        props.setProperty("messaging.consumer.concurrency", "2");
        props.setProperty("messaging.connect-timeout", "PT5S");
        props.setProperty("messaging.close-timeout", "PT60S");
        var config = MessagingConfig.fromProperties(props);
        assertThat(config.scheme()).isEqualTo("kafka");
        assertThat(config.clientId()).isEqualTo("test-app");
        assertThat(config.concurrency()).isEqualTo(2);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test void fromPropertiesRequiresUrl() {
        assertThatThrownBy(() -> MessagingConfig.fromProperties(new Properties()))
            .isInstanceOf(MessagingException.class).hasMessageContaining("messaging.url");
    }

    @Test void unknownCoreKeyFailsFast() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.typo-key", "value");
        assertThatThrownBy(() -> MessagingConfig.fromProperties(props))
            .isInstanceOf(MessagingException.class).hasMessageContaining("messaging.typo-key");
    }

    @Test void passthroughProperties() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.kafka.batch.size", "16384");
        props.setProperty("messaging.kafka.linger.ms", "5");
        var config = MessagingConfig.fromProperties(props);
        assertThat(config.passthroughProperties())
            .containsEntry("batch.size", "16384")
            .containsEntry("linger.ms", "5");
    }

    @Test void toStringRedactsPassword() {
        var config = MessagingConfig.builder()
            .url("kafka://user:secret@localhost:9092")
            .property("password", "hunter2")
            .property("ssl.truststore.password", "trust-pass").build();
        String str = config.toString();
        assertThat(str).doesNotContain("secret", "hunter2", "trust-pass");
        assertThat(str).contains("***");
    }

    @Test void invalidDurationRejects() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.connect-timeout", "30seconds");
        assertThatThrownBy(() -> MessagingConfig.fromProperties(props))
            .isInstanceOf(MessagingException.class).hasMessageContaining("connect-timeout");
    }

    @Test void concurrencyMustBePositive() {
        assertThatThrownBy(() -> MessagingConfig.builder()
            .url("kafka://localhost:9092").concurrency(0).build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void builderPassthroughProperties() {
        var config = MessagingConfig.builder()
            .url("kafka://localhost:9092").property("batch.size", "32768").build();
        assertThat(config.passthroughProperties()).containsEntry("batch.size", "32768");
    }
}
