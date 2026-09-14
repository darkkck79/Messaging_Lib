package com.messaging.jms;

import com.messaging.MessagingException;
import com.messaging.config.MessagingConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionFactoryBuilderTest {

    @Test void instantiatesByClassNameAndAppliesSetters() {
        var config = MessagingConfig.builder()
            .url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("brokerURL", "tcp://localhost:61616")
            .property("port", "61616")
            .property("timeout", "5000")
            .property("useSsl", "true")
            .build();

        var factory = (StubConnectionFactory) ConnectionFactoryBuilder.build(config);

        assertThat(factory.brokerUrl).isEqualTo("tcp://localhost:61616");
        assertThat(factory.port).isEqualTo(61616);
        assertThat(factory.timeout).isEqualTo(5000L);
        assertThat(factory.useSsl).isTrue();
    }

    @Test void missingConnectionFactoryKeyFails() {
        var config = MessagingConfig.builder().url("jms://localhost").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("connection-factory");
    }

    @Test void unknownClassNameFails() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", "com.example.DoesNotExist").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("com.example.DoesNotExist");
    }

    @Test void constructorThrowingClassFailsNamingKeyAndClass() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactoryThrowingConstructor.class.getName()).build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("connection-factory")
            .hasMessageContaining(StubConnectionFactoryThrowingConstructor.class.getName());
    }

    @Test void unknownSetterFailsNamingTheKey() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("noSuchProperty", "value").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("noSuchProperty");
    }

    @Test void deliveryModeNonPersistentIsRejectedBeforeSetterLookup() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("deliveryMode", "NON_PERSISTENT").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("deliveryMode")
            .hasMessageContaining("PERSISTENT");
    }

    @Test void deliveryModeNumericOneIsRejected() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("deliveryMode", "1").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("deliveryMode");
    }

    @Test void uncoercibleValueFailsNamingTheKey() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("port", "not-a-number").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("port");
    }
}
