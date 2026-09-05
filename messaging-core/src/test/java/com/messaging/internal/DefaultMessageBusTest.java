package com.messaging.internal;

import com.messaging.MessageBus;
import com.messaging.MessagingException;
import com.messaging.Topic;
import com.messaging.config.MessagingConfig;

import org.junit.jupiter.api.Test;

import java.util.Map;

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
}
