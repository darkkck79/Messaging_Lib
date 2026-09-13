package com.messaging;

import com.messaging.config.MessagingConfig;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MessagingFactoryTest {
    @Test void noProviderForSchemeThrows() {
        var config = MessagingConfig.builder().url("nonexistent://localhost:1234").build();
        assertThatThrownBy(() -> Messaging.connect(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("nonexistent").hasMessageContaining("No TransportProvider");
    }
}
