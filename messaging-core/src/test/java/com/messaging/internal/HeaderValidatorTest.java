package com.messaging.internal;

import com.messaging.Message;
import com.messaging.MessagingException;
import com.messaging.Topic;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class HeaderValidatorTest {

    private final HeaderValidator validator = new HeaderValidator();
    private final Topic topic = Topic.of("orders");

    @Test void rejectsHeaderKeyOutsideAllowedCharset() {
        Message message = new Message(new byte[0], Map.of("bad key!", "v"));

        assertThatThrownBy(() -> validator.validate(message, topic))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("bad key!");
    }

    @Test void rejectsMessagingReservedPrefix() {
        Message message = new Message(new byte[0], Map.of("messaging.custom", "v"));

        assertThatThrownBy(() -> validator.validate(message, topic))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("messaging.custom");
    }

    @Test void rejectsJmsReservedPrefix() {
        Message message = new Message(new byte[0], Map.of("JMSXGroupID", "v"));

        assertThatThrownBy(() -> validator.validate(message, topic))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("JMSXGroupID");
    }

    @Test void rejectsHeaderBlockOver64KiB() {
        Map<String, String> headers = new HashMap<>();
        headers.put("k", "v".repeat(70 * 1024));
        Message message = new Message(new byte[0], headers);

        assertThatThrownBy(() -> validator.validate(message, topic))
            .isInstanceOf(MessagingException.class);
    }
}
