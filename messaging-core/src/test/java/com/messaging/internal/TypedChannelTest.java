package com.messaging.internal;

import com.messaging.Codec;
import com.messaging.MessageBus;
import com.messaging.MessagingException;
import com.messaging.Topic;
import com.messaging.TypedChannel;
import com.messaging.config.MessagingConfig;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Typed publish must run outgoing headers through the same HeaderValidator gate that
 * plain byte[] publish already uses (see HeaderValidatorTest / DefaultMessageBusTest) —
 * a caller should not be able to smuggle an invalid header past validation just by
 * going through the typed channel instead of the raw publish() overload.
 */
class TypedChannelTest {

    private static final Codec<String> STRING_CODEC = new Codec<>() {
        @Override public byte[] encode(String value) { return value.getBytes(StandardCharsets.UTF_8); }
        @Override public String decode(byte[] bytes) { return new String(bytes, StandardCharsets.UTF_8); }
    };

    @Test void typedPublishRejectsHeaderKeyOutsideAllowedCharset() {
        MessagingConfig config = MessagingConfig.builder().url("fake://localhost").build();

        try (MessageBus bus = new DefaultMessageBus(config)) {
            TypedChannel<String> channel = bus.typed(Topic.of("orders"), STRING_CODEC);

            assertThatThrownBy(() -> channel.publish("hello", Map.of("bad key!", "v")))
                .isInstanceOf(MessagingException.class)
                .hasMessageContaining("bad key!");
        }
    }
}
