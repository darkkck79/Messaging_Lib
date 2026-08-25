package com.messaging;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class MessageTest {

    @Test void accessorsReturnConstructedValues() {
        byte[] body = {1, 2, 3};
        Map<String, String> headers = Map.of("k", "v");

        Message message = new Message(body, headers);

        assertThat(message.body()).containsExactly(1, 2, 3);
        assertThat(message.headers()).containsExactly(Map.entry("k", "v"));
    }

    @Test void bodyIsDefensivelyCopiedOnConstruction() {
        byte[] original = {1, 2, 3};
        Message message = new Message(original, Map.of());

        original[0] = 99;

        assertThat(message.body()).containsExactly(1, 2, 3);
    }

    @Test void headersAreCopiedOnConstruction() {
        Map<String, String> original = new HashMap<>();
        original.put("k", "v");
        Message message = new Message(new byte[0], original);

        original.put("k", "changed");

        assertThat(message.headers()).containsExactly(Map.entry("k", "v"));
    }

    @Test void headersAreUnmodifiable() {
        Message message = new Message(new byte[0], Map.of("k", "v"));

        assertThatThrownBy(() -> message.headers().put("k2", "v2"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void rejectsNullBody() {
        assertThatThrownBy(() -> new Message(null, Map.of()))
            .isInstanceOf(NullPointerException.class);
    }

    @Test void rejectsNullHeaders() {
        assertThatThrownBy(() -> new Message(new byte[0], null))
            .isInstanceOf(NullPointerException.class);
    }
}
