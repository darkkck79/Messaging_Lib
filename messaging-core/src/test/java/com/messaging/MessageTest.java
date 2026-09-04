package com.messaging;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class MessageTest {

    @Test void bodyRoundTrip() {
        byte[] data = "hello".getBytes();
        Message msg = new Message(data, Map.of());
        assertThat(msg.body()).isEqualTo(data);
    }

    @Test void bodyDefensiveCopyOnConstruction() {
        byte[] data = "hello".getBytes();
        Message msg = new Message(data, Map.of());
        data[0] = 'X';
        assertThat(msg.body()).isEqualTo("hello".getBytes());
    }

    @Test void bodyDefensiveCopyOnAccess() {
        Message msg = new Message("hello".getBytes(), Map.of());
        byte[] first = msg.body();
        first[0] = 'X';
        assertThat(msg.body()).isEqualTo("hello".getBytes());
    }

    @Test void nullBodyBecomesEmpty() {
        Message msg = new Message(null, Map.of());
        assertThat(msg.body()).isEmpty();
    }

    @Test void headersRoundTrip() {
        Message msg = new Message(new byte[0], Map.of("key", "value"));
        assertThat(msg.headers()).containsEntry("key", "value");
    }

    @Test void headersUnmodifiable() {
        Message msg = new Message(new byte[0], Map.of("key", "value"));
        assertThatThrownBy(() -> msg.headers().put("new", "val"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void headersDefensiveCopyOnConstruction() {
        var headers = new HashMap<String, String>();
        headers.put("key", "value");
        Message msg = new Message(new byte[0], headers);
        headers.put("injected", "bad");
        assertThat(msg.headers()).doesNotContainKey("injected");
    }

    @Test void nullHeadersBecomesEmpty() {
        Message msg = new Message(new byte[0], null);
        assertThat(msg.headers()).isEmpty();
    }

    @Test void nullHeaderValueRejected() {
        var headers = new HashMap<String, String>();
        headers.put("key", null);
        assertThatThrownBy(() -> new Message(new byte[0], headers))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("key");
    }

    @Test void emptyHeaderValueAllowed() {
        Message msg = new Message(new byte[0], Map.of("key", ""));
        assertThat(msg.headers()).containsEntry("key", "");
    }

    @Test void equalityUsesArrayContents() {
        Message a = new Message("hello".getBytes(), Map.of("k", "v"));
        Message b = new Message("hello".getBytes(), Map.of("k", "v"));
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test void inequalityOnDifferentBody() {
        Message a = new Message("hello".getBytes(), Map.of());
        Message b = new Message("world".getBytes(), Map.of());
        assertThat(a).isNotEqualTo(b);
    }
}
