package com.messaging.internal;

import com.messaging.MessagingException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class HeaderValidatorTest {

    @Test void validHeadersPass() {
        assertThatCode(() -> HeaderValidator.validateForPublish(
            Map.of("my-key", "value", "X.Custom.1", "data"))).doesNotThrowAnyException();
    }

    @Test void emptyHeadersPass() {
        assertThatCode(() -> HeaderValidator.validateForPublish(Map.of())).doesNotThrowAnyException();
    }

    @Test void invalidKeyCharsetRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("bad key!", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("bad key!");
    }

    @Test void reservedPrefixJmsRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("JMSCorrelationID", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("JMS");
    }

    @Test void reservedPrefixJmsxRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("JMSXGroupID", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("JMSX");
    }

    @Test void reservedPrefixMessagingRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("messaging.internal", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("messaging.");
    }

    @Test void headerBlockSizeExceeded() {
        var headers = new HashMap<String, String>();
        String bigValue = "x".repeat(8192);
        for (int i = 0; i < 9; i++) headers.put("key" + i, bigValue);
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(headers))
            .isInstanceOf(MessagingException.class).hasMessageContaining("64 KiB");
    }

    @Test void headerBlockSizeMeasuredInUtf8Bytes() {
        // 30_000 '€' chars = 90_000 UTF-8 bytes, but only 30_000 chars.
        // The spec caps the block at 64 KiB *on the wire*, so this must be rejected.
        var headers = Map.of("big", "€".repeat(30_000));
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(headers))
            .isInstanceOf(MessagingException.class).hasMessageContaining("64 KiB");
    }

    @Test void nonAsciiKeyRejected() {
        // Spec charset is [A-Za-z0-9_.-]+ — Unicode letters are outside it.
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("kéy", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("kéy");
    }

    @Test void headerBlockAtLimitPasses() {
        var headers = new HashMap<String, String>();
        String value = "x".repeat(8192);
        for (int i = 0; i < 7; i++) headers.put("key" + i, value);
        assertThatCode(() -> HeaderValidator.validateForPublish(headers)).doesNotThrowAnyException();
    }
}
