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

    @Test void headerBlockAtLimitPasses() {
        var headers = new HashMap<String, String>();
        String value = "x".repeat(8192);
        for (int i = 0; i < 7; i++) headers.put("key" + i, value);
        assertThatCode(() -> HeaderValidator.validateForPublish(headers)).doesNotThrowAnyException();
    }
}
