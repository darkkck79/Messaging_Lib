package com.messaging.internal;

import com.messaging.Message;
import com.messaging.MessagingException;
import com.messaging.Topic;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HeaderValidator {

    private static final Set<String> RESERVED_HEADERS = Set.of(
        "X-Messaging-Destination",
        "X-Messaging-Timestamp"
    );

    public void validate(Message message, Topic topic) {
        validateHeaders(message.headers(), topic.name());
    }

    private void validateHeaders(Map<String, String> headers, String destination) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isEmpty()) {
                throw new MessagingException("Header key must not be null or empty");
            }
            if (RESERVED_HEADERS.contains(key)) {
                throw new MessagingException("Reserved header not allowed: " + key);
            }
        }
    }
}
