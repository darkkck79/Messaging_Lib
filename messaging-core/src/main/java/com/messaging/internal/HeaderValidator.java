package com.messaging.internal;

import com.messaging.MessagingException;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Validates headers for publish-time constraints.
 * Checks charset, reserved prefixes (JMS, JMSX, messaging.), and total header block size (64 KiB).
 */
public class HeaderValidator {

    private static final Pattern VALID_KEY = Pattern.compile("[A-Za-z0-9_.\\-]+");
    private static final int MAX_HEADER_BLOCK_BYTES = 64 * 1024;

    /**
     * Validates headers for publish-time constraints.
     *
     * @param headers the headers to validate
     * @throws MessagingException if any header violates constraints
     */
    public static void validateForPublish(Map<String, String> headers) {
        if (headers == null) {
            throw new MessagingException("Headers must not be null");
        }

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key == null || key.isEmpty()) {
                throw new MessagingException("Header key must not be null or empty");
            }

            if (!VALID_KEY.matcher(key).matches()) {
                throw new MessagingException("Invalid header key charset (must match [A-Za-z0-9_.-]+): " + key);
            }

            if (key.startsWith("messaging.")) {
                throw new MessagingException("Reserved header prefix 'messaging.' on key: " + key);
            }

            if (key.startsWith("JMSX")) {
                throw new MessagingException("Reserved header prefix 'JMSX' on key: " + key);
            }

            if (key.startsWith("JMS")) {
                throw new MessagingException("Reserved header prefix 'JMS' on key: " + key);
            }
        }

        // Check total header block size (64 KiB cap) - measured in UTF-8 bytes
        int totalBytes = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            totalBytes += entry.getKey().getBytes(StandardCharsets.UTF_8).length
                        + entry.getValue().getBytes(StandardCharsets.UTF_8).length;
        }

        if (totalBytes > MAX_HEADER_BLOCK_BYTES) {
            throw new MessagingException("Header block exceeds 64 KiB limit");
        }
    }
}
