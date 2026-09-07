package com.messaging.internal;

import com.messaging.MessagingException;

import java.util.Map;

/**
 * Validates headers for publish-time constraints.
 * Checks charset, reserved prefixes (JMS, JMSX, messaging.), and total header block size (64 KiB).
 */
public class HeaderValidator {

    /**
     * Validates headers for publish-time constraints.
     *
     * @param headers the headers to validate (unmodifiable map)
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

            if (!isValidCharset(key)) {
                throw new MessagingException("Header key outside allowed charset: " + key);
            }

            if (key.startsWith("messaging.")) {
                throw new MessagingException("Reserved header not allowed: " + key);
            }

            if (key.startsWith("JMS")) {
                throw new MessagingException("Reserved header not allowed: " + key);
            }
        }

        // Check total header block size (64 KiB cap)
        int totalSize = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            totalSize += entry.getKey().length() + entry.getValue().length();
        }

        if (totalSize > 64 * 1024) {
            throw new MessagingException("Header block exceeds 64 KiB limit");
        }
    }

    private static boolean isValidCharset(String key) {
        if (key == null) {
            return false;
        }
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_' && c != '.' && c != '-') {
                return false;
            }
        }
        return true;
    }
}
