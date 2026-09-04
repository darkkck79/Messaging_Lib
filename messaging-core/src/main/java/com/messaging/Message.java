package com.messaging;

import java.util.Arrays;
import java.util.Map;

public record Message(byte[] body, Map<String, String> headers) {
    public Message {
        body = body == null ? new byte[0] : body.clone();
        if (headers == null) {
            headers = Map.of();
        } else {
            for (var entry : headers.entrySet()) {
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                        "Header value must not be null for key: " + entry.getKey());
                }
            }
            headers = Map.copyOf(headers);
        }
    }

    @Override
    public byte[] body() {
        return body.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message other)) return false;
        return Arrays.equals(body, other.body) && headers.equals(other.headers);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(body) + headers.hashCode();
    }

    @Override
    public String toString() {
        return "Message[body=" + Arrays.toString(body) + ", headers=" + headers + "]";
    }
}
