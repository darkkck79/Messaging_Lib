package com.messaging;

import java.util.Map;
import java.util.Objects;

public record Message(byte[] body, Map<String, String> headers) {
    public Message {
        Objects.requireNonNull(body, "body must not be null");
        Objects.requireNonNull(headers, "headers must not be null");
        body = body.clone();
        headers = Map.copyOf(headers);
    }

    @Override
    public byte[] body() {
        return body.clone();
    }
}
