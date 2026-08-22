package com.messaging;

import java.util.Objects;

public record Queue(String name) implements Destination {
    public Queue {
        Objects.requireNonNull(name, "Queue name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Queue name must not be blank");
        }
    }

    public static Queue of(String name) {
        return new Queue(name);
    }
}
