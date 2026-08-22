package com.messaging;

import java.util.Objects;

public record Topic(String name) implements Destination {
    public Topic {
        Objects.requireNonNull(name, "Topic name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be blank");
        }
    }

    public static Topic of(String name) {
        return new Topic(name);
    }
}
