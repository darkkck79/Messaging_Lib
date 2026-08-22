package com.messaging;

public sealed interface Destination permits Topic, Queue {
    String name();
}
