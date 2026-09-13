package com.messaging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class TypedChannel<T> {
    private final MessageBus bus;
    private final Destination destination;
    private final Codec<T> codec;

    public TypedChannel(MessageBus bus, Destination destination, Codec<T> codec) {
        this.bus = bus; this.destination = destination; this.codec = codec;
    }

    public CompletableFuture<Void> publish(T value) { return publish(value, Map.of()); }

    public CompletableFuture<Void> publish(T value, Map<String, String> headers) {
        return bus.publish(destination, new Message(codec.encode(value), headers));
    }

    public CompletableFuture<Subscription> subscribe(TypedHandler<T> handler) {
        return bus.subscribe(destination, message -> {
            T value = codec.decode(message.body());
            return handler.handle(value, message.headers());
        });
    }
}
