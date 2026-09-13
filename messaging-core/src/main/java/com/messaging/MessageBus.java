package com.messaging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface MessageBus extends AutoCloseable {
    CompletableFuture<Void> publish(Destination destination, Message message);
    default CompletableFuture<Void> publish(Destination destination, byte[] body) {
        return publish(destination, new Message(body, Map.of()));
    }
    CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler);
    <T> TypedChannel<T> typed(Destination destination, Codec<T> codec);
    @Override void close();
}
