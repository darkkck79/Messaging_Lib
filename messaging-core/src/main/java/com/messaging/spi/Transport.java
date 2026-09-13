package com.messaging.spi;

import com.messaging.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface Transport extends AutoCloseable {
    CompletableFuture<Void> publish(Destination destination, Message message);
    CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler);
    void close(Duration timeout);
    @Override default void close() { close(Duration.ofSeconds(30)); }
}
