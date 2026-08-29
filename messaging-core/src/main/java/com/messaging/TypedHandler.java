package com.messaging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface TypedHandler<T> {
    CompletableFuture<Void> handle(T message, Map<String, String> headers);
}
