package com.messaging;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface MessageHandler {
    CompletableFuture<Void> handle(Message message);
}
