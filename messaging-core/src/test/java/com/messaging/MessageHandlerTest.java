package com.messaging;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class MessageHandlerTest {

    @Test void isFunctionalInterface() {
        assertThat(MessageHandler.class.isInterface()).isTrue();
        assertThat(MessageHandler.class.getAnnotation(FunctionalInterface.class)).isNotNull();
    }

    @Test void lambdaReceivesMessageAndCompletesFuture() {
        AtomicReference<Message> received = new AtomicReference<>();
        MessageHandler handler = message -> {
            received.set(message);
            return CompletableFuture.completedFuture(null);
        };

        Message message = new Message("payload".getBytes(), Map.of("k", "v"));
        CompletableFuture<Void> result = handler.handle(message);

        assertThat(result).isCompleted();
        assertThat(received.get()).isEqualTo(message);
    }
}
