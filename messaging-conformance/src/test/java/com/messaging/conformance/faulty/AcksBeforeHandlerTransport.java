package com.messaging.conformance.faulty;

import com.messaging.Message;
import com.messaging.MessageHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Faulty variant: settles (dequeues) BEFORE running the handler, so a failing handler's
 * message is never redelivered — the message was already "acked" the moment it was
 * pulled off the deque. Violates §A. Used only by {@code AcksBeforeHandlerConformance},
 * excluded from normal test discovery, to prove {@code MetaConformanceTest} catches it.
 */
public final class AcksBeforeHandlerTransport extends InMemoryTransport {

    @Override
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler) {
        return new ConsumerUnit(deque, handler) {
            @Override
            protected void processOneMessage(Message message) {
                // Already removed from the deque by poll() in the base run() loop —
                // simply run the handler and never re-offer it, regardless of outcome.
                try {
                    CompletableFuture<Void> outcome = handler.handle(message);
                    if (outcome != null) outcome.exceptionally(ex -> null).join();
                } catch (Throwable ignored) {
                    // swallowed: this is the bug under test — no redelivery on failure
                }
            }
        };
    }
}
