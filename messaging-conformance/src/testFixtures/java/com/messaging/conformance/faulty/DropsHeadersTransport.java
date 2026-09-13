package com.messaging.conformance.faulty;

import com.messaging.Message;
import com.messaging.MessageHandler;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Faulty variant: strips headers before handing the message to the handler. Violates
 * §G (headers round-trip). Used only by {@code DropsHeadersConformance} to prove the
 * suite catches it.
 */
public final class DropsHeadersTransport extends InMemoryTransport {

    @Override
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler) {
        MessageHandler strippingHandler = message ->
            handler.handle(new Message(message.body(), Map.of()));
        return new ConsumerUnit(deque, strippingHandler);
    }
}
