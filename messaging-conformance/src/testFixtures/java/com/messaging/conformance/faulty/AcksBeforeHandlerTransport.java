package com.messaging.conformance.faulty;

import com.messaging.Destination;
import com.messaging.Message;
import com.messaging.MessageHandler;
import com.messaging.MessagingListener;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Faulty variant: never re-offers a failed message, regardless of outcome — as if it had
 * already been acked at dequeue time, before the handler ever ran. Violates §A. Used only
 * by {@code AcksBeforeHandlerConformance} to prove the suite catches it.
 */
public final class AcksBeforeHandlerTransport extends InMemoryTransport {

    public AcksBeforeHandlerTransport(MessagingListener listener, java.time.Duration closeTimeout, int concurrency) {
        super(listener, closeTimeout, concurrency);
    }

    @Override
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler,
                                               MessagingListener listener, Destination destination) {
        return new ConsumerUnit(deque, handler, listener, destination) {
            @Override
            protected void settle(Message message, boolean succeeded) {
                // Bug under test: never re-offer and never report onError, whatever the outcome.
            }
        };
    }
}
