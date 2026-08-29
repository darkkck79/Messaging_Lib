package com.messaging.spi;

import com.messaging.MessageHandler;
import com.messaging.Topic;
import com.messaging.TypedHandler;

import java.util.Map;

public interface Transport {
    void publish(String destinationName, byte[] body, Map<String, String> headers);

    <T> SubscriptionImpl subscribe(String destinationName, MessageHandler handler, TypedHandler<T> typedHandler);

    void close();

    default boolean supports(String providerName) {
        return false;
    }

    interface SubscriptionImpl extends AutoCloseable {
        @Override
        void close();
    }
}
