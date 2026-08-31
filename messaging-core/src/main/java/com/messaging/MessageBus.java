package com.messaging;

import java.util.Map;

public interface MessageBus extends AutoCloseable {
    Subscription subscribe(Topic topic, MessageHandler handler, TypedHandler<?> typedHandler);

    Subscription subscribe(Queue queue, MessageHandler handler, TypedHandler<?> typedHandler);

    <T> TypedChannel<T> typed(Topic topic, Codec<T> codec);

    <T> TypedChannel<T> typed(Queue queue, Codec<T> codec);

    void publish(Topic topic, byte[] body, Map<String, String> headers);

    void publish(Queue queue, byte[] body, Map<String, String> headers);

    @Override
    void close();
}
