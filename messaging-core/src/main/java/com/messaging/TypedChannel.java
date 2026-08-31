package com.messaging;

import java.util.Map;

public interface TypedChannel<T> {
    void publish(T message, Map<String, String> headers);

    Subscription subscribe(TypedHandler<T> handler);
}
