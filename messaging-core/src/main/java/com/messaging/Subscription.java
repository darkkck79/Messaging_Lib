package com.messaging;

public interface Subscription extends AutoCloseable {
    @Override
    void close();
}
