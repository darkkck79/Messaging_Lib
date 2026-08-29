package com.messaging;

public interface MessagingListener {
    default void connected(ConnectionState state) {}
    default void disconnected(ConnectionState state) {}
    default void reconnected(ConnectionState state) {}
}
