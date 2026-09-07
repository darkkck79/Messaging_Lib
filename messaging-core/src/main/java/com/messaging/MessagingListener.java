package com.messaging;

/** Event callbacks for connection and message lifecycle. All four are default no-ops so
 * applications override only what they observe. */
public interface MessagingListener {

    default void onPublished(Destination destination) {}

    default void onConsumed(Destination destination) {}

    default void onError(Destination destination, Throwable error) {}

    default void onConnectionStateChanged(ConnectionState state) {}
}
