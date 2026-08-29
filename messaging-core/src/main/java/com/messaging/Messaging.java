package com.messaging;

import com.messaging.config.MessagingConfig;

/**
 * Static factory for connecting to a messaging transport.
 * Delegates to {@link com.messaging.internal.DefaultMessageBus} via the SPI.
 */
public final class Messaging {

    private Messaging() {
        throw new AssertionError("no instances");
    }

    public static MessageBus connect(MessagingConfig config) {
        return new com.messaging.internal.DefaultMessageBus(config);
    }
}
