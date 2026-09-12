package com.messaging;

import com.messaging.config.MessagingConfig;
import com.messaging.internal.DefaultMessageBus;
import com.messaging.spi.TransportProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Static factory for connecting to a messaging transport.
 * Delegates to {@link com.messaging.internal.DefaultMessageBus} via the SPI.
 * Listener callbacks are wrapped to isolate exceptions from the configured {@link MessagingListener}.
 */
public final class Messaging {

    private static final Logger log = LoggerFactory.getLogger(Messaging.class);

    private Messaging() {
        throw new AssertionError("no instances");
    }

    public static MessageBus connect(MessagingConfig config) {
        MessagingListener rawListener = config.listener();
        MessagingListener safeListener = wrapSafe(rawListener);
        // Resolve transport eagerly so that a missing provider fails fast
        TransportProvider provider = TransportProvider.lookup(config);
        return new DefaultMessageBus(config, safeListener, provider);
    }

    /**
     * Wraps a listener so that any exception thrown by its callback methods
     * is logged and swallowed instead of propagating to the caller.
     */
    private static MessagingListener wrapSafe(MessagingListener raw) {
        return new MessagingListener() {
            @Override
            public void onPublished(Destination destination) {
                try { raw.onPublished(destination); }
                catch (Exception e) { log.warn("Listener threw on onPublished({})", destination, e); }
            }

            @Override
            public void onConsumed(Destination destination) {
                try { raw.onConsumed(destination); }
                catch (Exception e) { log.warn("Listener threw on onConsumed({})", destination, e); }
            }

            @Override
            public void onError(Destination destination, Throwable error) {
                try { raw.onError(destination, error); }
                catch (Exception e) { log.warn("Listener threw on onError({})", destination, e); }
            }

            @Override
            public void onConnectionStateChanged(ConnectionState state) {
                try { raw.onConnectionStateChanged(state); }
                catch (Exception e) { log.warn("Listener threw on onConnectionStateChanged({})", state, e); }
            }
        };
    }
}
