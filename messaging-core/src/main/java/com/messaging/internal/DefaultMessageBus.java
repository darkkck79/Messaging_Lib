package com.messaging.internal;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;

public final class DefaultMessageBus implements MessageBus {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessageBus.class);
    private final Transport transport;
    private final MessagingConfig config;
    private final MessagingListener listener;
    private volatile boolean closed;

    public DefaultMessageBus(Transport transport, MessagingConfig config, MessagingListener listener) {
        this.transport = transport; this.config = config; this.listener = SafeListener.wrap(listener);
    }

    @Override public CompletableFuture<Void> publish(Destination dest, Message msg) {
        checkNotClosed();
        HeaderValidator.validateForPublish(msg.headers());
        return transport.publish(dest, msg).whenComplete((v, ex) -> {
            if (ex != null) listener.onError(dest, ex);
            else listener.onPublished(dest);
        });
    }

    @Override public CompletableFuture<Subscription> subscribe(Destination dest, MessageHandler handler) {
        checkNotClosed(); return transport.subscribe(dest, handler);
    }

    @Override public <T> TypedChannel<T> typed(Destination dest, Codec<T> codec) {
        checkNotClosed(); return new TypedChannel<>(this, dest, codec);
    }

    @Override public void close() {
        if (closed) return; closed = true;
        try { transport.close(config.closeTimeout()); } catch (Exception e) { log.warn("Error closing transport", e); }
    }

    private void checkNotClosed() { if (closed) throw new IllegalStateException("MessageBus is closed"); }
}
