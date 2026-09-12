package com.messaging.internal;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class DefaultMessageBus implements MessageBus {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessageBus.class);

    private final TransportProvider provider;
    private final MessagingConfig config;
    private final MessagingListener listener;
    private volatile boolean closed;

    public DefaultMessageBus(Transport transport, MessagingConfig config, MessagingListener listener) {
        this.provider = null;
        this.config = config;
        this.listener = listener;
        // The transport itself manages connectivity
    }

    public DefaultMessageBus(MessagingConfig config, MessagingListener listener, TransportProvider provider) {
        this.provider = provider;
        this.config = config;
        this.listener = listener;
    }

    @Override
    public CompletableFuture<Void> publish(Destination destination, Message message) {
        checkNotClosed();
        HeaderValidator.validateForPublish(message.headers());
        return transport().publish(destination, message).whenComplete((v, ex) -> {
            if (ex != null) {
                try { listener.onError(destination, ex); }
                catch (Exception e) { log.warn("Listener threw on onError", e); }
            } else {
                try { listener.onPublished(destination); }
                catch (Exception e) { log.warn("Listener threw on onPublished", e); }
            }
        });
    }

    @Override
    public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
        checkNotClosed();
        return transport().subscribe(destination, handler);
    }

    @Override
    public <T> TypedChannel<T> typed(Destination destination, Codec<T> codec) {
        checkNotClosed();
        return new TypedChannel<>(this, destination, codec);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try { transport().close(config.closeTimeout()); }
        catch (Exception e) { log.warn("Error closing transport", e); }
    }

    private Transport transport() {
        return (provider != null) ? provider.open(config, listener) : new InternalTransport();
    }

    private void checkNotClosed() {
        if (closed) throw new IllegalStateException("MessageBus is closed");
    }

    private static class InternalTransport implements Transport {
        @Override
        public java.util.concurrent.CompletableFuture<Void> publish(Destination destination, Message message) {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
            return java.util.concurrent.CompletableFuture.completedFuture(new Subscription() {
                @Override
                public void close() {}
            });
        }

        @Override
        public void close(java.time.Duration timeout) {}
    }
}
