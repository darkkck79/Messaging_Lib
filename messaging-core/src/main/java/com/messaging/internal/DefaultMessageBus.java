package com.messaging.internal;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultMessageBus implements MessageBus, MessagingListener {

    private final MessagingConfig config;
    private final Map<String, Transport> transports = new ConcurrentHashMap<>();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicReference<ConnectionState> stateRef = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private final MessagingListener listener;

    public DefaultMessageBus(MessagingConfig config) {
        this.config = config;
        this.listener = config.listener();
        connected.set(true);
    }

    DefaultMessageBus(MessagingConfig config, MessagingListener listener) {
        this.config = config;
        this.listener = listener;
        connected.set(true);
    }

    @Override
    public Subscription subscribe(Topic topic, MessageHandler handler, TypedHandler<?> typedHandler) {
        checkConnected();
        Transport transport = getTransport(topic.name());
        Transport.SubscriptionImpl sub = transport.subscribe(topic.name(), isolate(topic, handler), typedHandler);
        stateRef.set(ConnectionState.CONNECTED);
        return sub::close;
    }

    @Override
    public Subscription subscribe(Queue queue, MessageHandler handler, TypedHandler<?> typedHandler) {
        checkConnected();
        Transport transport = getTransport(queue.name());
        Transport.SubscriptionImpl sub = transport.subscribe(queue.name(), isolate(queue, handler), typedHandler);
        stateRef.set(ConnectionState.CONNECTED);
        return sub::close;
    }

    /**
     * Wraps a handler so a thrown exception (or a future that completes exceptionally)
     * is reported to the listener instead of propagating into the transport's delivery loop,
     * which would otherwise abort delivery to any remaining subscribers.
     */
    private MessageHandler isolate(Destination destination, MessageHandler handler) {
        return message -> {
            try {
                java.util.concurrent.CompletableFuture<Void> result = handler.handle(message);
                return result.handle((value, error) -> {
                    if (error != null) {
                        listener.onError(destination, error);
                    } else {
                        listener.onConsumed(destination);
                    }
                    return null;
                });
            } catch (Throwable error) {
                listener.onError(destination, error);
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
        };
    }

    @Override
    public <T> TypedChannel<T> typed(Topic topic, Codec<T> codec) {
        checkConnected();
        return new DefaultTypedChannel<>(topic.name(), codec, getTransport(topic.name()));
    }

    @Override
    public <T> TypedChannel<T> typed(Queue queue, Codec<T> codec) {
        checkConnected();
        return new DefaultTypedChannel<>(queue.name(), codec, getTransport(queue.name()));
    }

    @Override
    public void publish(Topic topic, byte[] body, Map<String, String> headers) {
        checkConnected();
        HeaderValidator.validateForPublish(headers);
        Transport transport = getTransport(topic.name());
        transport.publish(topic.name(), body, headers);
        listener.onPublished(topic);
    }

    @Override
    public void publish(Queue queue, byte[] body, Map<String, String> headers) {
        checkConnected();
        HeaderValidator.validateForPublish(headers);
        Transport transport = getTransport(queue.name());
        transport.publish(queue.name(), body, headers);
        listener.onPublished(queue);
    }

    @Override
    public void onPublished(Destination destination) {
        listener.onPublished(destination);
    }

    @Override
    public void onConsumed(Destination destination) {
        listener.onConsumed(destination);
    }

    @Override
    public void onError(Destination destination, Throwable error) {
        listener.onError(destination, error);
    }

    @Override
    public void onConnectionStateChanged(ConnectionState state) {
        listener.onConnectionStateChanged(state);
    }

    @Override
    public void close() {
        for (Transport t : transports.values()) {
            t.close();
        }
        transports.clear();
        stateRef.set(ConnectionState.DISCONNECTED);
        connected.set(false);
    }

    private Transport getTransport(String name) {
        Transport transport = transports.computeIfAbsent(name, name2 -> {
            TransportProvider provider = TransportProvider.lookup(config);
            return provider.create(name, config);
        });
        return transport;
    }

    private void checkConnected() {
        if (!connected.get()) {
            throw new MessagingException("Not connected. Call connect() first.");
        }
    }

    public boolean isConnected() {
        return connected.get();
    }

    public ConnectionState state() {
        return stateRef.get();
    }

    private static class DefaultTypedChannel<T> implements TypedChannel<T> {
        private final String destinationName;
        private final Codec<T> codec;
        private final Transport transport;

        DefaultTypedChannel(String destinationName, Codec<T> codec, Transport transport) {
            this.destinationName = destinationName;
            this.codec = codec;
            this.transport = transport;
        }

        @Override
        public void publish(T message, Map<String, String> headers) {
            HeaderValidator.validateForPublish(headers);
            transport.publish(destinationName, codec.encode(message), headers);
        }

        @Override
        public Subscription subscribe(TypedHandler<T> handler) {
            final MessageHandler proxy = msg -> {
                T decoded = codec.decode(msg.body());
                return handler.handle(decoded, msg.headers());
            };
            Transport.SubscriptionImpl sub = transport.subscribe(destinationName, proxy, null);
            return sub::close;
        }
    }
}
