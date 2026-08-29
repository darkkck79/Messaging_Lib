package com.messaging.internal;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultMessageBus implements MessageBus, MessagingListener {

    private final MessagingConfig config;
    private final Map<String, Transport> transports = new ConcurrentHashMap<>();
    private final KeySetView<String, Transport> destinations = transports.keySet();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicReference<ConnectionState> stateRef = new AtomicReference<>(ConnectionState.DISCONNECTED);

    public DefaultMessageBus(MessagingConfig config) {
        this.config = config;
    }

    @Override
    public Subscription subscribe(Topic topic, MessageHandler handler, TypedHandler<?> typedHandler) {
        checkConnected();
        Transport transport = getTransport(topic.name());
        Transport.SubscriptionImpl sub = transport.subscribe(topic.name(), handler, typedHandler);
        stateRef.set(ConnectionState.CONNECTED);
        return sub;
    }

    @Override
    public Subscription subscribe(Queue queue, MessageHandler handler, TypedHandler<?> typedHandler) {
        checkConnected();
        Transport transport = getTransport(queue.name());
        Transport.SubscriptionImpl sub = transport.subscribe(queue.name(), handler, typedHandler);
        stateRef.set(ConnectionState.CONNECTED);
        return sub;
    }

    @Override
    public <T> TypedChannel<T> typed(Topic topic, Codec<T> codec) {
        checkConnected();
        return new DefaultTypedChannel<>(topic, codec, getTransport(topic.name()));
    }

    @Override
    public <T> TypedChannel<T> typed(Queue queue, Codec<T> codec) {
        checkConnected();
        return new DefaultTypedChannel<>(queue, codec, getTransport(queue.name()));
    }

    @Override
    public void publish(Topic topic, byte[] body, Map<String, String> headers) {
        checkConnected();
        Transport transport = getTransport(topic.name());
        transport.publish(topic.name(), body, headers);
    }

    @Override
    public void publish(Queue queue, byte[] body, Map<String, String> headers) {
        checkConnected();
        Transport transport = getTransport(queue.name());
        transport.publish(queue.name(), body, headers);
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

    @Override
    public void connected(ConnectionState state) {
        connected.set(true);
        stateRef.set(state);
    }

    @Override
    public void disconnected(ConnectionState state) {
        connected.set(false);
        stateRef.set(state);
    }

    @Override
    public void reconnected(ConnectionState state) {
        connected.set(true);
        stateRef.set(state);
    }

    public boolean isConnected() {
        return connected.get();
    }

    public ConnectionState state() {
        return stateRef.get();
    }

    private static class DefaultTypedChannel<T> implements TypedChannel<T> {
        private final Topic topic;
        private final Codec<T> codec;
        private final Transport transport;

        DefaultTypedChannel(Topic topic, Codec<T> codec, Transport transport) {
            this.topic = topic;
            this.codec = codec;
            this.transport = transport;
        }

        @Override
        public void publish(T message, Map<String, String> headers) {
            transport.publish(topic.name(), codec.encode(message), headers);
        }

        @Override
        public Subscription subscribe(TypedHandler<T> handler) {
            final MessageHandler proxy = msg -> {
                T decoded = codec.decode(msg.body());
                return handler.handle(decoded, msg.headers());
            };
            return transport.subscribe(topic.name(), proxy, null);
        }
    }
}
