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

    public DefaultMessageBus(MessagingConfig config) {
        this.config = config;
        connected.set(true);
    }

    @Override
    public Subscription subscribe(Topic topic, MessageHandler handler, TypedHandler<?> typedHandler) {
        checkConnected();
        Transport transport = getTransport(topic.name());
        Transport.SubscriptionImpl sub = transport.subscribe(topic.name(), handler, typedHandler);
        stateRef.set(ConnectionState.CONNECTED);
        return sub::close;
    }

    @Override
    public Subscription subscribe(Queue queue, MessageHandler handler, TypedHandler<?> typedHandler) {
        checkConnected();
        Transport transport = getTransport(queue.name());
        Transport.SubscriptionImpl sub = transport.subscribe(queue.name(), handler, typedHandler);
        stateRef.set(ConnectionState.CONNECTED);
        return sub::close;
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
    }

    @Override
    public void publish(Queue queue, byte[] body, Map<String, String> headers) {
        checkConnected();
        HeaderValidator.validateForPublish(headers);
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
