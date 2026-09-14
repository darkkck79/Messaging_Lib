package com.messaging.jms;

import com.messaging.ConnectionState;
import com.messaging.Destination;
import com.messaging.Message;
import com.messaging.MessageHandler;
import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import com.messaging.Queue;
import com.messaging.Subscription;
import com.messaging.Topic;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The JMS adapter's {@link Transport}. Connects with the URL's userinfo (never its host or
 * port — the broker address lives in factory properties, §I amendment). Publishes through
 * one lock-guarded, non-transacted session (§E); consumer units never touch it. Subscribing
 * spawns {@code config.concurrency()} {@link JmsConsumerUnit}s per call.
 */
public final class JmsTransport implements Transport {

    private final Connection connection;
    private final Session publishSession;
    private final MessageProducer publishProducer;
    private final Object publishLock = new Object();
    private final MessagingListener listener;
    private final List<JmsConsumerUnit> consumerUnits = new CopyOnWriteArrayList<>();
    private final int concurrency;
    private final String clientId;
    private volatile boolean closed;

    private JmsTransport(Connection connection, Session publishSession, MessageProducer publishProducer,
                          MessagingListener listener, int concurrency, String clientId) {
        this.connection = connection;
        this.publishSession = publishSession;
        this.publishProducer = publishProducer;
        this.listener = listener;
        this.concurrency = concurrency;
        this.clientId = clientId;
    }

    static JmsTransport connect(MessagingConfig config, MessagingListener listener) {
        ConnectionFactory factory = ConnectionFactoryBuilder.build(config);
        String userInfo = config.url().getUserInfo();

        ExecutorService connectExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "jms-connect");
            t.setDaemon(true);
            return t;
        });
        // Published from the connect thread so a connection that arrives past the deadline can
        // still be closed: a cancelled Future never hands its result back through get().
        AtomicReference<Connection> late = new AtomicReference<>();
        Future<Connection> future = connectExecutor.submit(() -> {
            Connection c = userInfo != null ? createWithUserInfo(factory, userInfo) : factory.createConnection();
            late.set(c);
            return c;
        });

        Connection connection;
        try {
            connection = future.get(config.connectTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            future.cancel(true);
            // Queued behind the connect task on the same single thread, so it runs once that
            // task finally returns — closing the late connection instead of leaking it.
            connectExecutor.execute(() -> {
                Connection lateConnection = late.get();
                if (lateConnection != null) closeQuietly(lateConnection);
            });
            connectExecutor.shutdown();
            throw new MessagingException("Failed to connect to JMS broker within " + config.connectTimeout(), e);
        }
        connectExecutor.shutdown();

        try {
            connection.setClientID(config.clientId());
            connection.setExceptionListener(ex -> listener.onConnectionStateChanged(ConnectionState.DISCONNECTED));
            // Non-transacted publish session (§B): the publish path never commits/rolls
            // back — it is a single synchronous send guarded by publishLock.
            Session publishSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer publishProducer = publishSession.createProducer(null);
            publishProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();
            listener.onConnectionStateChanged(ConnectionState.CONNECTED);
            return new JmsTransport(connection, publishSession, publishProducer, listener,
                config.concurrency(), config.clientId());
        } catch (Exception e) {
            closeQuietly(connection);
            throw new MessagingException("Failed to initialise JMS connection", e);
        }
    }

    private static Connection createWithUserInfo(ConnectionFactory factory, String userInfo) throws JMSException {
        String[] parts = userInfo.split(":", 2);
        String user = parts[0];
        String password = parts.length > 1 ? parts[1] : "";
        return factory.createConnection(user, password);
    }

    private static void closeQuietly(Connection connection) {
        try { connection.close(); } catch (Exception ignored) {}
    }

    @Override
    public CompletableFuture<Void> publish(Destination destination, Message message) {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("Transport is closed"));
        try {
            synchronized (publishLock) {
                jakarta.jms.Destination jmsDestination = resolveDestination(publishSession, destination);
                BytesMessage bytesMessage = publishSession.createBytesMessage();
                bytesMessage.writeBytes(message.body());
                for (var entry : message.headers().entrySet()) {
                    bytesMessage.setStringProperty(entry.getKey(), entry.getValue());
                }
                publishProducer.send(jmsDestination, bytesMessage);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new MessagingException("Failed to publish to " + destination, e));
        }
    }

    /** Shared by publish (this class) and each consumer unit's own session. */
    static jakarta.jms.Destination resolveDestination(Session session, Destination destination) throws JMSException {
        return switch (destination) {
            case Topic topic -> session.createTopic(topic.name());
            case Queue queue -> session.createQueue(queue.name());
        };
    }

    @Override
    public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("Transport is closed"));

        String sharedSubscriptionName = destination instanceof Topic
            ? clientId + "-" + UUID.randomUUID() : null;

        List<JmsConsumerUnit> units = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> ready = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            JmsConsumerUnit unit = new JmsConsumerUnit(connection, destination, sharedSubscriptionName, handler, listener);
            units.add(unit);
            consumerUnits.add(unit);
            ready.add(unit.start());
        }

        return CompletableFuture.allOf(ready.toArray(CompletableFuture[]::new)).handle((v, ex) -> {
            if (ex != null) {
                units.forEach(u -> u.stop(Duration.ofSeconds(5)));
                units.forEach(consumerUnits::remove);
                throw new MessagingException("Failed to subscribe to " + destination, ex);
            }
            Subscription subscription = () -> {
                units.forEach(u -> u.stop(Duration.ofSeconds(30)));
                units.forEach(consumerUnits::remove);
            };
            return subscription;
        });
    }

    @Override
    public void close(Duration timeout) {
        if (closed) return;
        closed = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        for (JmsConsumerUnit unit : consumerUnits) {
            unit.stop(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        }
        consumerUnits.clear();
        try { synchronized (publishLock) { publishSession.close(); } } catch (Exception ignored) {}
        closeQuietly(connection);
    }
}
