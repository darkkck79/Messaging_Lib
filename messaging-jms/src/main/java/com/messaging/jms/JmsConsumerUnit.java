package com.messaging.jms;

import com.messaging.Destination;
import com.messaging.Message;
import com.messaging.MessageHandler;
import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import com.messaging.Topic;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * One JMS consumer's owner loop (§B). Its own thread creates the transacted {@link Session}
 * and {@link MessageConsumer}, completes the "ready" future, then loops on
 * {@code receive(500ms)}: map the message, run the handler on this thread, commit on
 * success or rollback on failure. The only cross-thread calls are {@link #stop}'s signal,
 * a bounded {@code Thread.join}, and, as a last resort past the deadline, {@code close()}.
 */
final class JmsConsumerUnit {

    private static final long RECEIVE_TIMEOUT_MS = 500;

    private final Connection connection;
    private final Destination destination;
    private final String sharedSubscriptionName;
    private final MessageHandler handler;
    private final MessagingListener listener;

    private volatile Session session;
    private volatile MessageConsumer consumer;
    private volatile Thread thread;
    private volatile boolean stopping;
    private volatile CompletableFuture<Void> currentOutcome;

    JmsConsumerUnit(Connection connection, Destination destination, String sharedSubscriptionName,
                     MessageHandler handler, MessagingListener listener) {
        this.connection = connection;
        this.destination = destination;
        this.sharedSubscriptionName = sharedSubscriptionName;
        this.handler = handler;
        this.listener = listener;
    }

    /** Starts the owner thread; the returned future completes once the consumer exists. */
    CompletableFuture<Void> start() {
        CompletableFuture<Void> ready = new CompletableFuture<>();
        thread = new Thread(() -> run(ready), "jms-consumer-" + destination);
        thread.setDaemon(true);
        thread.start();
        return ready;
    }

    private void run(CompletableFuture<Void> ready) {
        try {
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            jakarta.jms.Destination jmsDestination = JmsTransport.resolveDestination(session, destination);
            if (destination instanceof Topic && sharedSubscriptionName != null) {
                consumer = session.createSharedConsumer((jakarta.jms.Topic) jmsDestination, sharedSubscriptionName);
            } else {
                consumer = session.createConsumer(jmsDestination);
            }
        } catch (Exception e) {
            ready.completeExceptionally(new MessagingException("Failed to create consumer for " + destination, e));
            return;
        }
        ready.complete(null);

        while (!stopping) {
            jakarta.jms.Message received;
            try {
                received = consumer.receive(RECEIVE_TIMEOUT_MS);
            } catch (Exception e) {
                listener.onError(destination, new MessagingException("receive() failed for " + destination, e));
                sleepQuietly(RECEIVE_TIMEOUT_MS);
                continue;
            }
            if (received == null) continue;
            processOneMessage(received);
        }
        closeQuietly();
    }

    private void processOneMessage(jakarta.jms.Message received) {
        CompletableFuture<Void> outcome;
        if (!(received instanceof BytesMessage bytesMessage)) {
            outcome = CompletableFuture.failedFuture(
                new MessagingException("Unsupported JMS message type: " + received.getClass()));
        } else {
            try {
                Message message = toMessage(bytesMessage);
                outcome = invokeHandler(message);
            } catch (Exception mappingFailed) {
                outcome = CompletableFuture.failedFuture(mappingFailed);
            }
        }
        currentOutcome = outcome;
        boolean succeeded;
        try {
            outcome.get();
            succeeded = true;
        } catch (Exception failedOrCancelled) {
            succeeded = false;
        }
        currentOutcome = null;
        settle(succeeded);
    }

    private CompletableFuture<Void> invokeHandler(Message message) {
        try {
            CompletableFuture<Void> outcome = handler.handle(message);
            return outcome != null ? outcome
                : CompletableFuture.failedFuture(new MessagingException("Handler returned null future"));
        } catch (Throwable synchronousThrow) {
            return CompletableFuture.failedFuture(synchronousThrow);
        }
    }

    private Message toMessage(BytesMessage bytesMessage) throws JMSException {
        byte[] body = new byte[(int) bytesMessage.getBodyLength()];
        bytesMessage.readBytes(body);
        var headers = new LinkedHashMap<String, String>();
        var names = bytesMessage.getPropertyNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            headers.put(name, String.valueOf(bytesMessage.getObjectProperty(name)));
        }
        if (bytesMessage.getJMSRedelivered()) headers.put("messaging.redelivered", "true");
        return new Message(body, headers);
    }

    private void settle(boolean succeeded) {
        try {
            if (succeeded) {
                session.commit();
                listener.onConsumed(destination);
            } else {
                session.rollback();
                listener.onError(destination, new MessagingException("Handler failed for " + destination));
            }
        } catch (Exception commitOrRollbackFailed) {
            listener.onError(destination, new MessagingException(
                "Settlement failed for " + destination, commitOrRollbackFailed));
        }
    }

    void stop(Duration timeout) {
        stopping = true;
        Thread t = thread;
        if (t == null) return;
        try {
            t.join(Math.max(0, timeout.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            CompletableFuture<Void> outcome = currentOutcome;
            if (outcome != null) outcome.cancel(true);
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) closeQuietly();
        }
    }

    private void closeQuietly() {
        try { if (consumer != null) consumer.close(); } catch (Exception ignored) {}
        try { if (session != null) session.close(); } catch (Exception ignored) {}
    }

    private static void sleepQuietly(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
