package com.messaging.conformance;

import com.messaging.Destination;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

/**
 * Connectivity tests (§D, §K): real adapters extend this. Per §D, only queue delivery
 * during an outage is guaranteed portably — topic delivery during an outage is an
 * explicit, untested portability exception and is deliberately not asserted here.
 */
public abstract class AbstractMessagingConnectivityTest extends AbstractMessagingConformanceTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** Restart the broker process, simulating an outage the adapter must recover from. */
    protected abstract void restartBroker();

    /** Sever network connectivity between the adapter and the broker. */
    protected abstract void cutNetwork();

    /** Restore network connectivity cut by {@link #cutNetwork()}. */
    protected abstract void restoreNetwork();

    @Test
    void queueSurvivesBrokerRestart() {
        Destination queue = provisionQueue("restart-survive");
        var received = new CopyOnWriteArrayList<String>();
        awaitSubscribed(queue, message -> {
            received.add(new String(message.body()));
            return CompletableFuture.completedFuture(null);
        });

        restartBroker();

        publish(queue, "post-restart".getBytes());
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("post-restart"));
    }

    @Test
    void queueMessagesPublishedDuringOutageDeliveredAfterRecovery() {
        Destination queue = provisionQueue("outage-delivery");

        // Published to the broker-backed queue with no consumer connected yet.
        publish(queue, "during-outage".getBytes());

        cutNetwork();
        restoreNetwork();

        var received = new CopyOnWriteArrayList<String>();
        awaitSubscribed(queue, message -> {
            received.add(new String(message.body()));
            return CompletableFuture.completedFuture(null);
        });

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("during-outage"));
    }

    @Test
    void reconnectAndResumeAfterNetworkCut() {
        Destination queue = provisionQueue("reconnect-resume");
        var received = new CopyOnWriteArrayList<String>();
        awaitSubscribed(queue, message -> {
            received.add(new String(message.body()));
            return CompletableFuture.completedFuture(null);
        });

        cutNetwork();
        restoreNetwork();

        publish(queue, "post-reconnect".getBytes());
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("post-reconnect"));
    }
}
