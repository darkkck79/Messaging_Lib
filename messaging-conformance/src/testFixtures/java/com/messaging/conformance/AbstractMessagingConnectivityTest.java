package com.messaging.conformance;

import com.messaging.*;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

/**
 * Connectivity tests (§D, §K): real adapters extend this. Per §D, only queue delivery
 * during an outage is guaranteed portably — topic delivery during an outage is an
 * explicit, untested portability exception and is deliberately not asserted here.
 */
public abstract class AbstractMessagingConnectivityTest extends AbstractMessagingConformanceTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Duration OUTAGE_PUBLISH_BOUND = Duration.ofSeconds(60);

    /** Restart the broker process, simulating an outage the adapter must recover from. */
    protected abstract void restartBroker();

    /** Sever network connectivity between the adapter and the broker. */
    protected abstract void cutNetwork();

    /** Restore network connectivity cut by {@link #cutNetwork()}. */
    protected abstract void restoreNetwork();

    /** Publish over a link that bypasses whatever {@link #cutNetwork()} cuts, so a test can
     * put a message on a broker-backed destination while the adapter under test is offline. */
    protected abstract void publishOutOfBand(Destination destination, byte[] body);

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
        var received = new CopyOnWriteArrayList<String>();
        awaitSubscribed(queue, message -> {
            received.add(new String(message.body()));
            return CompletableFuture.completedFuture(null);
        });

        cutNetwork();
        publishOutOfBand(queue, "during-outage".getBytes());
        restoreNetwork();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("during-outage"));
    }

    @Test
    void publishDuringOutageFails() {
        Destination queue = provisionQueue("outage-publish-fails");
        cutNetwork();
        try {
            long startNanos = System.nanoTime();
            assertThatThrownBy(() -> bus.publish(queue, "during-outage".getBytes()).join())
                .hasCauseInstanceOf(com.messaging.MessagingException.class);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            assertThat(elapsedMs).isLessThan(OUTAGE_PUBLISH_BOUND.toMillis());
        } finally {
            restoreNetwork();
        }
    }

    @Test
    void busCloseRedeliversInFlightOnNextStart() throws Exception {
        Destination queue = provisionQueue("bus-close-redelivers");
        var neverCompletes = new CompletableFuture<Void>();
        bus.subscribe(queue, message -> neverCompletes).join();
        publish(queue, "stuck".getBytes());
        Thread.sleep(300);

        bus.close();

        try (MessageBus freshBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(1), 1))) {
            var received = new CopyOnWriteArrayList<String>();
            freshBus.subscribe(queue, recordingHandler(received)).join();
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("stuck"));
        }
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
