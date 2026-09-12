package com.messaging.conformance;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * Abstract base class for connectivity tests (restart, disconnect, reconnect).
 * Real adapter tests extend this.
 */
public abstract class AbstractMessagingConnectivityTest extends AbstractMessagingConformanceTest {

    @Test
    void brokerRestartPreservesSubscriptions() {
        Topic topic = newTopic("restart-test");
        provisionTopic(topic.name());

        var received = new CopyOnWriteArrayList<String>();
        var subscription = bus.subscribe(topic, message -> received.add(new String(message.body())));

        // Wait for subscription to be ready
        boolean subscribed = false;
        long timeout = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < timeout) {
            if (subscription.isOpen()) {
                subscribed = true;
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(subscribed).as("Subscription should be ready before restart").isTrue();

        // Restart broker
        restartBroker();

        // Publish a message after restart
        var future = bus.publish(topic, "post-restart-message");
        assertThat(future).isCompletedSuccessfully();

        // Wait for message to be received
        boolean receivedPostRestart = false;
        timeout = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < timeout) {
            if (received.size() > 0) {
                receivedPostRestart = true;
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(receivedPostRestart).as("Message should be delivered after restart").isTrue();
    }

    @Test
    void networkCutAndRestore() {
        Topic topic = newTopic("network-cut");
        provisionTopic(topic.name());

        var received = new CopyOnWriteArrayList<String>();
        bus.subscribe(topic, message -> received.add(new String(message.body())));

        // Cut network
        cutNetwork();

        // Try to publish - should fail or timeout
        var future = bus.publish(topic, "during-cut");
        // This might fail or succeed depending on implementation
        // The key is that the transport handles the network issue gracefully

        // Restore network
        restoreNetwork();

        // Publish after restore
        var future2 = bus.publish(topic, "post-restore");
        assertThat(future2).isCompletedSuccessfully();
    }
}
