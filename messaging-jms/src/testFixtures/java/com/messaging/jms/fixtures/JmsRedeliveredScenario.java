package com.messaging.jms.fixtures;

import com.messaging.Destination;
import com.messaging.MessageBus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** JMS-only scenario (§G amendment): proves {@code messaging.redelivered} is set on
 * redelivery. Both fixture families implement this so the test isn't duplicated. */
public interface JmsRedeliveredScenario {

    MessageBus jmsBus();
    Destination jmsQueue(String name);

    @Test
    default void redeliveredHeaderSetOnRedelivery() {
        Destination queue = jmsQueue("redelivered-header");
        var seenHeaders = new CopyOnWriteArrayList<Map<String, String>>();
        var attempt = new AtomicInteger();
        jmsBus().subscribe(queue, message -> {
            seenHeaders.add(message.headers());
            if (attempt.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(new RuntimeException("force redelivery"));
            }
            return CompletableFuture.completedFuture(null);
        }).join();

        jmsBus().publish(queue, "payload".getBytes()).join();

        await().atMost(Duration.ofSeconds(30))
            .untilAsserted(() -> assertThat(seenHeaders).hasSizeGreaterThanOrEqualTo(2));
        assertThat(seenHeaders.get(0)).doesNotContainKey("messaging.redelivered");
        assertThat(seenHeaders.get(1)).containsEntry("messaging.redelivered", "true");
    }
}
