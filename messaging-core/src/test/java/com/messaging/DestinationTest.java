package com.messaging;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DestinationTest {

    @Test void topicCreation() {
        Topic topic = Topic.of("orders");
        assertThat(topic.name()).isEqualTo("orders");
        assertThat(topic).isInstanceOf(Destination.class);
    }

    @Test void queueCreation() {
        Queue queue = Queue.of("orders");
        assertThat(queue.name()).isEqualTo("orders");
        assertThat(queue).isInstanceOf(Destination.class);
    }

    @Test void topicRejectsNull() {
        assertThatThrownBy(() -> Topic.of(null)).isInstanceOf(NullPointerException.class);
    }

    @Test void topicRejectsBlank() {
        assertThatThrownBy(() -> Topic.of("  ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void queueRejectsNull() {
        assertThatThrownBy(() -> Queue.of(null)).isInstanceOf(NullPointerException.class);
    }

    @Test void queueRejectsBlank() {
        assertThatThrownBy(() -> Queue.of("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void topicEquality() {
        assertThat(Topic.of("a")).isEqualTo(Topic.of("a"));
        assertThat(Topic.of("a")).isNotEqualTo(Topic.of("b"));
    }

    @Test void queueEquality() {
        assertThat(Queue.of("a")).isEqualTo(Queue.of("a"));
    }

    @Test void sealedPermits() {
        assertThat(Destination.class.isSealed()).isTrue();
        assertThat(Destination.class.getPermittedSubclasses())
            .containsExactlyInAnyOrder(Topic.class, Queue.class);
    }
}
