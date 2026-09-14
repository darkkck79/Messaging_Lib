package com.messaging.internal;

import com.messaging.ConnectionState;
import com.messaging.MessagingListener;
import com.messaging.Topic;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class SafeListenerTest {

    @Test void wrapIsIdempotent() {
        MessagingListener wrapped = SafeListener.wrap(new MessagingListener() {});
        assertThat(SafeListener.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test void throwingListenerDoesNotPropagate() {
        MessagingListener throwing = new MessagingListener() {
            @Override public void onPublished(com.messaging.Destination d) { throw new RuntimeException("boom"); }
            @Override public void onConsumed(com.messaging.Destination d) { throw new RuntimeException("boom"); }
            @Override public void onError(com.messaging.Destination d, Throwable e) { throw new RuntimeException("boom"); }
            @Override public void onConnectionStateChanged(ConnectionState s) { throw new RuntimeException("boom"); }
        };
        MessagingListener safe = SafeListener.wrap(throwing);
        Topic t = Topic.of("x");
        assertThatCode(() -> {
            safe.onPublished(t);
            safe.onConsumed(t);
            safe.onError(t, new RuntimeException());
            safe.onConnectionStateChanged(ConnectionState.CONNECTED);
        }).doesNotThrowAnyException();
    }
}
