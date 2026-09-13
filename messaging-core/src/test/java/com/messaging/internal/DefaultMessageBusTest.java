package com.messaging.internal;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultMessageBusTest {
    @Mock Transport transport;
    DefaultMessageBus bus;

    @BeforeEach void setUp() {
        bus = new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(),
            MessagingListener.noOp());
    }

    @Test void publishDelegatesToTransport() {
        when(transport.publish(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        bus.publish(Queue.of("q"), new Message("hello".getBytes(), Map.of()));
        verify(transport).publish(eq(Queue.of("q")), any());
    }
    @Test void publishValidatesHeaders() {
        assertThatThrownBy(() -> bus.publish(Queue.of("q"), new Message(new byte[0], Map.of("JMSBadKey", "v"))))
            .isInstanceOf(MessagingException.class);
        verify(transport, never()).publish(any(), any());
    }
    @Test void closeIsIdempotent() {
        bus.close(); bus.close();
        verify(transport, times(1)).close(any(Duration.class));
    }
    @Test void apiCallAfterCloseThrows() {
        bus.close();
        assertThatThrownBy(() -> bus.publish(Queue.of("q"), new byte[0]))
            .isInstanceOf(IllegalStateException.class);
    }
    @Test void listenerExceptionDoesNotAffectPublish() {
        var bad = new MessagingListener() {
            @Override public void onPublished(Destination d) { throw new RuntimeException("boom"); }
        };
        var safeBus = new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(), bad);
        when(transport.publish(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        assertThatCode(() -> safeBus.publish(Queue.of("q"), new Message(new byte[0], Map.of())).join())
            .doesNotThrowAnyException();
    }
}
