package com.messaging.internal;

import com.messaging.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TypedChannelTest {
    @Mock MessageBus bus;
    static final Codec<String> CODEC = new Codec<>() {
        @Override public byte[] encode(String v) { return v.getBytes(StandardCharsets.UTF_8); }
        @Override public String decode(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    };

    @Test void publishEncodesValue() {
        when(bus.publish(any(Destination.class), any(Message.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        new TypedChannel<>(bus, Queue.of("q"), CODEC).publish("hello");
        var captor = ArgumentCaptor.forClass(Message.class);
        verify(bus).publish(eq(Queue.of("q")), captor.capture());
        assertThat(captor.getValue().body()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
    }

    @Test void subscribeDecodesValue() throws Exception {
        var handlerCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        when(bus.subscribe(eq(Queue.of("q")), handlerCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(mock(Subscription.class)));
        var received = new CompletableFuture<String>();
        new TypedChannel<>(bus, Queue.of("q"), CODEC)
            .subscribe((value, headers) -> { received.complete(value); return CompletableFuture.completedFuture(null); });
        handlerCaptor.getValue().handle(new Message("decoded".getBytes(StandardCharsets.UTF_8), Map.of("h", "v")));
        assertThat(received.get()).isEqualTo("decoded");
    }

    @Test void decodeFailureIsHandlerFailure() throws Exception {
        Codec<String> badCodec = new Codec<>() {
            @Override public byte[] encode(String v) { return v.getBytes(); }
            @Override public String decode(byte[] b) { throw new RuntimeException("decode failed"); }
        };
        var handlerCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        when(bus.subscribe(any(), handlerCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(mock(Subscription.class)));
        new TypedChannel<>(bus, Queue.of("q"), badCodec)
            .subscribe((v, h) -> CompletableFuture.completedFuture(null));
        assertThatThrownBy(() -> handlerCaptor.getValue().handle(new Message("data".getBytes(), Map.of())))
            .hasMessageContaining("decode failed");
    }
}
