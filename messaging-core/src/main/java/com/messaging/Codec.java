package com.messaging;

public interface Codec<T> {
    byte[] encode(T message);

    T decode(byte[] bytes);
}
