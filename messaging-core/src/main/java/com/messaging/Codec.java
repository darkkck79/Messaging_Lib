package com.messaging;

@FunctionalInterface
public interface Codec<T> {
    byte[] encode(T message);

    T decode(byte[] bytes);
}
