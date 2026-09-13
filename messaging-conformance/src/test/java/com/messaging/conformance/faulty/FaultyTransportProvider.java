package com.messaging.conformance.faulty;

import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;

/**
 * Single provider, scheme "faulty", selecting the deliberately broken transport variant
 * by the connection URL's host — e.g. {@code faulty://acks-before-handler}.
 */
public final class FaultyTransportProvider implements TransportProvider {

    @Override
    public String scheme() { return "faulty"; }

    @Override
    public Transport open(MessagingConfig config, MessagingListener listener) {
        String variant = config.url().getHost();
        return switch (variant) {
            case "acks-before-handler" -> new AcksBeforeHandlerTransport();
            case "queue-fans-out" -> new QueueFansOutTransport();
            case "drops-headers" -> new DropsHeadersTransport();
            case null, default -> throw new MessagingException("Unknown faulty transport variant: " + variant);
        };
    }
}
