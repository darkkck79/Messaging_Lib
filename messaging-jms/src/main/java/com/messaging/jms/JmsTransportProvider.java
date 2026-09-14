package com.messaging.jms;

import com.messaging.MessagingListener;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;

/** Registers the {@code jms} scheme. The listener handed to {@link #open} is already
 * isolated by {@code Messaging.connect}, so this adapter never wraps it again. */
public final class JmsTransportProvider implements TransportProvider {

    @Override public String scheme() { return "jms"; }

    @Override public Transport open(MessagingConfig config, MessagingListener listener) {
        return JmsTransport.connect(config, listener);
    }
}
