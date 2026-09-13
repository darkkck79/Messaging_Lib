package com.messaging.spi;

import com.messaging.MessagingListener;
import com.messaging.config.MessagingConfig;

public interface TransportProvider {
    String scheme();
    Transport open(MessagingConfig config, MessagingListener listener);
}
