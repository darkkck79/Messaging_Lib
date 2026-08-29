package com.messaging.spi;

import com.messaging.config.MessagingConfig;

public interface TransportProvider {
    Transport create(String destinationName, MessagingConfig config);

    static TransportProvider lookup(MessagingConfig config) {
        String providerName = config.transportProperties().getOrDefault("provider", "default");
        return ServiceLoader.load(TransportProvider.class, TransportProvider.class.getClassLoader())
            .stream()
            .filter(p -> p.supports(providerName) || providerName.equals("default"))
            .findFirst()
            .orElse(TransportProvider.DEFAULT);
    }

    boolean supports(String providerName);
}
