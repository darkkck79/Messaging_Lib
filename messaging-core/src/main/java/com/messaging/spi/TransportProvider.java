package com.messaging.spi;

import com.messaging.config.MessagingConfig;

import java.util.ServiceLoader;

public interface TransportProvider {
    Transport create(String destinationName, MessagingConfig config);

    boolean supports(String providerName);

    static TransportProvider lookup(MessagingConfig config) {
        String providerName = config.transportProperties().getOrDefault("provider", "default");
        return ServiceLoader.load(TransportProvider.class, TransportProvider.class.getClassLoader())
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> providerName.equals("default") || p.supports(providerName))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No TransportProvider found for: " + providerName));
    }
}
