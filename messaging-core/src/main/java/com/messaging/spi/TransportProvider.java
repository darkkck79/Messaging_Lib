package com.messaging.spi;

import com.messaging.MessagingException;
import com.messaging.config.MessagingConfig;

import java.net.URI;
import java.util.ServiceLoader;

public interface TransportProvider {
    Transport create(String destinationName, MessagingConfig config);

    boolean supports(String providerName);

    static TransportProvider lookup(MessagingConfig config) {
        String providerName = config.transportProperties().getOrDefault("provider", config.url().getScheme());
        return ServiceLoader.load(TransportProvider.class, TransportProvider.class.getClassLoader())
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> p.supports(providerName))
            .findFirst()
            .orElseThrow(() -> new MessagingException(
                "No TransportProvider found for scheme '" + providerName + "': " + config.url()));
    }
}
