package com.messaging;

import com.messaging.config.MessagingConfig;
import com.messaging.internal.DefaultMessageBus;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;
import java.util.*;

public final class Messaging {
    private Messaging() {}

    public static MessageBus connect(MessagingConfig config) {
        String scheme = config.scheme();
        MessagingListener listener = com.messaging.internal.SafeListener.wrap(config.listener());
        TransportProvider provider = resolveProvider(scheme);
        Transport transport = provider.open(config, listener);
        return new DefaultMessageBus(transport, config, listener);
    }

    private static TransportProvider resolveProvider(String scheme) {
        List<TransportProvider> matches = new ArrayList<>();
        Map<String, String> available = new LinkedHashMap<>();
        ClassLoader tcl = Thread.currentThread().getContextClassLoader();
        ClassLoader fallback = Messaging.class.getClassLoader();
        for (ClassLoader cl : tcl != fallback ? List.of(tcl, fallback) : List.of(tcl)) {
            try {
                for (TransportProvider p : ServiceLoader.load(TransportProvider.class, cl)) {
                    available.put(p.scheme(), p.getClass().getName());
                    if (scheme.equals(p.scheme())) matches.add(p);
                }
            } catch (ServiceConfigurationError e) {
                throw new MessagingException("Malformed TransportProvider on classpath: " + e.getMessage(), e);
            }
            if (!matches.isEmpty()) break;
        }
        if (matches.isEmpty())
            throw new MessagingException("No TransportProvider for scheme '" + scheme + "'. Available: " + available);
        if (matches.size() > 1)
            throw new MessagingException("Duplicate TransportProviders for scheme '" + scheme + "': "
                + matches.stream().map(p -> p.getClass().getName()).toList());
        return matches.getFirst();
    }
}
