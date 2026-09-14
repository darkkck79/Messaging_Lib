package com.messaging.jms.fixtures;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;

import java.net.URI;

/**
 * Shared base for {@link ExternalArtemisConformance} and {@link ExternalIbmMqConformance}:
 * runs {@link AbstractMessagingConformanceTest} against an external broker named by the
 * {@code messaging.sample.url}/{@code messaging.sample.admin-url} system properties. Each
 * subclass supplies only the provider-specific {@link BrokerAdmin}, config builder, and
 * default credentials (used when the URL carries none).
 */
public abstract class ExternalJmsConformance extends AbstractMessagingConformanceTest implements JmsRedeliveredScenario {

    private BrokerAdmin admin;
    private MessageBus lastBus;

    protected abstract BrokerAdmin admin(String adminUrl);
    protected abstract MessagingConfig config(String host, int port, String user, String password, BusSettings settings);
    protected abstract String defaultUser();
    protected abstract String defaultPassword();

    @Override
    protected final MessageBus createBus(BusSettings settings) {
        String url = System.getProperty("messaging.sample.url");
        String adminUrl = System.getProperty("messaging.sample.admin-url");
        if (admin == null) admin = admin(adminUrl);

        URI uri = URI.create(url);
        String[] userInfo = uri.getUserInfo() != null
            ? uri.getUserInfo().split(":", 2) : new String[]{defaultUser(), defaultPassword()};
        var config = config(uri.getHost(), uri.getPort(),
            userInfo[0], userInfo.length > 1 ? userInfo[1] : "", settings);
        lastBus = Messaging.connect(config);
        return lastBus;
    }

    @Override protected final Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected final Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override public final MessageBus jmsBus() { return lastBus; }
    @Override public final Destination jmsQueue(String name) { return provisionQueue(name); }
}
