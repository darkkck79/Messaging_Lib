package com.messaging.jms.fixtures;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.conformance.BusSettings;

import java.net.URI;

/** Runs {@link AbstractMessagingConformanceTest} against an external Artemis broker named
 * by system properties — used by the sample app's {@code jms-artemis} target. */
public class ExternalArtemisConformance extends AbstractMessagingConformanceTest implements JmsRedeliveredScenario {

    private BrokerAdmin admin;
    private MessageBus lastBus;

    @Override
    protected MessageBus createBus(BusSettings settings) {
        String url = System.getProperty("messaging.sample.url");
        String adminUrl = System.getProperty("messaging.sample.admin-url");
        if (admin == null) admin = ArtemisBroker.admin(adminUrl);

        URI uri = URI.create(url);
        String[] userInfo = uri.getUserInfo() != null
            ? uri.getUserInfo().split(":", 2) : new String[]{"artemis", "artemis"};
        var config = ArtemisBroker.config(uri.getHost(), uri.getPort(),
            userInfo[0], userInfo.length > 1 ? userInfo[1] : "", settings);
        lastBus = Messaging.connect(config);
        return lastBus;
    }

    @Override protected Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override public MessageBus jmsBus() { return lastBus; }
    @Override public Destination jmsQueue(String name) { return provisionQueue(name); }
}
