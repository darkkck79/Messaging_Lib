package com.messaging.jms.fixtures;

import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;

import java.util.UUID;

/** Builds the {@code MessagingConfig} plus the matching {@link BrokerAdmin} for an
 * Artemis broker at {@code host:port}. */
public final class ArtemisBroker {
    private ArtemisBroker() {}

    public static MessagingConfig config(String host, int port, String user, String password, BusSettings settings) {
        return MessagingConfig.builder()
            .url("jms://" + user + ":" + password + "@" + host + ":" + port)
            .clientId("artemis-" + UUID.randomUUID())
            .concurrency(settings.concurrency())
            .closeTimeout(settings.closeTimeout())
            .listener(settings.listener())
            .property("connection-factory", "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory")
            .property("brokerURL", "tcp://" + host + ":" + port
                + "?reconnectAttempts=-1&callFailoverTimeout=30000&callTimeout=30000")
            .build();
    }

    public static BrokerAdmin admin(String adminUrl) {
        return new ArtemisAdmin(adminUrl);
    }
}
