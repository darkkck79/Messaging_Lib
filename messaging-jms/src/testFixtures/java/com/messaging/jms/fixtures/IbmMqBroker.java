package com.messaging.jms.fixtures;

import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;

import java.util.UUID;

/** Builds the {@code MessagingConfig} plus the matching {@link BrokerAdmin} for an IBM MQ
 * broker at {@code host:port}, queue manager {@code QM1}, channel {@code DEV.APP.SVRCONN}. */
public final class IbmMqBroker {
    private IbmMqBroker() {}

    public static MessagingConfig config(String host, int port, String user, String password, BusSettings settings) {
        return MessagingConfig.builder()
            .url("jms://" + user + ":" + password + "@" + host + ":" + port)
            .clientId("ibmmq-" + UUID.randomUUID())
            .concurrency(settings.concurrency())
            .closeTimeout(settings.closeTimeout())
            .listener(settings.listener())
            .property("connection-factory", "com.ibm.mq.jakarta.jms.MQConnectionFactory")
            .property("hostName", host)
            .property("port", String.valueOf(port))
            .property("channel", "DEV.APP.SVRCONN")
            .property("queueManager", "QM1")
            .property("transportType", "1")
            .property("clientReconnectOptions", "16777216")
            .property("clientReconnectTimeout", "30")
            .build();
    }

    public static BrokerAdmin admin(String adminUrl) {
        return new IbmMqAdmin(adminUrl);
    }
}
