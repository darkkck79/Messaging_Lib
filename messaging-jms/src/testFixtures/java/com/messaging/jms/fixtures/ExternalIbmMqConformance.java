package com.messaging.jms.fixtures;

import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;

/** Runs {@link com.messaging.conformance.AbstractMessagingConformanceTest} against an
 * external IBM MQ broker — used by the sample app's {@code jms-ibm-mq} target. */
public final class ExternalIbmMqConformance extends ExternalJmsConformance {

    @Override protected BrokerAdmin admin(String adminUrl) { return IbmMqBroker.admin(adminUrl); }

    @Override
    protected MessagingConfig config(String host, int port, String user, String password, BusSettings settings) {
        return IbmMqBroker.config(host, port, user, password, settings);
    }

    @Override protected String defaultUser() { return "app"; }
    @Override protected String defaultPassword() { return "passw0rd"; }
}
