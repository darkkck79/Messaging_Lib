package com.messaging.jms.fixtures;

import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;

/** Runs {@link com.messaging.conformance.AbstractMessagingConformanceTest} against an
 * external Artemis broker — used by the sample app's {@code jms-artemis} target. */
public final class ExternalArtemisConformance extends ExternalJmsConformance {

    @Override protected BrokerAdmin admin(String adminUrl) { return ArtemisBroker.admin(adminUrl); }

    @Override
    protected MessagingConfig config(String host, int port, String user, String password, BusSettings settings) {
        return ArtemisBroker.config(host, port, user, password, settings);
    }

    @Override protected String defaultUser() { return "artemis"; }
    @Override protected String defaultPassword() { return "artemis"; }
}
