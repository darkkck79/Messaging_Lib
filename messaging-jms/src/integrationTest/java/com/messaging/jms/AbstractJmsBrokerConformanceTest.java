package com.messaging.jms;

import com.messaging.Destination;
import com.messaging.MessageBus;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.jms.fixtures.BrokerAdmin;
import com.messaging.jms.fixtures.JmsRedeliveredScenario;

/**
 * Shared shape for a JMS broker's direct-connect conformance test: only container lifecycle
 * and {@link #createBus} differ between brokers (see {@link ArtemisConformanceTest} and
 * {@link IbmMqConformanceTest}); provisioning and the {@link JmsRedeliveredScenario} wiring
 * do not.
 */
abstract class AbstractJmsBrokerConformanceTest extends AbstractMessagingConformanceTest
        implements JmsRedeliveredScenario {

    protected static BrokerAdmin admin;

    @Override protected final Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected final Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override public final MessageBus jmsBus() { return bus; }
    @Override public final Destination jmsQueue(String name) { return provisionQueue(name); }
}
