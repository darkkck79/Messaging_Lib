package com.messaging.jms;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.conformance.BusSettings;
import com.messaging.jms.fixtures.BrokerAdmin;
import com.messaging.jms.fixtures.IbmMqAdmin;
import com.messaging.jms.fixtures.IbmMqBroker;
import com.messaging.jms.fixtures.JmsRedeliveredScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves the JMS adapter — with **zero code changes from the Artemis run**
 * ({@link ArtemisConformanceTest}) — against a real IBM MQ broker. Connects directly to the
 * container's mapped ports, the same direct-connect pattern used for Artemis, for the same
 * reason: see {@link ArtemisConformanceTest}'s javadoc for the recorded Toxiproxy limitation
 * on this environment. The §K network-outage/reconnection scenarios are therefore not
 * exercised here either.
 *
 * <p><b>Not run in this environment:</b> no IBM MQ container/license is available where this
 * test was authored. It has NOT been executed. Before trusting it, run it against a real IBM
 * MQ dev image per {@code docs/superpowers/manual-tests/2026-09-15-ibmmq-conformance.md} and
 * report the outcome — per the plan's Phase 6 rule, if a scenario fails and the fix would
 * require touching {@code messaging-jms/src/main}, stop and record it as a risk rather than
 * changing the adapter to special-case IBM MQ.
 */
@Testcontainers
class IbmMqConformanceTest extends AbstractMessagingConformanceTest implements JmsRedeliveredScenario {

    private static final GenericContainer<?> IBM_MQ = new GenericContainer<>(
            DockerImageName.parse("icr.io/ibm-messaging/mq:9.4.5.1-r1"))
        .withExposedPorts(1414, 9443)
        .withEnv("LICENSE", "accept")
        .withEnv("MQ_QMGR_NAME", "QM1")
        .withEnv("MQ_APP_PASSWORD", "passw0rd")
        .waitingFor(Wait.forLogMessage(".*QMNAME\\(QM1\\).*STATUS\\(Running\\).*\\n", 1));

    private static BrokerAdmin admin;

    @BeforeAll
    static void startContainer() {
        IBM_MQ.start();
        admin = new IbmMqAdmin("https://app:passw0rd@" + IBM_MQ.getHost() + ":" + IBM_MQ.getMappedPort(9443));
    }

    @AfterAll
    static void stopContainer() {
        IBM_MQ.stop();
    }

    @Override
    protected MessageBus createBus(BusSettings settings) {
        var config = IbmMqBroker.config(IBM_MQ.getHost(), IBM_MQ.getMappedPort(1414), "app", "passw0rd", settings);
        return Messaging.connect(config);
    }

    @Override protected Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override public MessageBus jmsBus() { return bus; }
    @Override public Destination jmsQueue(String name) { return provisionQueue(name); }
}
