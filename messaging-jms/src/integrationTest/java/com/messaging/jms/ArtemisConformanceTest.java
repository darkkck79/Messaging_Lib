package com.messaging.jms;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;
import com.messaging.jms.fixtures.ArtemisAdmin;
import com.messaging.jms.fixtures.BrokerAdmin;
import com.messaging.jms.fixtures.JmsRedeliveredScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;

/**
 * Proves the JMS adapter against a real Artemis broker (§B, §C, §D, §E, §F, §G, §J), connecting
 * directly to the container's mapped ports rather than through Toxiproxy.
 *
 * <p><b>Known limitation (recorded per plan's Phase 6-style rule for unresolved risks):</b> the
 * network-outage/reconnection scenarios from {@code AbstractMessagingConnectivityTest} (§K —
 * {@code cutNetwork}/{@code restoreNetwork}) are NOT exercised here. In this environment
 * (Docker Desktop for Windows, npipe transport), routing the JMS connection through a
 * Testcontainers Toxiproxy proxy on a custom bridge network reproducibly fails during the
 * Artemis client's initial topology handshake (repeated {@code AMQ219006: Channel disconnected}
 * until connect-timeout), while the identical connection made directly to the broker's
 * mapped port succeeds immediately. This was isolated with standalone diagnostics (direct
 * connect: reliable; Toxiproxy-proxied connect: fails) — the adapter and broker are not at
 * fault. See the spec's Risks section for the full record. The §K reconnection behavior
 * itself is still exercised by {@code JmsTransport}'s {@code ExceptionListener}-driven
 * {@code DISCONNECTED} notification, which is unit-testable without a real network cut.
 */
@Testcontainers
class ArtemisConformanceTest extends AbstractMessagingConformanceTest implements JmsRedeliveredScenario {

    private static final GenericContainer<?> ARTEMIS = new GenericContainer<>(
            DockerImageName.parse("apache/activemq-artemis:2.43.0"))
        .withExposedPorts(61616, 8161)
        .withEnv("ARTEMIS_USER", "artemis")
        .withEnv("ARTEMIS_PASSWORD", "artemis")
        .withClasspathResourceMapping("artemis/broker.xml",
            "/var/lib/artemis-instance/etc-override/broker.xml", BindMode.READ_ONLY)
        .waitingFor(Wait.forHttp("/console/").forPort(8161).forStatusCodeMatching(code -> code < 500)
            .withStartupTimeout(Duration.ofSeconds(120)));

    private static BrokerAdmin admin;

    @BeforeAll
    static void startContainer() {
        ARTEMIS.start();
        admin = new ArtemisAdmin("http://artemis:artemis@"
            + ARTEMIS.getHost() + ":" + ARTEMIS.getMappedPort(8161));
    }

    @AfterAll
    static void stopContainer() {
        ARTEMIS.stop();
    }

    @Override
    protected MessageBus createBus(BusSettings settings) {
        String host = ARTEMIS.getHost();
        int port = ARTEMIS.getMappedPort(61616);
        var config = MessagingConfig.builder()
            .url("jms://artemis:artemis@" + host + ":" + port)
            .clientId("artemis-test-" + UUID.randomUUID())
            .concurrency(settings.concurrency())
            .connectTimeout(Duration.ofSeconds(20))
            .closeTimeout(settings.closeTimeout())
            .listener(settings.listener())
            .property("connection-factory", "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory")
            .property("brokerURL", "tcp://" + host + ":" + port)
            .build();
        return Messaging.connect(config);
    }

    @Override protected Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override public MessageBus jmsBus() { return bus; }
    @Override public Destination jmsQueue(String name) { return provisionQueue(name); }
}
