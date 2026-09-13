package com.messaging.conformance;

import com.messaging.conformance.faulty.AcksBeforeHandlerConformance;
import com.messaging.conformance.faulty.DropsHeadersConformance;
import com.messaging.conformance.faulty.QueueFansOutConformance;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

/**
 * Proves the conformance suite itself catches real faults: each deliberately broken
 * in-memory transport must make a specific test method FAIL when run through it. A
 * suite that cannot fail on a broken transport would be worthless as a portability
 * check for real adapters (JMS, Kafka).
 */
class MetaConformanceTest {

    @Test
    void acksBeforeHandlerTransportFailsRedeliveryTest() {
        assertMethodFails(AcksBeforeHandlerConformance.class, "redeliveryOnExceptionalFuture");
    }

    @Test
    void queueFansOutTransportFailsCompetingConsumersTest() {
        assertMethodFails(QueueFansOutConformance.class, "queueCompetingConsumers");
    }

    @Test
    void dropsHeadersTransportFailsHeadersRoundTripTest() {
        assertMethodFails(DropsHeadersConformance.class, "headersRoundTrip");
    }

    private void assertMethodFails(Class<?> testClass, String methodName) {
        LauncherDiscoveryRequest discoveryRequest = request()
            .selectors(selectMethod(testClass, methodName))
            .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(discoveryRequest);

        TestExecutionSummary summary = listener.getSummary();
        assertThat(summary.getTestsFailedCount())
            .as("%s#%s must FAIL to prove the fault is caught", testClass.getSimpleName(), methodName)
            .isGreaterThan(0);
    }
}
