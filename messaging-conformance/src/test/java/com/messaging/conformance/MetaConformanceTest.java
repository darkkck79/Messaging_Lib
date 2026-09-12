package com.messaging.conformance;

import com.messaging.conformance.faulty.AcksBeforeHandlerConformance;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

/**
 * Proves the conformance suite itself catches real faults: running it against a
 * transport that acknowledges before invoking the handler must FAIL, not pass.
 */
class MetaConformanceTest {

    @Test void acksBeforeHandlerFaultIsDetected() {
        LauncherDiscoveryRequest discoveryRequest = request()
            .selectors(selectClass(AcksBeforeHandlerConformance.class))
            .build();
        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(discoveryRequest);

        TestExecutionSummary summary = listener.getSummary();
        assertThat(summary.getTotalFailureCount())
            .as("faulty AcksBeforeHandlerTransport must fail the conformance suite")
            .isGreaterThan(0);
    }
}
