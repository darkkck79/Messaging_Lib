package com.messaging.sample;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

/**
 * Runs one conformance suite class through the JUnit Platform Launcher and reports one
 * result per scenario. Skipped and aborted scenarios count as failures, matching the
 * root build's skip-is-failure rule.
 */
final class SuiteRunner {

    record ScenarioResult(String name, boolean passed, String message) {}

    private static final int MAX_MESSAGE = 160;

    private SuiteRunner() {}

    static List<ScenarioResult> run(Class<?> suite) {
        List<ScenarioResult> results = new ArrayList<>();
        TestExecutionListener listener = new TestExecutionListener() {
            @Override
            public void executionSkipped(TestIdentifier id, String reason) {
                if (id.isTest()) results.add(new ScenarioResult(name(id), false, "SKIPPED: " + reason));
            }

            @Override
            public void executionFinished(TestIdentifier id, TestExecutionResult result) {
                boolean passed = result.getStatus() == TestExecutionResult.Status.SUCCESSFUL;
                // Containers are reported only when they fail (e.g. the suite class cannot start).
                if (id.isTest() || !passed) {
                    results.add(new ScenarioResult(name(id), passed,
                        result.getThrowable().map(SuiteRunner::summary).orElse("")));
                }
            }
        };
        LauncherFactory.create().execute(request().selectors(selectClass(suite)).build(), listener);
        return results;
    }

    private static String name(TestIdentifier id) {
        return id.getSource()
            .filter(MethodSource.class::isInstance)
            .map(source -> ((MethodSource) source).getMethodName())
            .orElse(id.getDisplayName());
    }

    /**
     * The first assertion failure in the cause chain (Awaitility wraps the real assertion
     * in a timeout), else the failure itself — on one line, truncated for the console.
     */
    private static String summary(Throwable failure) {
        Throwable shown = failure;
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof AssertionError) {
                shown = t;
                break;
            }
        }
        String message = shown.getMessage() == null ? "" : shown.getMessage().replaceAll("\\s+", " ").strip();
        if (message.isEmpty()) return shown.getClass().getSimpleName();
        return message.length() <= MAX_MESSAGE ? message : message.substring(0, MAX_MESSAGE) + "...";
    }
}
