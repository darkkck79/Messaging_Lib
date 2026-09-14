package com.messaging.sample;

import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.conformance.InMemoryConformance;
import com.messaging.sample.SuiteRunner.ScenarioResult;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SampleAppTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private int run(String... args) {
        return SampleApp.run(args, new PrintStream(out, true), new PrintStream(err, true));
    }

    @Test
    void inMemoryPassesEveryScenarioInTheSuite() {
        long scenarioCount = Arrays.stream(AbstractMessagingConformanceTest.class.getDeclaredMethods())
            .filter(m -> m.isAnnotationPresent(Test.class))
            .count();

        var results = SuiteRunner.run(InMemoryConformance.class);

        assertThat(results).hasSize((int) scenarioCount);
        assertThat(results).allSatisfy(r ->
            assertThat(r.passed()).as("%s: %s", r.name(), r.message()).isTrue());
    }

    @Test
    void dropsHeadersFaultIsReportedAsCaught() {
        int exit = run("--transport", "faulty-drops-headers");

        assertThat(exit).isZero();
        assertThat(out.toString())
            .containsPattern("FAIL\\s+headersRoundTrip")
            .contains("fault CAUGHT");
    }

    @Test
    void undetectedFaultIsReportedAsMissed() {
        var target = new SampleApp.Target(InMemoryConformance.class, "bodyRoundTrip");

        boolean ok = SampleApp.report(target, List.of(new ScenarioResult("bodyRoundTrip", true, "")),
            new PrintStream(out, true));

        assertThat(ok).isFalse();
        assertThat(out.toString()).contains("fault MISSED");
    }

    @Test
    void failingScenarioOnCorrectTransportFailsTheTarget() {
        var target = new SampleApp.Target(InMemoryConformance.class, null);

        boolean ok = SampleApp.report(target, List.of(
                new ScenarioResult("bodyRoundTrip", true, ""),
                new ScenarioResult("headersRoundTrip", false, "boom")),
            new PrintStream(out, true));

        assertThat(ok).isFalse();
        assertThat(out.toString()).containsPattern("FAIL\\s+headersRoundTrip\\s+boom");
    }

    @Test
    void unknownArgumentIsRejected() {
        int exit = run("--bogus-flag");

        assertThat(exit).isEqualTo(1);
        assertThat(err.toString()).contains("Unknown argument: --bogus-flag");
    }

    @Test
    void brokerTargetWithoutUrlExitsOne() {
        int exit = run("--transport", "jms-artemis");

        assertThat(exit).isEqualTo(1);
        assertThat(err.toString()).contains("--url").contains("--admin-url");
    }

    @Test
    void brokerTargetWithoutAdminUrlExitsOne() {
        int exit = run("--transport", "jms-artemis", "--url", "jms://user:pass@localhost:61616");

        assertThat(exit).isEqualTo(1);
    }

    @Test
    void urlWithNonBrokerTargetIsAccepted() {
        int exit = run("--transport", "in-memory", "--url", "jms://user:pass@localhost:61616");

        assertThat(exit).isEqualTo(0);
    }

    @Test
    void unknownTransportFailsAndListsValidNames() {
        int exit = run("--transport", "nope");

        assertThat(exit).isEqualTo(1);
        assertThat(err.toString()).contains("in-memory").contains("faulty-drops-headers");
    }
}
