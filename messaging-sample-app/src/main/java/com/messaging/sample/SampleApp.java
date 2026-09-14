package com.messaging.sample;

import com.messaging.conformance.InMemoryConformance;
import com.messaging.conformance.faulty.AcksBeforeHandlerConformance;
import com.messaging.conformance.faulty.DropsHeadersConformance;
import com.messaging.conformance.faulty.QueueFansOutConformance;
import com.messaging.jms.fixtures.ExternalArtemisConformance;
import com.messaging.jms.fixtures.ExternalIbmMqConformance;
import com.messaging.sample.SuiteRunner.ScenarioResult;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CLI that runs the conformance suite against each registered transport and prints a
 * PASS/FAIL line per scenario. In-memory targets need nothing; broker targets
 * (`jms-artemis`, `jms-ibm-mq`) require `--url` and `--admin-url` and are never included
 * in `--transport all`, so `all` stays Docker/broker-free.
 */
public final class SampleApp {

    /** A suite to run; {@code expectedFailure == null} means every scenario must pass.
     * {@code broker} targets require {@code --url}/{@code --admin-url} and are excluded
     * from {@code all}. */
    record Target(Class<?> suite, String expectedFailure, boolean broker) {
        Target(Class<?> suite, String expectedFailure) { this(suite, expectedFailure, false); }
    }

    private static final Map<String, Target> TARGETS = new LinkedHashMap<>();
    static {
        TARGETS.put("in-memory", new Target(InMemoryConformance.class, null));
        TARGETS.put("faulty-acks-before-handler",
            new Target(AcksBeforeHandlerConformance.class, AcksBeforeHandlerConformance.EXPECTED_FAILURE));
        TARGETS.put("faulty-queue-fans-out",
            new Target(QueueFansOutConformance.class, QueueFansOutConformance.EXPECTED_FAILURE));
        TARGETS.put("faulty-drops-headers",
            new Target(DropsHeadersConformance.class, DropsHeadersConformance.EXPECTED_FAILURE));
        TARGETS.put("jms-artemis", new Target(ExternalArtemisConformance.class, null, true));
        TARGETS.put("jms-ibm-mq", new Target(ExternalIbmMqConformance.class, null, true));
    }

    private SampleApp() {}

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        String transport = "all";
        String url = null;
        String adminUrl = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--list" -> {
                    TARGETS.keySet().forEach(out::println);
                    return 0;
                }
                case "--transport" -> {
                    if (++i == args.length) return usage(err, "--transport needs a value");
                    transport = args[i];
                }
                case "--url" -> {
                    if (++i == args.length) return usage(err, "--url needs a value");
                    url = args[i];
                }
                case "--admin-url" -> {
                    if (++i == args.length) return usage(err, "--admin-url needs a value");
                    adminUrl = args[i];
                }
                default -> { return usage(err, "Unknown argument: " + args[i]); }
            }
        }
        boolean all = transport.equals("all");
        if (!all && !TARGETS.containsKey(transport)) return usage(err, "Unknown transport: " + transport);

        if (!all && TARGETS.get(transport).broker()) {
            if (url == null || adminUrl == null) {
                return usage(err, "Broker target '" + transport + "' requires --url and --admin-url");
            }
            System.setProperty("messaging.sample.url", url);
            System.setProperty("messaging.sample.admin-url", adminUrl);
        }

        List<Map.Entry<String, Target>> targets = all
            ? TARGETS.entrySet().stream().filter(e -> !e.getValue().broker()).toList()
            : List.of(Map.entry(transport, TARGETS.get(transport)));

        boolean ok = true;
        for (var entry : targets) {
            out.println("== " + entry.getKey() + " ==");
            ok &= report(entry.getValue(), SuiteRunner.run(entry.getValue().suite()), out);
        }
        out.println(ok ? "RESULT: every target behaved as expected" : "RESULT: FAILED");
        return ok ? 0 : 1;
    }

    /** Prints one line per scenario plus the target's verdict; true when the target behaved as expected. */
    static boolean report(Target target, List<ScenarioResult> results, PrintStream out) {
        for (var r : results) {
            out.printf("  %s  %s%s%n", r.passed() ? "PASS" : "FAIL", r.name(),
                r.message().isEmpty() ? "" : "  " + r.message());
        }
        if (results.isEmpty()) {
            out.println("  no scenarios ran");
            return false;
        }
        if (target.expectedFailure() == null) {
            long failed = results.stream().filter(r -> !r.passed()).count();
            out.println(failed == 0 ? "  all " + results.size() + " scenarios passed" : "  " + failed + " scenario(s) FAILED");
            return failed == 0;
        }
        boolean caught = results.stream()
            .anyMatch(r -> !r.passed() && r.name().equals(target.expectedFailure()));
        out.printf("  fault %s (%s %s)%n", caught ? "CAUGHT" : "MISSED", target.expectedFailure(),
            caught ? "failed as expected" : "did not fail against a broken transport");
        return caught;
    }

    private static int usage(PrintStream err, String problem) {
        err.println(problem);
        err.println("Usage: SampleApp [--transport <name|all>] [--url <jms://user:pass@host:port>] "
            + "[--admin-url <http(s)://user:pass@host:port>] [--list]");
        err.println("Transports: " + String.join(", ", TARGETS.keySet()));
        return 1;
    }
}
