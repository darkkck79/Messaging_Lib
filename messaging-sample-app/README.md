# messaging-sample-app

A CLI for validating the conformance suite by hand. It runs every scenario in `AbstractMessagingConformanceTest` against each registered transport and prints a PASS/FAIL line per scenario. These are the same assertions the build enforces, run through the JUnit Platform Launcher rather than a second copy, so the app can't drift from `./gradlew test`.

Design: [`docs/superpowers/specs/2026-09-13-sample-app-design.md`](../docs/superpowers/specs/2026-09-13-sample-app-design.md)

## Run

```sh
./gradlew :messaging-sample-app:run                                          # all targets
./gradlew :messaging-sample-app:run --args="--transport faulty-drops-headers" # one target
./gradlew :messaging-sample-app:run --args="--list"                          # target names
```

No Docker is needed.

## Targets

| Target | Transport | Passes when |
|---|---|---|
| `in-memory` | Correct in-memory transport | every scenario passes |
| `faulty-acks-before-handler` | Settles before the handler runs, so nothing is redelivered | `redeliveryOnExceptionalFuture` fails |
| `faulty-queue-fans-out` | Queue delivers to every subscriber | `queueCompetingConsumers` fails |
| `faulty-drops-headers` | Strips headers before the handler | `headersRoundTrip` fails |

A faulty target's expected failure is the `EXPECTED_FAILURE` constant on its `*Conformance` class. `MetaConformanceTest` uses the same constant. Other scenarios that depend on the same broken behaviour may fail too; for example, all four redelivery scenarios fail on `faulty-acks-before-handler`.

## Output

```
== faulty-drops-headers ==
  PASS  bodyRoundTrip
  FAIL  headersRoundTrip  Expecting map: {} to contain entries: ["key1"="value1"] but could not find ...
  ...
  fault CAUGHT (headersRoundTrip failed as expected)
RESULT: every target behaved as expected
```

Skipped or aborted scenarios are reported as FAIL.

## Exit code

- **0**: `in-memory` passed every scenario, and every faulty target failed its expected scenario (`fault CAUGHT`).
- **1**: anything else. That includes a failing `in-memory` scenario, a fault that went undetected (`fault MISSED`), a suite that ran no scenarios, and bad arguments.

To see the app catch a real regression, delete the `deque.offerFirst(message)` redelivery line in `InMemoryTransport`. `in-memory` then reports four redelivery FAILs and the run exits 1.

## Adding a broker adapter

Once the JMS and Kafka adapters exist, each one adds a `Target` entry to `SampleApp`. The entry points at a suite class that connects with `Messaging.connect` against an external broker URL. The first adapter also adds the CLI option for that URL. Brokers are external, so restart and network-cut scenarios (`AbstractMessagingConnectivityTest`) stay in each adapter's Testcontainers `integrationTest`.
