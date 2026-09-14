# messaging-sample-app

A CLI for validating the conformance suite by hand. It runs every scenario in `AbstractMessagingConformanceTest` against each registered transport and prints a PASS/FAIL line per scenario. These are the same assertions the build enforces, run through the JUnit Platform Launcher rather than a second copy, so the app can't drift from `./gradlew test`.

Design: [`docs/superpowers/specs/2026-09-13-sample-app-design.md`](../docs/superpowers/specs/2026-09-13-sample-app-design.md)

## Run

```sh
./gradlew :messaging-sample-app:run                                          # all targets
./gradlew :messaging-sample-app:run --args="--transport faulty-drops-headers" # one target
./gradlew :messaging-sample-app:run --args="--list"                          # target names
```

No Docker is needed for `--transport all` or any in-memory/faulty target.

## Targets

| Target | Transport | Passes when |
|---|---|---|
| `in-memory` | Correct in-memory transport | every scenario passes |
| `faulty-acks-before-handler` | Settles before the handler runs, so nothing is redelivered | `redeliveryOnExceptionalFuture` fails |
| `faulty-queue-fans-out` | Queue delivers to every subscriber | `queueCompetingConsumers` fails |
| `faulty-drops-headers` | Strips headers before the handler | `headersRoundTrip` fails |
| `jms-artemis` | Real Artemis broker (requires `--url`/`--admin-url`) | every scenario passes |
| `jms-ibm-mq` | Real IBM MQ broker (requires `--url`/`--admin-url`) | every scenario passes |

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

## Broker targets

`jms-artemis` and `jms-ibm-mq` run the conformance suite against a real, externally
running broker instead of the in-memory transport. Both require `--url` and `--admin-url`;
neither runs as part of `--transport all`.

```sh
# Artemis (see messaging-jms's integrationTest for the pinned image version)
docker run --rm -p 61616:61616 -p 8161:8161 \
  -e ARTEMIS_USER=artemis -e ARTEMIS_PASSWORD=artemis \
  apache/activemq-artemis:2.43.0

./gradlew :messaging-sample-app:run --args="--transport jms-artemis \
  --url jms://artemis:artemis@localhost:61616 \
  --admin-url http://artemis:artemis@localhost:8161"

# IBM MQ (dev image; LICENSE=accept is required)
docker run --rm -p 1414:1414 -p 9443:9443 \
  -e LICENSE=accept -e MQ_QMGR_NAME=QM1 -e MQ_APP_PASSWORD=passw0rd \
  icr.io/ibm-messaging/mq:9.4.5.1-r1

./gradlew :messaging-sample-app:run --args="--transport jms-ibm-mq \
  --url jms://app:passw0rd@localhost:1414 \
  --admin-url https://app:passw0rd@localhost:9443"
```

With the broker stopped, the same command exits 1.

## Adding a broker adapter

JMS is done (`jms-artemis`, `jms-ibm-mq` above). Kafka's target is the remaining item: it
adds a `Target` entry to `SampleApp` pointing at a suite class that connects with
`Messaging.connect` against an external broker URL, following the same `broker`-flag /
`--url` / `--admin-url` shape JMS established. Brokers are external, so restart and
network-cut scenarios (`AbstractMessagingConnectivityTest`) stay in each adapter's
Testcontainers `integrationTest`.
