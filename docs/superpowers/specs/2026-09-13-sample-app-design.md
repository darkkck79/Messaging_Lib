# Sample app: CLI conformance validator

Sub-project 1 of 2+. The broker adapters (JMS on Artemis and IBM MQ, then Kafka; PLAN.md steps 5–8) are later sub-projects, each with its own spec and plan. Each one registers into the slot this app defines.

## Purpose

`messaging-sample-app` is a runnable CLI that lets a person check the conformance suite by hand. It shows each scenario passing against a correct transport and failing against a deliberately broken one.

## Decisions

| Decision | Choice | Why |
|---|---|---|
| What the app runs | The existing `AbstractMessagingConformanceTest` subclasses, through the JUnit Platform Launcher | One source of truth, so the app and the build can't drift. This is the pattern `MetaConformanceTest` already uses |
| Interface | CLI (`application` plugin) | Simplest option, and scriptable |
| Transports now | `in-memory`, plus the three faulty variants | JMS and Kafka adapters don't exist yet |
| Brokers later | An existing broker URL, not Testcontainers | User choice. Restart and network-cut scenarios stay in each adapter's `integrationTest` |
| Skipped or aborted scenario | Reported as FAIL | Matches the root build's skip-is-failure listener |

## Changes to `messaging-conformance`

Move these from `src/test` to `src/testFixtures`, keeping the package `com.messaging.conformance.faulty`:
- `InMemoryTransport`
- `AcksBeforeHandlerTransport`, `QueueFansOutTransport`, `DropsHeadersTransport`
- `AcksBeforeHandlerConformance`, `QueueFansOutConformance`, `DropsHeadersConformance`

Deleted as unused: `FaultyTransportProvider` and its `META-INF/services` file (nothing connected through `faulty://`, and in testFixtures the services file would register a stray scheme for every consumer), plus `AbstractFaultyConformanceTest`, which duplicated the in-memory wiring. The faulty suites now extend `InMemoryConformance` and override its `createTransport()` hook. `QueueFansOutTransport` overrides `InMemoryTransport.fansOut(Destination)` rather than copying `subscribe`.

Then:
- Move the body of `InMemoryConformanceTest` into a public testFixtures class `com.messaging.conformance.InMemoryConformance`. `src/test` keeps `class InMemoryConformanceTest extends InMemoryConformance {}`, so `./gradlew test` still runs the control suite.
- Each faulty `*Conformance` declares `public static final String EXPECTED_FAILURE`, naming the scenario it must fail. `MetaConformanceTest` and the app both read these constants.
- Delete the `excludeTestsMatching` filter from `build.gradle`. The `test` task never scans testFixtures, so the filter would be dead.

## `messaging-sample-app`

Package `com.messaging.sample`, two classes.

**`SuiteRunner`**: `List<ScenarioResult> run(Class<?> suite)`. It runs the suite through the Launcher and collects one `ScenarioResult(name, passed, message)` per test method. `message` is the failure message collapsed to one line and truncated to 160 characters, or empty. A container that fails to start is also reported as a failed result.

**`SampleApp`**: a registry `Map<String, Target>` with `Target(Class<?> suite, String expectedFailure)`:

| Name | Suite | Expected failure |
|---|---|---|
| `in-memory` | `InMemoryConformance` | none, so every scenario must pass |
| `faulty-acks-before-handler` | `AcksBeforeHandlerConformance` | `AcksBeforeHandlerConformance.EXPECTED_FAILURE` |
| `faulty-queue-fans-out` | `QueueFansOutConformance` | `QueueFansOutConformance.EXPECTED_FAILURE` |
| `faulty-drops-headers` | `DropsHeadersConformance` | `DropsHeadersConformance.EXPECTED_FAILURE` |

Arguments:
- `--transport <name|all>` (default `all`)
- `--list`, which prints the target names

An unknown name exits 1 and lists the valid names.

Output, one block per target:

```
== faulty-drops-headers ==
  PASS  bodyRoundTrip
  FAIL  headersRoundTrip  Expecting map ... to contain entries ...
  ...
  fault CAUGHT (headersRoundTrip failed as expected)
```

**Exit code** is 0 only when:
- every target without an expected failure passes every scenario, **and**
- every target with an expected failure fails that scenario (`fault CAUGHT`).

Otherwise it's 1, and `fault MISSED` is printed. The app therefore follows the #1 E2E rule: it can't report green against a broken transport, and it can't report green when a known fault goes undetected.

## Adapter slot (sub-projects 2+)

**Decided by the JMS adapter spec (`2026-09-13-jms-adapter-design.md`):** `SampleApp.Target`
gained a `broker` flag. Broker targets (`jms-artemis`, `jms-ibm-mq`) require both `--url
<jms://user:pass@host:port>` and `--admin-url <http(s)://user:pass@host:port>` — missing
either exits 1 — and are excluded from `--transport all`, so the no-argument run stays
Docker-free. Each broker target's suite class (`ExternalArtemisConformance`,
`ExternalIbmMqConformance`, both in `messaging-jms`'s test fixtures) reads those two values
back from the `messaging.sample.url` / `messaging.sample.admin-url` system properties that
`SampleApp` sets before running the suite, and provisions destinations through the
matching `BrokerAdmin`. Kafka's target will follow the same shape when its spec lands.

## Error handling

- A suite that fails to launch or discover tests is reported as a single FAIL for that target, and the exit code is 1.
- Bad arguments print usage to stderr and exit 1.

## Testing

`messaging-sample-app/src/test`:
- `in-memory`: all PASS, with one result per `@Test` method in the abstract suite (counted by reflection).
- `faulty-drops-headers`: `headersRoundTrip` is FAIL and the verdict is CAUGHT.
- `SampleApp.report` with fixed results (no suite launch): an expected failure that passed gives MISSED, and a failing scenario on a control target fails it. This proves both verdicts can fail.
- An unknown argument and an unknown `--transport` both exit 1.

Manual check: break redelivery in `InMemoryTransport` by removing `offerFirst`. `run` must show FAIL and exit 1.
