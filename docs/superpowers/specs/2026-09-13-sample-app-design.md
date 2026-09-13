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

An adapter adds a registry entry whose suite class builds its bus with `Messaging.connect` against an external broker URL. The first adapter adds the CLI option for that URL and decides how it reaches the suite; the app has no `--url` until then (YAGNI). Only `AbstractMessagingConformanceTest` scenarios run against external brokers. How destinations get named and provisioned on each broker (for example IBM MQ's `DEV.QUEUE.1`) is decided in that adapter's spec.

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
