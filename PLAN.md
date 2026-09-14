# Messaging_Lib — Phase 1: core API, conformance suite, JMS & Kafka adapters

**Behavioural contracts live in [`docs/superpowers/specs/2026-08-18-messaging-lib-design.md`](docs/superpowers/specs/2026-08-18-messaging-lib-design.md).** This file is the build plan. Section references below (§A–§M) point there. Nothing about delivery, threading, or lifecycle semantics is restated here — one source, no drift.

## Context

`D:\projects\Messaging_Lib` is empty apart from `CLAUDE.md` and is not yet a git repo. We are building a Java 21 messaging library that wraps multiple broker transports behind one API, driven by two goals:

1. **Swap transports without app changes** — the transport is a config choice, so the common API is a deliberate lowest-common-denominator.
2. **One connectivity layer for many apps** — teams on different brokers share the same config, connection handling, and observability.

Target transports: **Kafka, JMS (Artemis, IBM MQ), RabbitMQ/AMQP, STOMP-over-WebSocket, Solace**. Raw (non-STOMP) WebSocket was dropped during design: it has no broker semantics — no queues, no competing consumers, nothing to redeliver — so it cannot satisfy the conformance suite or the portability promise.

**Phase 1 (this plan)** builds the core API, the SPI, the conformance suite, and proves them against **JMS and Kafka** — deliberately the two most dissimilar models (push broker with true queues and transactional settlement vs. pull-based partitioned log with offsets). The JMS adapter is proven against **two unrelated providers, Artemis and IBM MQ**, because a portable JMS adapter tested on one provider is just that provider's adapter with a misleading name.

`PLAN_REVIEW.md` reviewed the first draft of this plan. Its five critical findings and every High/Medium gap are resolved in the design doc; the coverage table is that document's appendix.

## What changed after review

| Was | Now | Why |
|---|---|---|
| `Reconnector` in core | Deleted. Adapters own the connection lifecycle (§K) | Core had no connection-state signal, and a generic reconnect loop fights each client's native recovery |
| Handler future drives acks | Owner-loop rule (§B) | `KafkaConsumer` is not thread-safe; a JMS `Session` is single-threaded |
| `concurrency` = parallel records | `concurrency` = parallel consumer units, one message in flight each (§C) | Out-of-order commits turn at-least-once into at-most-once on Kafka |
| `INDIVIDUAL_ACKNOWLEDGE` | Transacted sessions, `jakarta.jms-api` only, tested on Artemis **and IBM MQ** | `INDIVIDUAL_ACKNOWLEDGE` is an Artemis extension, not Jakarta JMS |
| "ephemeral topic" | Subscription lifetime defined; topic-during-outage is an explicit portability exception; queue-during-outage is guaranteed (§D) | The two adapters could not otherwise both be correct |
| `Message.id` | Removed (§G) | Kafka has no equivalent; the field could only lie on one transport |
| SPI frozen after Phase 1 | Provisional through Phase 2 | Two adapters is thin evidence for five transports |
| Manual "break an adapter and see" | Faulty in-memory transports + a meta-test | A manual edit is not a durable acceptance gate |

## Module layout

Gradle multi-module, Groovy DSL, Java 21 toolchain.

```
settings.gradle
build.gradle                      // shared config for all subprojects
gradle/libs.versions.toml         // every version pinned
gradlew, gradlew.bat, gradle/wrapper/   // pinned 8.10.x

messaging-core/          API + SPI + config + DefaultMessageBus.  Dep: slf4j-api only
messaging-conformance/   Abstract suite (java-test-fixtures) + faulty in-memory transports
messaging-jms/           jakarta.jms-api only. Fixtures: Artemis AND IBM MQ — implemented, see 2026-09-13-jms-adapter-design.md
messaging-kafka/         kafka-clients
messaging-codec-json/    Codec<T> via Jackson
messaging-sample-app/    CLI that runs the conformance suite per transport and prints PASS/FAIL (see its README)
```

Phase 2+: `messaging-amqp`, `messaging-stomp-ws`, `messaging-solace`, `messaging-spring-boot-starter`. Core stays framework-free.

**ActiveMQ Classic is not a target.** Artemis plus IBM MQ is a strictly stronger portability pair, and a third full broker run costs more time than it proves.

## messaging-core

Public API (`com.example.messaging`):

| Type | Shape |
|---|---|
| `Messaging` | `static MessageBus connect(MessagingConfig)` — `ServiceLoader` resolution per §I |
| `MessageBus extends AutoCloseable` | `publish(Destination, byte[])`, `publish(Destination, Message)`, `subscribe(Destination, MessageHandler)`, `<T> typed(Destination, Codec<T>)` |
| `Destination` | Sealed interface; `Topic` and `Queue` records with `of(String)` |
| `Message` | Record of `byte[] body` + `Map<String,String> headers` — no `id` (§G) |
| `MessageHandler` | `@FunctionalInterface CompletableFuture<Void> handle(Message)` |
| `Subscription extends AutoCloseable` | `close()` ends the lifetime (§D, §J) |
| `TypedChannel<T>`, `Codec<T>` | §L |
| `MessagingListener`, `ConnectionState` | §H |
| `MessagingException` | Unchecked; adapters wrap client exceptions in it |

Config (`com.example.messaging.config`): `MessagingConfig` with `fromProperties(Properties)`, `parse(String uri)`, a builder, and namespaced passthrough (`messaging.<scheme>.*`). Keys, precedence, duration format, and redaction: §I.

SPI (`com.example.messaging.spi`): `TransportProvider` and `Transport`, exactly as in §K. This is the only thing adapters implement.

Internal: `DefaultMessageBus` wraps a `Transport`, fires listener callbacks with exception isolation (§H), and builds typed channels — keeping adapters free of cross-cutting concerns. There is no `Reconnector`.

## Adapters

**messaging-jms** — `jakarta.jms:jakarta.jms-api` on the compile classpath and nothing else. No provider classes, no provider constants.

- One transacted `Session` per consumer unit, driven by a synchronous `receive()` loop, one message in flight (§B, §C). `commit()` on handler success, `rollback()` on failure.
- `Topic` → non-durable topic subscriber. `Queue` → JMS queue consumer. `PERSISTENT` sends (§E).
- *Artemis fixture*: `apache/activemq-artemis`, URL `tcp://…?reconnectAttempts=-1`.
- *IBM MQ fixture*: `icr.io/ibm-messaging/mq`, `LICENSE=accept`, `MQ_QMGR_NAME=QM1`, `MQ_APP_PASSWORD`, port 1414, channel `DEV.APP.SVRCONN`, destinations `DEV.QUEUE.1..3` and topic string `dev/`. Client `com.ibm.mq:com.ibm.mq.jakarta.client` is **test-scope only**. Reconnect via `WMQ_CLIENT_RECONNECT`. The image is large and slow to start — note it in the docs and budget CI time for it.

**messaging-kafka** — `org.apache.kafka:kafka-clients`.

- `enable.auto.commit=false`, `allow.auto.create.topics=false` (§F), `acks=all` (§E).
- Commit only after the handler outcome reaches the poll loop; `pause()`/`resume()` keeps the loop alive while a handler is in flight; `seek()` back to the failed offset on failure (§B).
- `Topic` → group `<client-id>-<uuid>`, `auto.offset.reset=latest` → fan-out, ephemeral. `Queue` → group = queue name → competing consumers.
- One in-flight record per partition; `concurrency` spawns consumers, capped by partition count (§C).

## messaging-conformance

One abstract JUnit 5 class every adapter extends, exposed through the **`java-test-fixtures`** plugin so JUnit never reaches a production artifact. Adapters consume it with `testImplementation testFixtures(project(':messaging-conformance'))`.

The fixture supplies `bus()`, `topic()`, `queue()`, `secondQueue()`, `restartBroker()`, and `cutNetwork()`. Destination *names* come from the fixture, never from the suite (§F) — IBM MQ's developer image dictates its own.

**Scenarios**

*Payload and headers* — body round trip · empty-body round trip · headers round trip incl. Unicode and empty values · header at the size limit · invalid header name rejected · reserved prefix (`JMS`, `messaging.`) rejected on publish.

*Patterns* — topic fans out with both subscribers confirmed ready first · queue competition over N messages with no simultaneous delivery of one attempt to two consumers.

*Settlement (§A)* — redelivery on exceptional future · on synchronous throw · on `null` future · on cancelled future · successful handler not redelivered, asserted duplicate-tolerantly.

*Lifecycle (§D, §J)* — `subscribe` future means ready-for-delivery · subscription close stops delivery · close during an in-flight handler · bus close cancels in-flight work without settling · bus close during reconnect backoff returns promptly · repeated `close()` is idempotent.

*Failure surfaces* — publish to a missing destination fails · subscribe to a missing destination fails · a listener that throws does not change delivery (§H).

*Connectivity* — queue survives broker restart, **including messages published during the outage** · publish during outage fails · reconnect and resume after a transient disconnect.

*Ordering* — `concurrency = 1` preserves order.

Kafka adds: per-partition ordering across multiple partitions, and rebalance during in-flight work. JMS adds: `messaging.redelivered` is set on a redelivery (§G) — Kafka cannot assert this, which is why it is adapter-specific rather than shared.

The review also asked for out-of-order-completion and multiple-in-flight-per-session scenarios. Both are **structurally impossible** under §C — one in-flight message per consumer unit — so there is nothing to assert. That is the point of the constraint, and the README says so rather than leaving a reader to wonder why the tests are absent.

**Restart and disconnect are separate tests.** Broker restart uses container stop/start over a persistent volume, so broker identity and storage survive — otherwise the test proves a fresh broker works, not that reconnection does. Transient connectivity loss uses the Testcontainers **Toxiproxy** module, which cuts the network without touching broker state.

**The negative control is a real test, not a manual step.** `messaging-conformance` ships deliberately faulty in-memory transports — `AcksBeforeHandlerTransport`, `QueueFansOutTransport`, `DropsHeadersTransport` — and a meta-test asserting the suite **fails** against each. It runs in the fast `test` task, needs no Docker, and needs no source edits. This is the durable form of `CLAUDE.md`'s #1 E2E rule: a suite that stays green against a knowingly broken transport is worthless, and now the build says so.

## Build configuration

1. **Wrapper pinned at Gradle 8.10.x** — Java 21 as both toolchain and runtime. `gradlew`, `gradlew.bat`, and wrapper metadata are committed. Every plugin and dependency version lives in `gradle/libs.versions.toml`.
2. **Two verification tasks.** `test` — unit tests plus the negative-control meta-test; fast, no Docker. `integrationTest` — a separate source set running the conformance suite against real brokers; Docker required, generous timeouts. `check` depends on both.
3. **Skip-is-failure is mechanical.** A root-build `TestListener` fails the task on any `SKIPPED` result, verified once against a deliberately `@Disabled` fixture. Prose cannot enforce policy; a listener can.
4. **`/simplify` is workflow, not build tooling.** It stays in the contributor instructions, paired with `./gradlew check` as the repeatable gate.

## Build order

1. `git init`, `.gitignore`. Commit the design doc and this plan **before any code**.
2. Gradle skeleton: `settings.gradle`, root `build.gradle`, version catalog, wrapper.
3. **`messaging-core`** — API, config, SPI, `DefaultMessageBus`. Unit tests for URI/property parsing, precedence, duration parsing, credential redaction, and `ServiceLoader` resolution including the missing-provider, duplicate-scheme, and malformed-provider paths (§I).
4. **`messaging-conformance`** — the suite, the faulty transports, and the meta-test. The suite proves itself before any real adapter exists.
5. **`messaging-jms` + Artemis fixture.**
6. **IBM MQ fixture against the unchanged adapter** — the portability proof. Any adapter change needed here is a finding to record, not a chore to absorb quietly.
7. **`messaging-kafka` + fixture.** Expect the SPI to strain here. Fix the SPI rather than special-casing Kafka.
8. **`messaging-codec-json`** — decode failure follows the handler-failure contract (§L).
9. **Contract-mapping spike** for AMQP, STOMP, and Solace: map each to §A–§M and record where it does not fit. Only then decide whether the SPI can be frozen.
10. **Docs** — `README.md` (quickstart, config table, per-transport semantics table, and the ordering / delivery / poison-message / retention caveats) and `docs/adding-a-transport.md`.
11. Run `/simplify` across the phase before committing, per `CLAUDE.md`.

## Verification

- `./gradlew test` — compiles everything, runs unit tests, and runs the negative-control meta-test. No Docker.
- `./gradlew integrationTest` — the conformance suite against Artemis, IBM MQ, and Kafka. **Docker must be running.** All three must pass every scenario; a skipped scenario fails the build mechanically, not by convention.
- `./gradlew :messaging-sample-app:run` — manual validation: runs the conformance suite against the in-memory transport (must pass every scenario) and each faulty transport (must fail its `EXPECTED_FAILURE` scenario); exits non-zero otherwise. `jms-artemis`/`jms-ibm-mq` register against an external broker via `--url`/`--admin-url` (spec: `docs/superpowers/specs/2026-09-13-sample-app-design.md`, `docs/superpowers/specs/2026-09-13-jms-adapter-design.md`); Kafka's target will follow the same shape.
- **Cross-transport portability check** — one example that publishes and subscribes, run unchanged against all three brokers by changing only `messaging.url` and the adapter jar on the classpath. This is the product claim; verify it directly rather than inferring it from compilation.
- **Contract cross-check** — every scenario in the suite traces to a section of the design doc, and every observable rule in §A–§M has a scenario. A rule with no test is either unenforced or shouldn't be in the contract.

## Out of scope for Phase 1, with reasons

| Item | Why not now |
|---|---|
| Contiguous-success watermark commits | Removed by the one-in-flight-per-partition constraint (§C) |
| Provider-specific JMS capability modules | No second ack strategy exists to select between |
| ActiveMQ Classic as a target | Artemis + IBM MQ is the stronger portability pair |
| Per-subscription concurrency | Config-level suffices; adding it later is compatible |
| RabbitMQ/AMQP, STOMP-over-WebSocket, Solace adapters | Phase 2, after the contract-mapping spike (step 9) |
| Spring Boot starter, Micrometer metrics | Designed for, built later; core stays framework-free |
| Request/reply, explicit ack API, durable topic subscriptions, retry/DLQ | Deliberately outside the lowest-common-denominator API |
