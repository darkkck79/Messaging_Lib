# JMS transport adapter (Artemis + IBM MQ) — design & plan outline

## Context

`messaging-jms` is an empty shell (only `build.gradle`); so is `messaging-kafka`. The build order in `PLAN.md` puts JMS next (steps 5–6), and the existing implementation plan (`docs/superpowers/plans/2026-08-21-messaging-lib.md` Tasks 5–6) only sketches it — and its sketch hard-codes Artemis/IBM MQ class names, contradicting `PLAN.md`'s "no provider classes, no provider constants". Exploring the repo also surfaced conformance-suite and core defects that real brokers would expose or that make tests vacuous (CLAUDE.md #1 rule).

Outcome: a JMS adapter that passes the shared conformance suite (plus connectivity tests) against **Artemis and IBM MQ with no adapter changes between them**, the suite fixed so it can actually fail, and the sample app able to run the suite against an external Artemis or IBM MQ broker.

Classification: **architectural** (new adapter against a provisional SPI, plus suite/spec changes). After approval: write this design to `docs/superpowers/specs/2026-09-13-jms-adapter-design.md` for review, then invoke writing-plans for the step-by-step TDD plan (which supersedes Tasks 5–6 of the 2026-08-21 plan). Kafka is a separate spec/plan later.

## Decisions (from Q&A)

| Topic | Decision |
|---|---|
| Scope | JMS only: Artemis + IBM MQ integration tests, suite defect fixes, sample-app broker targets |
| ConnectionFactory | Configured FQCN: `messaging.jms.connection-factory=<class>`; every other `messaging.jms.*` key applied as a bean setter. Adapter compiles against `jakarta.jms-api` only |
| Missing destination | Suite asserts **both** Topic and Queue variants fail, for publish and subscribe |
| Sample app | External broker URL for **both** Artemis and IBM MQ; provisioning via each broker's HTTP admin API |
| New scenarios | In-flight close scenarios; publish-during-outage fails. (Header edge cases: not now) |

## Adapter design — `messaging-jms/src/main/java/com/messaging/jms/`

- **`JmsTransportProvider`** — scheme `jms`; registered in `META-INF/services/com.messaging.spi.TransportProvider`.
- **`ConnectionFactoryBuilder`** (package-private, unit-tested without a broker) — instantiates the configured FQCN via its no-arg constructor, applies remaining passthrough props through `set<Key>` setters (String/int/long/boolean coercion), error naming key on unknown setter. Rejects `deliveryMode=NON_PERSISTENT`/`1` at connect time naming the key (§E).
- **`JmsTransport`**
  - Connect: `createConnection(user, pass)` from URL userinfo (URL host is not used — broker address lives in factory props; documented), bounded by `connectTimeout`; `setClientID(clientId)`; `ExceptionListener` → `onConnectionStateChanged(DISCONNECTED)`; `start()` → `CONNECTED`.
  - Publish (§E): one non-transacted publish `Session` + unidentified producer, guarded by a lock (never touched by consumer threads). `BytesMessage`, headers → string properties, `PERSISTENT`. Synchronous send → completed/failed future; failures wrapped in `MessagingException` naming the destination.
  - Subscribe (§C, §D): creates `config.concurrency()` `JmsConsumerUnit`s; future completes when all consumers exist; any failure closes the others and fails with `MessagingException` naming the destination. Returned `Subscription.close()` is idempotent.
  - `close(timeout)` (§J): idempotent; stop all units against one deadline, then close publish session and connection.
- **`JmsConsumerUnit`** — the owner loop (§B). Its own thread creates its transacted `Session` + `MessageConsumer` (then completes the ready future), and loops `receive(500ms)`:
  - Map to `Message` (BytesMessage only; non-bytes treated as handler failure — documented); add `messaging.redelivered=true` when `getJMSRedelivered()` (§G).
  - Invoke handler; sync throw / `null` / exceptional / cancelled → failure (§A). Wait for outcome on the owner thread in 500 ms slices so stop is observed.
  - Success → `commit()` → `onConsumed`. Failure → `rollback()` → `onError`. A failed commit (e.g. failover rollback) → `onError`; broker redelivers.
  - Stop: stop receiving; wait for in-flight outcome until deadline; then cancel the handler's future → `rollback()`; close consumer/session. Only cross-thread calls: `close()` (§B).
- Known limitation, documented in spec §H: Jakarta API exposes no reconnect event, so `RECONNECTED` is not emitted by JMS; `DISCONNECTED` fires only when the client gives up.

## Conformance suite & core fixes (existing defects)

`messaging-core`
- `MessagingConfig.fromProperties` — resolve `messaging.url` before other keys (hash iteration order currently rejects `messaging.jms.*` as unknown). Test with an insertion-ordered `Properties` placing the scheme key first.
- Delete dead `transportProperties` / `transportProperty` (unused, untested).
- `toString` redaction prints `:-1` when URL has no port — fix.

`messaging-conformance`
- Hook change: `createBus(MessagingListener listener, Duration closeTimeout)`; base uses no-op + short timeout.
- `listenerThrowDoesNotChangeDelivery` — currently installs no throwing listener (vacuous). Build a bus with a listener that throws on every callback; publish two messages, assert both delivered. `InMemoryTransport` gets the listener and fires `onConsumed` so the in-memory run isn't vacuous either.
- `publishToMissingDestinationFails` / `subscribeToMissingDestinationFails` — Topic **and** Queue variants.
- New base scenarios: `subscriptionCloseWaitsForInFlightHandler`, `subscriptionCloseCancelsStuckHandlerUnsettled` (redelivered to a new subscription), `busCloseCancelsStuckHandler` (close returns ≈ timeout; handler future cancelled).
- `InMemoryTransport` — implement §J close (wait for in-flight up to timeout, then cancel and re-offer) instead of interrupt-immediately, so the control run satisfies the new scenarios.
- `AbstractMessagingConnectivityTest`
  - Fix `queueMessagesPublishedDuringOutageDeliveredAfterRecovery` (currently publishes *before* the cut): new hook `publishOutOfBand(Destination, byte[])` (fixture publishes via a bus that bypasses the proxy) — subscribe, cut, publish out-of-band, restore, assert delivery.
  - New: `publishDuringOutageFails` (bounded; network restored in `finally`), `busCloseRedeliversInFlightOnNextStart`.

## Test fixtures — `messaging-jms/src/testFixtures` (shared by integrationTest and sample app)

- `BrokerAdmin` — `createQueue(name)` / `createTopic(name)` → broker-legal `Destination`, with a per-run unique suffix so tests never share destinations.
  - `ArtemisAdmin` — Jolokia over `java.net.http` (`createQueue` ANYCAST / `createAddress` MULTICAST).
  - `IbmMqAdmin` — MQ REST `mqsc` endpoint: `DEFINE QLOCAL('DEV.<NAME>')` (names sanitised: uppercase, `-`→`_`, ≤48 chars — covered by dev-image `DEV.**` app authority); topics map to `dev/<name>` (no admin call). Dev image's self-signed TLS → trust-all client, fixtures/sample only.
  - Missing destinations: Artemis with auto-create disabled rejects both; IBM MQ rejects queues as unknown (2085) and non-`dev/` topics as unauthorized (2035) — spec §F amended to say so.
- `artemis/broker.xml` override (auto-create queues/addresses off), reused by containers and README's `docker run`.
- `ArtemisBroker` / `IbmMqBroker` — build `MessagingConfig` (factory FQCN + props: Artemis `brokerURL=tcp://h:p?reconnectAttempts=-1&callFailoverTimeout=…`; MQ `hostName, port, channel=DEV.APP.SVRCONN, queueManager=QM1, transportType=1, clientReconnectOptions`) and the matching admin.
- JMS-only scenario `redeliveredHeaderSetOnRedelivery` as a JUnit `@Test` default method on an interface, implemented by both fixture families (no duplication).
- `ExternalArtemisConformance` / `ExternalIbmMqConformance extends AbstractMessagingConformanceTest` — read URL/admin URL from system properties (for the sample app).

`messaging-jms/src/integrationTest`: `ArtemisConformanceTest`, `IbmMqConformanceTest extends AbstractMessagingConnectivityTest` — Testcontainers broker + Toxiproxy on a shared network (bus through proxy, out-of-band bus direct). `restartBroker` = docker stop/start of the same container (storage survives; proxy alias keeps address stable). Images pinned to catalog versions, not `:latest`.

## Sample app

- Targets `jms-artemis`, `jms-ibm-mq`; run only when named (`all` stays Docker/broker-free).
- New options `--url <jms://user:pass@host:port>` and `--admin-url <http(s)://user:pass@host:port>`, required for broker targets; passed to the suite class via system properties.
- Deps: `implementation testFixtures(project(':messaging-jms'))`, `runtimeOnly` Artemis and IBM MQ Jakarta clients.
- Tests: arg validation (broker target without `--url` exits 1); existing tests unchanged.

## Docs to update

Spec `2026-08-18-messaging-lib-design.md` (§F missing-topic semantics per provider, §H RECONNECTED limitation, §I `messaging.jms.*` keys and URL usage), `2026-09-13-sample-app-design.md` (adapter slot decided), `messaging-sample-app/README.md` (targets, options, `docker run` commands for both brokers), `PLAN.md` verification lines, `2026-08-21` plan (Tasks 5–6 marked superseded), `architecture.json/html` if module/fixture structure it shows changed.

## Implementation order (to be expanded by writing-plans, TDD throughout)

1. Core fixes (config order, dead code, toString).
2. Suite hook/scenario changes + `InMemoryTransport` §J close/listener; `test` green, meta-test green.
3. `ConnectionFactoryBuilder` + §E rejection (unit tests, no Docker).
4. `JmsTransport` + `JmsConsumerUnit` + provider registration.
5. Fixtures + Artemis integration test green.
6. IBM MQ fixture against the **unchanged** adapter — any adapter change is recorded as a finding.
7. Sample app targets/options.
8. Docs; `/simplify`; full verification; commit only after verification.

## Verification

- **Prerequisite: Docker Desktop running** (currently not).
- `gradlew.bat test` — core/unit, `ConnectionFactoryBuilder`, meta-test, in-memory control incl. new scenarios, sample-app tests.
- `gradlew.bat :messaging-jms:integrationTest` — every scenario on Artemis and IBM MQ, zero skips (mechanically enforced).
- Break checks (proves tests can fail): remove `rollback()` → redelivery scenarios fail; skip handler-future cancel on stop → stuck-handler scenario fails; restore the vacuous listener test's old body → confirm new version fails when `Messaging.wrapSafe` isolation is removed.
- Sample app manual run: start both brokers with README `docker run` commands; `run --args="--transport jms-artemis --url … --admin-url …"` and same for `jms-ibm-mq` → exit 0; stop broker → exit 1.
- No UI changes, so chrome-devtools verification is N/A.

## Risks to confirm early (not blockers)

- Artemis Jolokia MBean broker name / `Origin` header requirements in 2.43.
- IBM MQ publish-during-outage bound: with client reconnect, sends may block for `MQReconnectTimeout` (default 1800 s) — may need `mqclient.ini` via `MQCLNTCF`; if not portably boundable, record as finding rather than loosen the test.
- Suite `TIMEOUT = 5s` may be tight for IBM MQ; only change with evidence.
