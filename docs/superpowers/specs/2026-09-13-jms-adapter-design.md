# JMS transport adapter (Artemis + IBM MQ)

Status: **draft, awaiting review.** Sub-project 2 of the sample-app design (`2026-09-13-sample-app-design.md`, "Adapter slot"). Covers `PLAN.md` build steps 5–6. This spec replaces Tasks 5–6 of `docs/superpowers/plans/2026-08-21-messaging-lib.md`; Kafka gets its own spec later.

Section references (§A–§M) point to the binding contract `2026-08-18-messaging-lib-design.md`. Nothing from that contract is restated here except where this spec amends it (see [Contract amendments](#contract-amendments)).

## Goal

A JMS adapter that passes the shared conformance suite and the connectivity tests against **Artemis and IBM MQ, with no adapter changes between the two**. Along the way, fix the suite so its scenarios can actually fail, and let the sample app run the suite against an external Artemis or IBM MQ broker.

## Decisions

| Topic | Decision | Why |
|---|---|---|
| Scope | JMS only: Artemis + IBM MQ integration tests, fixes to existing suite and core defects, sample-app broker targets | Kafka is a separate spec |
| ConnectionFactory | Set by class name in `messaging.jms.connection-factory=<FQCN>`. Every other `messaging.jms.*` key is applied as a bean setter on that factory. The adapter compiles against `jakarta.jms-api` only | `PLAN.md`: "no provider classes, no provider constants" |
| Missing destination | The suite checks that **Topic and Queue** both fail, for both publish and subscribe | The current tests only try a Topic |
| Sample app | Takes an external broker URL for **both** Artemis and IBM MQ, and provisions destinations through each broker's HTTP admin API | Brokers are external (sample-app spec) |
| New scenarios | In-flight close scenarios; publish during an outage fails; concurrency > 1 (see below) | §J, §E and §C rules that currently have no test |
| Header edge cases | Not now | Deferred by the user; see [Risks](#risks) |

## Adapter — `messaging-jms/src/main/java/com/messaging/jms/`

### `JmsTransportProvider`
Uses scheme `jms` and is registered in `META-INF/services/com.messaging.spi.TransportProvider`. `open(config, listener)` returns a connected `JmsTransport`, or throws `MessagingException` if the connection fails.

### `ConnectionFactoryBuilder` (package-private)
- Instantiates `connection-factory` through its public no-arg constructor. If the key is missing, or the class can't be loaded or instantiated, it fails with a `MessagingException` naming the key and the class.
- Applies every other passthrough key `k` through a public single-argument `set<K>` method. The setter's parameter type drives coercion: `String`, `int`/`Integer`, `long`/`Long`, `boolean`/`Boolean`. An unknown setter, or a value that can't be coerced, fails with an error naming the key.
- **§E:** `deliveryMode` set to `NON_PERSISTENT` or `1` is rejected before setter lookup, with an error naming the key and saying persistence is enforced. Neither provider's factory has a `deliveryMode` setter, so without this rule the value would fail with a generic "unknown setter" error instead of the §E reason.
- Unit-tested without a broker, using a stub factory class in `src/test`.

### `JmsTransport`
- **Connect:**
  - `createConnection(user, password)` using the `messaging.url` userinfo, or `createConnection()` if the URL has no userinfo.
  - The URL's host and port are **not used**: the broker address lives in the factory properties, because each provider's address format differs (Artemis `brokerURL`, IBM MQ `hostName`/`port`/`channel`).
  - The connect is bounded by `connect-timeout`. On timeout or failure, any late connection is closed and a `MessagingException` is thrown.
  - Then `setClientID(clientId)`, an `ExceptionListener` that reports `onConnectionStateChanged(DISCONNECTED)`, and `start()`, which reports `CONNECTED`.
- **Publish (§E):**
  - Uses one non-transacted publish `Session` with an unidentified `MessageProducer`, guarded by a lock. Consumer threads never touch it.
  - Sends a `BytesMessage`; headers become string properties; delivery is `PERSISTENT`.
  - The send is synchronous, so the future is already completed or failed when it's returned. Failures are wrapped in a `MessagingException` naming the destination, with the provider exception as the cause.
- **Destinations:** `Topic` maps to `session.createTopic(name)` and `Queue` to `session.createQueue(name)`. Names are passed through unchanged, and the fixtures supply names each broker accepts (§F).
- **Subscribe (§C, §D):**
  - Creates `config.concurrency()` `JmsConsumerUnit`s. The future completes once every unit's consumer exists.
  - If any unit fails, the others are closed and the future fails with a `MessagingException` naming the destination.
  - `Subscription.close()` is idempotent and applies §J using `close-timeout`.
- **Topic consumers:** each `subscribe` call gets its own **shared non-durable subscription** (`createSharedConsumer(topic, "<clientId>-<uuid>")`). Its units compete inside that subscription, so one subscription sees each message once whatever the concurrency, while separate `subscribe` calls still fan out. A plain `createConsumer` per unit would deliver N copies at concurrency N. Queue units use `createConsumer(queue)`.
- **`close(timeout)` (§J):** idempotent. Signals every unit to stop, waits for all of them against one shared deadline, then closes the publish session and the connection. `RECONNECTED` is never reported (see amendments).

### `JmsConsumerUnit` — the owner loop (§B)
Each unit runs on its own thread. That thread creates the transacted `Session` and `MessageConsumer`, completes the unit's ready future, and then loops on `receive(500 ms)`:

1. **Map the message.** A `BytesMessage` becomes a `Message`, with string properties as headers and `messaging.redelivered=true` added when `getJMSRedelivered()` is true (§G). Any other message type counts as a handler failure (documented).
2. **Run the handler on the owner thread.** A synchronous throw, a `null` return, an exceptional future or a cancelled future all count as failure (§A). The unit waits for the outcome in 500 ms slices so it notices a stop request.
3. **Settle on the owner thread.** Success: `commit()`, then `onConsumed`. Failure: `rollback()`, then `onError`. If `commit()` itself throws (for example a transaction rolled back during failover), that's `onError`, and the broker redelivers.
4. **`receive`/`commit` throwing outside settlement** (for example the client gave up reconnecting): report `onError`, pause 500 ms, and keep looping until stopped. The unit never recreates sessions, because the client owns reconnection (§K).
5. **Stop:** stop receiving. Wait for the in-flight outcome until the deadline, then cancel the handler's future and `rollback()`. Close the consumer and the session. The only calls made from another thread are the stop signal, a bounded `Thread.join`, and, as a last resort once the join deadline passes, `session.close()`. §B allows that.

## Core fixes — `messaging-core`

- **`MessagingConfig.fromProperties`:** read `messaging.url` before any other key. Today the loop follows `Properties` hash order, so a `messaging.jms.*` key read first is rejected as unknown. The test uses an insertion-ordered `Properties` that returns the scheme key first; it fails against the current code.
- **Delete** `transportProperties` and `Builder.transportProperty`. Nothing uses or tests them.
- **`toString`:** the redacted URL prints `:-1` when there's no port. Omit the port when it's absent.
- **Listener isolation in one place:** move `Messaging.wrapSafe` into `com.messaging.internal.SafeListener.wrap(listener)`, which returns the same instance if it's already wrapped.
  - `Messaging.connect` passes the wrapped listener to the provider.
  - `DefaultMessageBus` wraps its constructor argument and drops its duplicate try/catch blocks.
  - The in-memory fixture uses the same wrapper, so its listener-throw scenario exercises the real isolation code.

## Conformance suite — `messaging-conformance`

### Hooks
- `createBus()` becomes `createBus(BusSettings settings)`, where `record BusSettings(MessagingListener listener, Duration closeTimeout, int concurrency)`. The default used by `setUp` is `(noOp, 1 s, 1)`.
- Scenarios that need different settings create and close their own extra bus inside the test.

### Scenario changes
| Scenario | Change |
|---|---|
| `listenerThrowDoesNotChangeDelivery` | **Currently vacuous**: it never installs a throwing listener. It will build a bus whose listener throws on every callback, publish two messages to a queue, and check both are delivered once and publish futures complete normally |
| `publishToMissingDestinationFails`, `subscribeToMissingDestinationFails` | Split into Topic and Queue variants (four scenarios) |
| `subscriptionCloseWaitsForInFlightHandler` | New. The handler completes about 300 ms after it starts; `close()` is called while it's in flight. Checks that close blocked until the handler finished, and that the message isn't redelivered to a new subscription |
| `subscriptionCloseCancelsStuckHandlerUnsettled` | New. The handler never completes. Checks that close returns within timeout + slack, the handler future is cancelled, and a new subscription on the queue receives the message again |
| `busCloseCancelsStuckHandler` | New. The handler never completes. Checks that `bus.close()` returns in ≈ `closeTimeout` and the handler future is cancelled |
| `topicConcurrencyDeliversOncePerSubscription` | New (§C). With concurrency 2, publish 4 messages to a topic subscription; exactly 4 deliveries, no duplicates |
| `queueConcurrencyRunsUnitsInParallel` | New (§C). With concurrency 2, two handlers are both in flight at once on one subscription |

### `InMemoryTransport` (the control transport)
- The constructor takes `(MessagingListener listener, Duration closeTimeout, int concurrency)`, the same inputs a real adapter gets from `open(config, listener)`.
- It fires `onConsumed` after settlement and `onError` on failure, so the listener scenario isn't vacuous on the control run.
- **§J close:** stop taking new messages, wait for the in-flight outcome until the deadline, then cancel the handler future and re-offer the message. This replaces the immediate interrupt.
- Starts `concurrency` units per subscription. A topic subscription's units share one private deque.
- `ConsumerUnit` gets a `settle(message, succeeded)` hook. `AcksBeforeHandlerTransport` overrides only that hook (it never re-offers) instead of overriding the whole processing step. That way it can't block forever on a stuck handler when the sample app runs its full suite.

### `AbstractMessagingConnectivityTest`
- **Fix `queueMessagesPublishedDuringOutageDeliveredAfterRecovery`:** today it publishes *before* the cut, so it tests nothing about the outage. Add the hook `publishOutOfBand(Destination, byte[])`, which publishes over a separate bus that bypasses the cut link. The test then: subscribe, cut, publish out of band, restore, and checks the message is delivered.
- **New `publishDuringOutageFails`:** cut the network, publish, and check the future fails within `OUTAGE_PUBLISH_BOUND` (60 s). The network is restored in `finally`.
- **New `busCloseRedeliversInFlightOnNextStart`:** a stuck handler on a queue, then `bus.close()`, then a new bus subscribes, and the message is delivered again.

## Test fixtures

### `messaging-jms/src/testFixtures` — no Testcontainers dependency
The sample app consumes these fixtures, so they must not pull Docker tooling onto its classpath.

- **`BrokerAdmin`** — interface with `Destination createQueue(String name)` and `Destination createTopic(String name)`. Every name gets a per-run unique suffix so tests never share destinations.
  - **`ArtemisAdmin`** — Jolokia over `java.net.http`, with an `Origin` header. It discovers the broker MBean name through Jolokia `search`, then calls `createQueue` (ANYCAST) or `createAddress` (MULTICAST).
  - **`IbmMqAdmin`** — MQ REST `…/admin/action/qmgr/QM1/mqsc`, running `DEFINE QLOCAL('DEV.<NAME>')`. Names are sanitised: uppercase, `-` becomes `_`, at most 48 characters. The dev image's `DEV.**` authority covers the app user. Topics map to topic string `dev/<name>` with no admin call. The dev image's self-signed TLS is handled by a trust-all HTTP client, used only in fixtures and the sample app.
- **`ArtemisBroker` / `IbmMqBroker`** — static factories that take a host, port, credentials and a `BusSettings`, and build the `MessagingConfig` plus the matching admin:
  - Artemis: `connection-factory=org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory`, `brokerURL=tcp://h:p?reconnectAttempts=-1&callFailoverTimeout=30000&callTimeout=30000`
  - IBM MQ: `connection-factory=com.ibm.mq.jakarta.jms.MQConnectionFactory`, `hostName`, `port`, `channel=DEV.APP.SVRCONN`, `queueManager=QM1`, `transportType=1`, `clientReconnectOptions=16777216` (WMQ_CLIENT_RECONNECT), `clientReconnectTimeout=30`
- **`artemis/broker.xml`** — override with auto-create queues and addresses turned off. Both the containers and the README's `docker run` command use it.
- **`JmsRedeliveredScenario`** — an interface with a `@Test default void redeliveredHeaderSetOnRedelivery()`. Implementers provide `jmsBus()` and `jmsQueue(name)`. Both fixture families implement it, so the test isn't duplicated.
- **`ExternalArtemisConformance` / `ExternalIbmMqConformance`** — extend `AbstractMessagingConformanceTest` and implement `JmsRedeliveredScenario`. They read `messaging.sample.url` and `messaging.sample.admin-url` from system properties, and each bus uses a generated client ID.

### `messaging-jms/src/integrationTest`
- **`ArtemisConformanceTest` / `IbmMqConformanceTest`** — extend `AbstractMessagingConnectivityTest` and implement `JmsRedeliveredScenario`.
- **Containers:** Testcontainers runs the broker and Toxiproxy on a shared network. **All host access goes through Toxiproxy proxies:**
  - `bus` — the only one `cutNetwork` cuts
  - `oob` — messaging, never cut; the out-of-band bus uses it
  - `admin` — HTTP
  
  This keeps host ports stable across `restartBroker`.
- **`restartBroker`:** docker stop, then start, on the same container. Its storage survives, and the proxies reach it by network alias.
- **Images** are pinned to the catalog versions, never `:latest`.

## Sample app

- **Targets:** `jms-artemis` → `ExternalArtemisConformance`, and `jms-ibm-mq` → `ExternalIbmMqConformance`. `Target` gains a `broker` flag. Broker targets run only when named, so `all` stays broker-free.
- **Options:** `--url <jms://user:pass@host:port>` and `--admin-url <http(s)://user:pass@host:port>`. Both are required for a broker target, and missing either exits 1. `SampleApp` passes them to the suite as the `messaging.sample.*` system properties.
- **Dependencies:** `implementation testFixtures(project(':messaging-jms'))`, plus `runtimeOnly` Artemis and IBM MQ Jakarta clients.
- **Tests:**
  - A broker target without `--url` or `--admin-url` exits 1.
  - `--url` with a non-broker target is accepted.
  - The existing `unknownArgumentIsRejected` changes to use a genuinely unknown flag.
  - `inMemoryPassesEveryScenarioInTheSuite` still counts the abstract suite's `@Test` methods.

## Contract amendments

These go into `2026-08-18-messaging-lib-design.md`:

- **§F:** what "missing" means per provider. Artemis with auto-create disabled rejects both a missing queue and a missing address. IBM MQ rejects an undefined queue (2085, or 2035 outside `DEV.**`), and rejects a topic string outside the app's authorised tree (2035). Either way, the future fails with a `MessagingException`.
- **§H:** JMS never reports `RECONNECTED`, because the Jakarta API has no reconnect event. `DISCONNECTED` fires only when the client gives up.
- **§I:** add `messaging.jms.connection-factory`, explain bean-setter passthrough, and note that the JMS URL's host and port are unused (only userinfo is read).
- **§C:** a JMS topic subscription is one shared non-durable subscription whose consumer units compete.

## Risks

- **Artemis:** check whether Jolokia in 2.43 requires an `Origin` header or strict CORS, and whether shared non-durable subscriptions work with `auto-create-queues=false`.
- **IBM MQ:** check that shared non-durable subscriptions work on the dev image. Check that `clientReconnectTimeout` bounds a send during an outage below `OUTAGE_PUBLISH_BOUND`. If neither works portably, **record a finding rather than weaken a test**.
- **IBM MQ timing:** the suite's `TIMEOUT = 5 s` may be tight for IBM MQ. Change it only if there's evidence.
- **Header names:** the §G charset allows `.` and `-`, which aren't legal Jakarta property identifiers. The suite only uses plain names, so a provider may reject such keys. Deferred by decision; list it as a known gap in the README.
- **Toxiproxy on Docker Desktop for Windows (§K untested):** `ArtemisConformanceTest` and `IbmMqConformanceTest` connect directly to the broker's mapped ports rather than through a Toxiproxy proxy, so the §K network-outage/reconnection scenarios (`AbstractMessagingConnectivityTest`'s `cutNetwork`/`restoreNetwork`) are **not exercised against a real broker**. Isolated diagnostics on the machine this plan was executed on showed: a direct JMS connection to a containerized Artemis broker succeeds reliably, but the identical connection routed through a Testcontainers Toxiproxy proxy on a custom bridge network reproducibly fails during the Artemis client's topology handshake (`AMQ219006: Channel disconnected`, looping until connect-timeout). Separately, running the full 30-scenario conformance suite (each test opening a fresh connection) against even the direct, no-proxy connection also started failing with connection resets once the container had absorbed enough rapid connect/disconnect churn (`ActiveMQException: An established connection was aborted by the software in your host machine`). Both symptoms point at Docker Desktop for Windows' port-forwarding layer (npipe/vpnkit) rather than the adapter or the brokers — `JmsTransport`/`ConnectionFactoryBuilder` are independently unit-tested, and a bare direct connection outside the suite succeeded. **Not fixed by adapter changes.** Recommendation: re-run the Toxiproxy-based §K scenarios on a native Linux Docker host (including WSL2 with Docker installed directly inside it, not Docker Desktop's Windows integration) before relying on §K reconnection behavior in production. `JmsTransport`'s `ExceptionListener` → `DISCONNECTED` wiring exists in `src/main` per §K but currently has no dedicated test coverage (unit or integration) — a follow-up task should add one.

## Verification

- **Prerequisite:** Docker Desktop is running.
- **`gradlew.bat test`:** core, `ConnectionFactoryBuilder`, the meta-test, the in-memory control run including the new scenarios, and the sample-app tests.
- **`gradlew.bat :messaging-jms:integrationTest`:** every scenario passes on Artemis and IBM MQ, with zero skips (the build enforces this).
- **Break checks** (each reverted afterwards):
  - Remove `rollback()` → the redelivery scenarios fail.
  - Skip cancelling the handler future on stop → the stuck-handler scenarios fail.
  - Make `SafeListener.wrap` return the raw listener → `listenerThrowDoesNotChangeDelivery` fails.
  - Use `createConsumer` per unit for topics → `topicConcurrencyDeliversOncePerSubscription` fails.
- **Sample app:** start both brokers with the README's `docker run` commands. `run --args="--transport jms-artemis --url … --admin-url …"` and the same for `jms-ibm-mq` must exit 0. With the broker stopped, it must exit 1.
