# Review of `PLAN.md`

## Overall assessment

`PLAN.md` has a strong product boundary and a sensible Phase 1: it limits the common API, chooses two meaningfully different brokers, keeps the core framework-free, and makes adapter conformance a first-class requirement. The negative-control test—deliberately breaking acknowledgement behavior and requiring the suite to fail—is especially valuable.

The plan is not implementation-ready yet. Its main risks are not missing classes or build details; they are unresolved ownership and delivery-semantics contracts. In particular, reconnection, asynchronous handler completion, concurrency, and acknowledgement all cross the proposed core/SPI/adapter boundary. Implementing the current text literally could lose messages, invoke broker clients from invalid threads, or make the JMS adapter specific to Artemis while presenting it as generic JMS.

The best next step is to resolve the critical decisions below in `PLAN.md` before creating the Gradle skeleton. This review does not propose a replacement API; it identifies the contracts the plan must settle.

## What is already strong

- **Scope discipline:** request/reply, durable topic subscriptions, retry/DLQ policy, metrics, and framework integration are explicitly deferred.
- **Useful transport contrast:** Kafka and Artemis expose different enough delivery models to challenge a common abstraction early.
- **Small core model:** bytes plus string headers is a defensible lowest-common-denominator payload.
- **Dependency direction:** `ServiceLoader` keeps adapter implementations out of the core's compile-time dependency graph.
- **Executable portability goal:** running one unchanged example against both adapters is a stronger acceptance criterion than compilation alone.
- **Conformance philosophy:** shared behavioral tests and a required negative control align with `CLAUDE.md`'s E2E-test rule.

## Critical issues to resolve before implementation

### 1. Reconnection has no viable owner or signal in the proposed SPI

**Finding:** `Reconnector` is assigned to core, but `Transport` exposes only `publish`, `subscribe`, and `close`. There is no connection-state signal, terminal failure callback, or factory contract through which core can distinguish a transient broker outage from a failed operation and safely replace the transport. An idle consumer can lose connectivity without any core API call occurring. Kafka's client also already handles broker failures and group rebalancing internally, so an independent generic reconnect loop can conflict with adapter-managed recovery. The Kafka client documentation explicitly describes transparent broker-failure handling and consumer-group adaptation ([KafkaConsumer](https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)).

**Impact:** subscriptions can silently stop, be recreated twice, or change identity. Pending publishes and handler completions have no defined fate. Listener callbacks such as `onReconnected` cannot have a reliable meaning.

**Required plan correction:** choose one lifecycle owner. Either adapters own physical reconnect/resubscription and report normalized state changes to core, or core owns transport replacement through an expanded lifecycle SPI with explicit failure signals and replay rules. Define how pending operations, active subscriptions, backoff, shutdown, and duplicate reconnect attempts behave. Do not implement both generic and client-native reconnection independently.

### 2. Future completion conflicts with broker-client thread confinement

**Finding:** a handler can complete on any application thread, while both target client APIs restrict broker operations. A Kafka consumer is not thread-safe; only `wakeup()` is safe from another thread ([KafkaConsumer, “Multi-threaded Processing”](https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)). A Jakarta Messaging `Session` is a single-threaded context, and using it or its children from another thread while a listener owns it is erroneous, except for close operations ([Jakarta Messaging `Session`](https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/session)). The plan currently says only that handler execution uses a core executor.

**Impact:** attaching `acknowledge`, `recover`, `seek`, `commit`, `pause`, or `resume` directly to a `CompletableFuture` completion can violate client contracts and introduce intermittent corruption or deadlocks.

**Required plan correction:** state that future completion reports an outcome but never directly manipulates a broker client. Each adapter must serialize completion outcomes back onto its consumer/session owner loop before acknowledging, recovering, seeking, committing, or resuming. Define how synchronous handler throws, a `null` future, cancellation, and never-completing futures are handled.

### 3. Kafka concurrency can violate at-least-once delivery

**Finding:** with concurrency greater than one, records from a partition may finish out of order. Committing offset N+1 because record N+1 succeeded while N is still running or has failed permanently skips N after restart. Seeking to N after later completions does not undo an already committed higher offset. Kafka requires a committed offset to identify the *next* record to consume ([KafkaConsumer commit contract](https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)). `pause()` and `resume()` provide flow control but do not by themselves make out-of-order commits safe.

**Impact:** the advertised at-least-once guarantee can become at-most-once for failed records whenever concurrency exceeds one.

**Required plan correction:** specify a per-partition in-flight model and a contiguous-success watermark. Only advance a committed offset through the highest consecutive completed record; on failure, prevent any later success from advancing past the gap. Also define behavior during rebalance and partition revocation. If Phase 1 does not need this complexity, constrain each partition to one in-flight handler and state that `consumer.concurrency` scales consumers/partitions rather than parallel records within one partition.

### 4. `INDIVIDUAL_ACKNOWLEDGE` contradicts a portable JMS adapter

**Finding:** Jakarta Messaging standardizes `AUTO_ACKNOWLEDGE`, `CLIENT_ACKNOWLEDGE`, `DUPS_OK_ACKNOWLEDGE`, and transacted sessions. In `CLIENT_ACKNOWLEDGE`, acknowledging one message acknowledges all messages delivered by that session, and `recover()` restarts from the first unacknowledged message ([Jakarta Messaging acknowledgment rules](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html)). `INDIVIDUAL_ACKNOWLEDGE` is an Artemis extension exposed through `ActiveMQJMSConstants`, not the Jakarta API ([Artemis extra acknowledgement modes](https://activemq.apache.org/components/artemis/documentation/2.31.2/book.pdf)). ActiveMQ Classic has a similarly named provider extension, but through a different provider type ([ActiveMQ Classic `ActiveMQSession`](https://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQSession.html)).

**Impact:** an implementation compiled only against `jakarta.jms-api` cannot use the proposed mode portably. Depending on Artemis classes turns `messaging-jms` into an Artemis adapter despite its broader name and stated target.

**Required plan correction:** choose and document one honest scope: Artemis-only for Phase 1; provider-specific capability modules; or a standard-JMS design using separate sessions/transactions whose behavior remains correct with asynchronous handlers. Add at least one non-Artemis compatibility test before claiming ActiveMQ/Artemis portability.

### 5. “Ephemeral topic” behavior is inconsistent across reconnects

**Finding:** Kafka topic subscriptions use a generated consumer group and `auto.offset.reset=latest`, while JMS uses a non-durable topic subscriber. `auto.offset.reset` applies only when a group has no valid committed position; it does not force every reconnect to the end. Reusing the generated Kafka group on reconnect retains backlog, while generating a new group skips messages published during the outage. A non-durable JMS subscriber normally does not retain messages while absent.

**Impact:** the same API can either replay or drop outage-time topic messages depending on adapter and reconnect implementation. The conformance suite cannot determine correct behavior from the current text.

**Required plan correction:** define subscription lifetime precisely: when identity is created and destroyed, whether outage-time messages belong to that lifetime, whether an unacknowledged in-flight message must be redelivered, and whether explicit `Subscription.close()` differs from transient disconnect. Then make both adapters and reconnect tests enforce the same observable contract or document the difference as an explicit portability exception.

## Major design and maintainability gaps

| Severity | Area | Gap and why it matters | Plan-level action |
|---|---|---|---|
| High | Subscription readiness | It is unclear whether a completed `subscribe` future means the broker consumer is active. Tests and applications can publish too early and lose topic messages. | Define successful completion as ready-for-delivery, plus failure and cancellation behavior. |
| High | Publish completion | “Publish succeeded” could mean locally queued, acknowledged by a broker, or durably stored. Kafka `acks` and JMS delivery mode can weaken the promise through passthrough configuration. | State the minimum completion guarantee and reject or document native settings that undercut it. |
| High | Resource provisioning | The plan does not say whether topics, queues, and Artemis address/routing types must pre-exist or are created by adapters. Kafka auto-creation and JMS provider defaults are not portable. | Choose application-managed or adapter-managed provisioning and define type/name collisions. |
| High | Future adapters | Declaring the SPI frozen after only Kafka and Artemis is stronger than the evidence supports. RabbitMQ, STOMP, and Solace may expose different settlement, topology, and reconnect constraints. | Perform contract-mapping spikes for the deferred transports before freezing the SPI, or label it provisional through Phase 2. |
| High | Shutdown | `AutoCloseable.close()` is synchronous, but behavior for in-flight handlers, pending publishes, reconnect backoff, and repeated close is unspecified. | Define idempotence, drain-versus-cancel policy, timeout behavior, and post-close failures. |
| Medium | Message identity | The producer of `Message.id`, behavior for caller-provided IDs, and ID stability across redelivery are unspecified. Kafka has no direct equivalent of JMS-assigned message identity. | Define ownership, nullability, uniqueness scope, and redelivery stability, or remove the field from Phase 1. |
| Medium | Mutability | Records do not make `byte[]` or maps immutable. Caller mutation after publish or handler mutation can create data races and surprising retries. | Require defensive copies and an unmodifiable header map, and document size/null limits. |
| Medium | Header mapping | Kafka headers are byte-valued and JMS properties have naming/type restrictions and reserved names. A string-only core type does not eliminate mapping conflicts. | Define UTF-8 encoding, duplicate-key policy, reserved-name handling, validation, and round-trip limitations. |
| Medium | Listener contract | Callback parameters, ordering, execution thread, exception isolation, and the meaning of “consumed” are absent. | Specify signatures and guarantee that listener failures cannot change publish/ack outcomes. |
| Medium | Provider discovery | Missing-provider behavior is covered, but duplicate schemes and `ServiceConfigurationError` are not. `ServiceLoader` is lazy, can fail while iterating, and has class-loader considerations ([Java 21 `ServiceLoader`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/ServiceLoader.html)). | Define deterministic duplicate rejection, loading errors, and the class-loader/module-path support boundary. |
| Medium | Configuration | Durations, valid URI shapes, namespace stripping, precedence, secrets, unknown keys, and reconnect defaults are not specified. | Add a validation/precedence table and require actionable errors without logging credentials. |
| Medium | Typed channels | Decode failure and header access are not described. A poison payload may redeliver forever because retry/DLQ is out of scope. | State that decode failures follow the handler-failure contract and document the intentional poison-message consequence. |
| Medium | Queue durability | Kafka offsets expire under broker retention, while JMS queue durability depends on broker topology and message delivery mode. “Durable” is too absolute. | Replace the blanket claim with concrete retention and provisioning assumptions per adapter. |
| Low | Destination namespace | A `Topic("x")` and `Queue("x")` can refer to one Kafka topic but different routing constructs in JMS. | Define whether cross-type name reuse is rejected, namespaced, or explicitly unsupported. |

## Build and module review

1. **Add and pin the Gradle Wrapper.** The layout and build steps mention Gradle files but not `gradlew`, `gradlew.bat`, or wrapper metadata. Java 21 requires an appropriate Gradle version; Java 21 toolchain support starts at Gradle 8.4 and running Gradle on Java 21 starts at 8.5 ([Gradle compatibility matrix](https://docs.gradle.org/current/userguide/compatibility.html)). Pin plugin and dependency versions for reproducibility.
2. **Define how conformance tests are shared.** An abstract JUnit class intended only for adapter tests should be published as test fixtures rather than leaking JUnit through a production artifact. Gradle's `java-test-fixtures` plugin supports cross-project consumption through `testFixtures(project(...))` ([Gradle test fixtures](https://docs.gradle.org/current/userguide/java_testing.html)).
3. **Separate fast and broker-backed verification.** `build` normally depends on `test`, so the current `build` and `test` descriptions are redundant unless integration tests use a distinct source set/task. Define unit and conformance tasks, their Docker requirement, timeouts, and CI behavior.
4. **Make skipped tests fail mechanically.** The prose says a skipped scenario is failure, but JUnit/Gradle does not enforce that policy merely because it is documented. Add build-level enforcement and verify it with a deliberately skipped fixture.
5. **Clarify the design-document step.** The repository already has this detailed plan but no separate approved design document. State whether the planned design document is derived from `PLAN.md` or must pass another review gate before code.
6. **Treat `/simplify` as workflow, not build tooling.** It is an agent command, not a portable Gradle or shell command. Keep it in contributor workflow instructions and pair it with repeatable compiler/test checks.

## Conformance-suite gaps

The listed scenarios are a good start but are not sufficient to prove the stated guarantees. Add or clarify the following cases:

- Body and empty-body round trips, not only headers.
- Both topic subscribers confirmed ready before publish.
- Queue competition over many messages, with no simultaneous delivery of one attempt to two consumers.
- Duplicate-tolerant assertions: at-least-once does not promise that a successfully handled message can never be redelivered across every failure window.
- Stable message identity and redelivery metadata, once the ID/header contract is settled.
- Synchronous handler throw, exceptional future, cancelled future, `null` future, and a future that remains incomplete during close.
- Listener callbacks throwing exceptions without changing delivery outcomes.
- Kafka multi-partition, out-of-order completion, failure gaps, and rebalance during in-flight work.
- JMS asynchronous completion with multiple in-flight messages and session-safe acknowledgement/recovery.
- Publish and subscription failure during broker outage, followed by recovery.
- Explicit subscription close during handler execution and bus close during reconnect backoff.
- Header Unicode, empty values, reserved names, invalid names, and configured size limits.
- Missing and duplicate `ServiceLoader` providers, malformed provider declarations, and invalid URI/configuration.
- Pre-existing versus absent destinations according to the chosen provisioning contract.
- Topic outage semantics that distinguish transient disconnect from explicit unsubscribe.

The broker-restart test should distinguish client-native recovery from library-managed recovery. A network cut or proxy-based fault can test transient connectivity without conflating it with broker data loss or container recreation. Testcontainers permits explicit lifecycle control ([manual lifecycle documentation](https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/)), but the test must state whether restarting the container preserves broker identity and storage; otherwise it may be testing a fresh broker rather than reconnection.

The manual “break an adapter” check should become a repeatable test of the conformance harness itself. For example, run selected conformance behaviors against intentionally faulty test transports that acknowledge too early or fan a queue message out to multiple consumers. A manual source edit is useful during development but is not a durable acceptance gate.

## Recommended decision order

1. Settle lifecycle ownership and reconnection semantics.
2. Define the subscription lifetime, readiness, shutdown, and publish-completion contracts.
3. Define the adapter owner-loop rule for all broker-client operations.
4. Decide whether Phase 1 is Artemis-specific or genuinely standard JMS.
5. Choose the Kafka concurrency/offset algorithm or constrain Phase 1 concurrency.
6. Resolve message ID, mutability, header mapping, resource provisioning, and configuration validation.
7. Mark the SPI provisional until deferred transports receive contract-mapping reviews.
8. Expand the conformance contract before implementing either adapter.
9. Pin the build, add the wrapper, and make conformance/skip policies executable in CI.

Once these decisions are incorporated, the existing module split and implementation order remain reasonable. JMS-first is still a useful way to validate the happy path, provided the plan no longer assumes that Artemis-specific behavior proves a portable JMS contract.
