# Messaging_Lib — semantics contract

Status: **approved**. Derived from `PLAN.md` and the findings in `PLAN_REVIEW.md`; this document does not require a separate review gate.

This is the binding behavioural contract for the library. `PLAN.md` is the build plan and links here rather than restating any of it. Where the two disagree, this document wins.

Every rule below is observable: if it cannot be asserted by the conformance suite or by a compile-time signature, it does not belong here.

## Scope of the guarantee

The library promises **at-least-once delivery of byte payloads with string headers, over a transport chosen by configuration**. It does not promise exactly-once, cross-partition ordering, request/reply, or transactional coupling with an application database.

Two patterns exist, and the destination type — not a flag — selects between them:

| Destination | Semantics | Lifetime |
|---|---|---|
| `Topic` | Fan-out. Every live subscriber receives every message. | Ephemeral; see §D |
| `Queue` | Competing consumers. Exactly one subscriber receives each delivery attempt. | Durable; see §M |

---

## A. Delivery and settlement

Delivery is **at-least-once**. Duplicates are possible at every failure boundary and callers must tolerate them. A handler that has already succeeded may still see the same message again if the process dies before settlement reaches the broker.

The handler returns a `CompletableFuture<Void>` whose sole job is to report an outcome:

| Handler outcome | Library action |
|---|---|
| Future completes normally | Settle — Kafka offset commit, JMS transaction commit |
| Future completes exceptionally | Do not settle → broker redelivers |
| Handler throws synchronously | Treated as exceptional |
| Handler returns `null` | Treated as exceptional |
| Future is cancelled | Treated as exceptional |
| Future never completes | Occupies its consumer unit until bus close, then cancelled → redelivered |

**There is no retry policy and no dead-letter queue.** A message whose handler always fails redelivers indefinitely, consuming its consumer unit. This is deliberate: retry policy is application- and broker-specific, and a wrong default is worse than none. Operators bound it at the broker — IBM MQ `BOTHRESH` plus a backout queue, Artemis `max-delivery-attempts`, or a circuit breaker inside the handler.

The same rule governs codec failures (§L) — a payload that cannot be decoded is a failed handler, and therefore a poison message.

## B. The owner-loop rule

**A handler future never touches a broker client.** This is the most important internal invariant, and it exists because both target client APIs forbid what the naive implementation would do:

- A `KafkaConsumer` is not thread-safe. Only `wakeup()` is safe from another thread.
- A Jakarta Messaging `Session` is a single-threaded context. Using it, or any object it created, from another thread while a consumer owns it is erroneous — `close()` excepted.

An application handler completes on whatever thread it likes. Attaching `commit`, `rollback`, `seek`, `acknowledge`, `recover`, `pause`, or `resume` to that completion would call broker clients from illegal threads and produce intermittent corruption or deadlock.

Therefore: **completion enqueues an outcome record to the adapter's owner loop, and the owner loop performs every broker-client call.** One owner loop per consumer unit.

| Adapter | Owner loop | Settlement performed there |
|---|---|---|
| Kafka | The `poll()` thread | `commitSync` on success; `seek()` back to the failed offset on failure; `pause()`/`resume()` to keep polling alive while a handler is in flight |
| JMS | A synchronous `receive()` loop | `commit()` on success; `rollback()` on failure |

The JMS adapter deliberately uses a synchronous receive loop rather than a `MessageListener`. A listener runs on the provider's own dispatch thread, which would entangle transacted settlement with provider-controlled threading and make the completion hand-off harder to reason about — on IBM MQ in particular.

Cross-thread calls permitted anywhere in the library: `KafkaConsumer.wakeup()`, and `close()` on JMS `Connection`, `Session`, and `MessageConsumer`. Nothing else.

## C. Concurrency and ordering

`messaging.consumer.concurrency` (default `1`) is the number of **independent consumer units** for a subscription. Each unit has **exactly one message in flight**.

| Adapter | A consumer unit is | Cap |
|---|---|---|
| Kafka | One `KafkaConsumer` in the group, with its own poll thread | Partition count — extra units idle |
| JMS | One transacted `Session` with one `MessageConsumer` | None |

**JMS topics (added by the JMS adapter spec):** a topic subscription is one shared
non-durable subscription (`createSharedConsumer`); its `concurrency` consumer units
compete inside that one subscription, so the subscription sees each message once
regardless of concurrency, while separate `subscribe` calls on the same topic still each
get their own copy.

**Ordering:** guaranteed per Kafka partition, and per JMS consumer unit. Never guaranteed across partitions, across consumer units, or across a redelivery. At `concurrency = 1` against a single-partition topic, total order holds.

This constraint is what makes at-least-once safe on Kafka. A Kafka committed offset identifies the *next* record to consume, so committing offset N+1 while record N is still in flight or has permanently failed silently skips N on restart — turning the advertised at-least-once into at-most-once. Allowing only one in-flight record per partition removes the possibility structurally. **The contiguous-success watermark algorithm is explicitly not built**; it is the correct fix for parallel in-partition processing, and it is unnecessary because parallel in-partition processing is not offered.

**Rebalance:** on partition revocation, the in-flight handler for a revoked partition is cancelled and not settled. Its record is redelivered to whichever consumer receives the partition. That is a normal duplicate, not an error.

## D. Subscription readiness and lifetime

**Readiness.** `subscribe(...)` returns a future that completes only when the broker-side consumer is established and will receive messages published from that moment on — Kafka after partition assignment completes, JMS after the consumer is created. It completes exceptionally if the destination is absent or access is denied (§F). Applications and tests may publish immediately after the future completes without losing topic messages.

**Lifetime.** A subscription lives from readiness until `Subscription.close()` or bus close. Its identity — Kafka consumer group, JMS subscriber — is stable for that whole span.

**A transient disconnect is not an unsubscribe.** Adapter-managed reconnection (§K) restores the same identity. `Subscription.close()` is the only way to end a subscription's lifetime.

**Portability exception — topic messages published during a disconnect.** Whether these arrive after recovery is transport-specific and is *not* guaranteed:

| Adapter | Behaviour |
|---|---|
| Kafka | Retained. The group's committed position survives the outage, so the backlog is delivered on reconnect. |
| JMS | Dropped. A non-durable topic subscriber receives nothing while absent. |

This divergence is irreducible without durable topic subscriptions, which are out of scope. It is documented, it appears in the README's per-transport table, and **the conformance suite asserts neither behaviour** — asserting either would make one correct adapter fail.

`Queue` has no such exception. Messages published to a queue during an outage **must** be delivered after recovery, on every transport, and the suite tests exactly that.

## E. Publish completion

A publish future completes when **the broker has acknowledged receipt and the message is durable to the extent the destination allows**. Not when it is locally queued, and not when a consumer has processed it.

| Adapter | Enforced setting |
|---|---|
| Kafka | `acks=all`; the future completes from the producer callback |
| JMS | `DeliveryMode.PERSISTENT`; the send completes before the future does |

Passthrough configuration that would weaken this — `messaging.kafka.acks` set to `0` or `1`, or a JMS delivery mode of `NON_PERSISTENT` — is **rejected at connect time with an error naming the key and the reason**. Silently honouring it would let a config file quietly downgrade the library's central promise.

Publishing while disconnected fails once the underlying client's own timeout elapses. The library performs no buffering, queueing, or retry of publishes; a failed publish is the caller's to handle.

## F. Provisioning and destination naming

**Destinations must pre-exist. The library never creates one.**

- Kafka consumers set `allow.auto.create.topics=false`; the adapter never calls `AdminClient`.
- IBM MQ cannot auto-create queues at all — the clearest evidence that application-managed provisioning is the only portable choice.

Publishing or subscribing to a missing destination fails the future with a `MessagingException` naming the destination and carrying the transport's own error as cause. It does not hang, and it does not silently succeed.

Conformance fixtures provision destinations **outside the library API** — broker admin tooling, container init scripts, or the image's own defaults — and then supply the destination *names* to the suite. The suite never hard-codes a name, because the IBM MQ developer image ships fixed `DEV.QUEUE.1..3` and topic string `dev/` that cannot be renamed.

**Cross-type name reuse is unsupported.** `Topic("orders")` and `Queue("orders")` resolve to the same Kafka topic but to entirely different constructs on JMS. The library neither rejects nor namespaces this; the behaviour is undefined and documented as such.

**What "missing" means per JMS provider (JMS adapter spec):**

| Provider | Missing queue | Missing/unauthorised topic |
|---|---|---|
| Artemis (auto-create disabled) | Rejected | Rejected |
| IBM MQ | Rejected — reason code 2085, or 2035 outside `DEV.**` | Rejected — reason code 2035 if the topic string is outside the app's authorised tree |

Either way, the publish or subscribe future fails with a `MessagingException` naming the
destination, with the provider's own exception as cause.

## G. The message model

```java
record Message(byte[] body, Map<String, String> headers) { }
```

**There is no `id` field.** JMS assigns message identity; Kafka has no equivalent, so a core `id` could only be fabricated for one transport or left null for the other. Both are dishonest. Transport identity, where it exists, travels as a reserved header.

**Immutability.** `body` is defensively copied on construction and again on delivery to a handler. `headers` is copied into an unmodifiable map. Caller mutation after publish, and handler mutation before a redelivery, are both impossible rather than merely discouraged.

**Header rules.**

| Rule | Value |
|---|---|
| Key charset | `[A-Za-z0-9_.-]+` — the intersection that is safe as a JMS property name and a Kafka header key |
| Reserved key prefixes | `JMS`, `JMSX` (provider-reserved), and `messaging.` (library-reserved) |
| Value encoding | UTF-8 on the wire; empty values are legal, `null` values are rejected |
| Duplicate Kafka header keys inbound | Last wins |
| Total header block size | Rejected above 64 KiB |

Violations are rejected at publish time with an error naming the offending key — not at the broker, and never silently truncated.

**Reserved headers set by adapters:**

| Header | Set by | Meaning |
|---|---|---|
| `messaging.redelivered` | JMS (`JMSRedelivered`), IBM MQ (backout count > 0) | This delivery is a retry |

Kafka **omits** `messaging.redelivered` entirely, because Kafka records carry no redelivery flag and inferring one from offsets would be a guess. An absent header means "unknown", not "false". This divergence is documented rather than papered over.

## H. Listener contract

```java
interface MessagingListener {
    void onPublished(Destination destination);
    void onConsumed(Destination destination);
    void onError(Destination destination, Throwable error);
    void onConnectionStateChanged(ConnectionState state);   // CONNECTED, DISCONNECTED, RECONNECTED
}
```

`onConnectionStateChanged` replaces the original `onReconnected`, which had no reliable meaning once adapters own the lifecycle (§K).

- `onConsumed` fires **after settlement**, so it means "processed and settled", not "received".
- Callbacks run on the adapter's owner thread and **must not block**. A slow listener slows delivery.
- **A listener that throws is caught, logged at WARN, and cannot change a publish or ack outcome.** Observability never alters behaviour. The conformance suite asserts this with a listener that throws on every callback.
- A no-op default instance is supplied; a `null` listener is never passed to an adapter.

**JMS reconnection (JMS adapter spec):** the Jakarta Messaging API exposes no reconnect
event, so the JMS adapter never reports `RECONNECTED`. `DISCONNECTED` fires only when the
client's own reconnect logic (Artemis `reconnectAttempts`, IBM MQ `WMQ_CLIENT_RECONNECT`)
gives up and calls the registered `ExceptionListener`.

## I. Provider discovery and configuration

**Discovery.** `Messaging.connect(config)` resolves the `TransportProvider` whose `scheme()` matches the config URI, via `ServiceLoader` over the thread-context class loader, falling back to `Messaging`'s own loader.

| Situation | Behaviour |
|---|---|
| No provider for the scheme | `MessagingException` naming the scheme, the schemes that *are* available, and the likely missing artifact |
| Two providers claiming one scheme | Deterministic failure naming both implementation classes. Never an arbitrary pick |
| `ServiceConfigurationError` while iterating | Wrapped in `MessagingException` with the malformed provider named |

Module-path and custom-class-loader support beyond the TCCL fallback is out of scope and documented as such.

**Configuration precedence**, highest first: explicit `MessagingConfig` builder values → `messaging.*` properties → adapter defaults.

| Key | Default | Notes |
|---|---|---|
| `messaging.url` | — | Required. Its scheme selects the adapter |
| `messaging.client-id` | generated | Used in Kafka group names and JMS client IDs |
| `messaging.consumer.concurrency` | `1` | See §C |
| `messaging.connect-timeout` | `PT10S` | |
| `messaging.close-timeout` | `PT30S` | See §J |
| `messaging.jms.connection-factory` | — (required for the `jms` scheme) | Fully-qualified class name of a `jakarta.jms.ConnectionFactory` implementation, instantiated via its public no-arg constructor |

Every other `messaging.jms.*` key is applied to that factory instance as a public
single-argument bean setter (`messaging.jms.brokerURL` → `setBrokerURL`, coercing the
string value to the setter's parameter type: `String`, `int`/`Integer`, `long`/`Long`, or
`boolean`/`Boolean`). An unrecognised setter, or a value that can't be coerced, fails fast
naming the key. `messaging.jms.deliveryMode` set to `NON_PERSISTENT` or `1` is rejected
before setter lookup (§E). The `messaging.url`'s host and port are **not used** by the JMS
adapter — only its userinfo (for `createConnection(user, password)`) — because each
provider's own broker-address format differs (Artemis `brokerURL`, IBM MQ
`hostName`/`port`/`channel`) and lives in the passthrough properties instead.

- Unknown `messaging.*` keys **fail fast** — a typo in a core key is a bug, not a passthrough.
- Unknown `messaging.<scheme>.*` keys pass through to the native client untouched, minus the prefix, subject to the §E rejections.
- Durations are ISO-8601 (`Duration.parse`, e.g. `PT10S`). One format, no custom parser to write or test.
- `MessagingConfig.toString()` redacts any key matching `*password*` or `*secret*`, and the URI's userinfo. Credentials never reach a log.

## J. Shutdown

`close()` is **idempotent and synchronous**, on both `MessageBus` and `Subscription`.

1. Stop delivery — no new message is handed to a handler.
2. Wait up to `messaging.close-timeout` for in-flight handlers and pending publishes.
3. Cancel remaining handlers and fail remaining publish futures.
4. Close broker resources.

**A cancelled handler is not settled**, so its message is redelivered on the next start (§A). Closing a bus during reconnect backoff is legal and returns promptly; the adapter abandons recovery. Any API call after close throws `IllegalStateException`.

`Subscription.close()` during an in-flight handler follows the same rule: the handler is given until the timeout, then cancelled unsettled.

## K. The SPI — provisional

```java
public interface TransportProvider {
    String scheme();
    Transport open(MessagingConfig config, MessagingListener listener);
}

public interface Transport extends AutoCloseable {
    CompletableFuture<Void> publish(Destination destination, Message message);
    CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler);
    void close(Duration timeout);
}
```

Concurrency is read from config by the adapter, not passed through the SPI. Per-subscription concurrency is not offered (YAGNI); adding it later is a compatible change.

**Adapters own the connection lifecycle.** There is no `Reconnector` in core, and core performs no recovery of any kind. All three Phase 1 targets already reconnect natively — Kafka transparently handles broker failure and group rebalancing, Artemis via `reconnectAttempts` on the connection URL, IBM MQ via `WMQ_CLIENT_RECONNECT`. A second, generic reconnect loop layered on top would fight the client's own recovery, duplicate subscriptions, and change subscription identity underneath the application. Adapters instead normalise what their client reports into `onConnectionStateChanged` (§H).

Consequence, stated rather than hidden: **in-flight transacted work at the moment of disconnect is lost and redelivered.** Under at-least-once that is correct behaviour, not a defect.

**The SPI is provisional through Phase 2, not frozen.** Two adapters — even two as dissimilar as Kafka and JMS — is thin evidence for a contract that RabbitMQ, STOMP, and Solace must also satisfy; those transports differ in settlement, topology, and reconnect models. A contract-mapping spike for each precedes any freeze (`PLAN.md`, step 9).

## L. Typed channels and codecs

```java
interface Codec<T> {
    byte[] encode(T value);
    T decode(byte[] bytes);
}
```

`bus.typed(destination, codec)` returns a `TypedChannel<T>` that encodes on publish and decodes on delivery. The typed handler receives the decoded value **and** the header map — headers are not hidden by the codec layer, because routing and correlation metadata is usually needed alongside the payload.

**A decode failure is a handler failure** (§A): the message is not settled and is redelivered. Since there is no retry or DLQ, an undecodable payload is a poison message that redelivers indefinitely and occupies its consumer unit. This is a direct, intended consequence of the no-DLQ decision, and the README states it so nobody discovers it in production.

Codecs must be thread-safe and must not carry state across messages. `messaging-codec-json` (Jackson) is the only codec shipped in Phase 1; core has no codec dependency.

## M. Durability and retention — what "durable" actually means

The original plan called queues "durable" without qualification. That is too absolute to be true on either transport, so the concrete assumptions are:

| Adapter | What backs a `Queue` | Retention limit |
|---|---|---|
| Kafka | A topic, with the consumer group named after the queue; durability *is* the committed offset | Bounded by broker `retention.ms` / `retention.bytes`. **A consumer offline longer than retention loses messages.** Offsets themselves expire per `offsets.retention.minutes` |
| JMS (Artemis, IBM MQ) | A real broker queue holding `PERSISTENT` messages | Bounded by queue depth, message expiry, and disk. Messages survive broker restart |

`Topic` is ephemeral on both, per §D.

The library does not configure retention, queue depth, or expiry — those are provisioning concerns (§F) belonging to whoever creates the destination. The README states these limits per transport instead of repeating the word "durable".

---

## Appendix: coverage of `PLAN_REVIEW.md`

Every finding in the review is resolved here, or deferred with a reason in `PLAN.md`.

| Review finding | Resolution |
|---|---|
| §1 Reconnection has no owner | §K — adapters own it; `Reconnector` deleted; `onConnectionStateChanged` |
| §2 Future completion vs. thread confinement | §B — owner-loop rule |
| §3 Kafka concurrency breaks at-least-once | §C — one in-flight record per partition |
| §4 `INDIVIDUAL_ACKNOWLEDGE` is not portable | Transacted sessions, `jakarta.jms-api` only; proven on Artemis **and IBM MQ** |
| §5 Ephemeral topic inconsistency | §D — lifetime defined; topic-during-outage is an explicit, untested portability exception; queue-during-outage is guaranteed and tested |
| High: subscription readiness | §D |
| High: publish completion | §E |
| High: resource provisioning | §F |
| High: SPI frozen on thin evidence | §K — provisional through Phase 2 |
| High: shutdown | §J |
| Medium: message identity | §G — `id` removed |
| Medium: mutability | §G — defensive copies, unmodifiable headers |
| Medium: header mapping | §G — charset, reserved prefixes, UTF-8, duplicates, size cap |
| Medium: listener contract | §H |
| Medium: provider discovery | §I — duplicates, `ServiceConfigurationError`, class-loader boundary |
| Medium: configuration | §I — precedence, durations, unknown keys, redaction |
| Medium: typed channels | §L — decode failure is handler failure; poison consequence stated |
| Medium: queue durability | §M — concrete retention assumptions replace the blanket claim |
| Low: destination namespace | §F — cross-type reuse unsupported and undefined |
| Build and conformance findings | `PLAN.md` — wrapper, test fixtures, `integrationTest` source set, mechanical skip-is-failure, negative control as a real test, restart-vs-disconnect separation |
