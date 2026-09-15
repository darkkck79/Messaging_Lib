# JMS Transport Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a JMS transport adapter that passes the shared conformance suite and connectivity tests against both Artemis and IBM MQ with zero adapter code differences between them, after fixing the defects in `messaging-core` and `messaging-conformance` that currently make parts of the suite vacuous or wrong, and wire the sample app to run the suite against an external broker.

**Architecture:** A new `com.messaging.jms` package in `messaging-jms` implements the existing `TransportProvider`/`Transport` SPI using only `jakarta.jms-api`. Broker identity (Artemis vs. IBM MQ) lives entirely in configuration — a connection-factory class name plus bean-setter passthrough properties — never in adapter code. Each JMS subscription owns a dedicated thread running a synchronous `receive()` loop against a transacted `Session` (the "owner loop", §B), settling with `commit()`/`rollback()` on that same thread. Before the adapter is written, `messaging-core` and `messaging-conformance` get fixed so the suite can actually catch adapter bugs (real listener-throw test, both destination types checked for "missing", real in-flight-close scenarios). Testcontainers + Toxiproxy prove the adapter against real Artemis and IBM MQ containers; the sample app proves it against a broker the user points at.

**Tech Stack:** Java 21, Gradle (Groovy DSL), JUnit 5 + AssertJ + Awaitility, `jakarta.jms-api` 3.1.0, Artemis Jakarta client 2.43.0, IBM MQ Jakarta client 9.4.5.1, Testcontainers 1.21.4 (generic containers + Toxiproxy), Jolokia (via plain `java.net.http`, no client library) for Artemis admin, MQ REST `mqsc` for IBM MQ admin.

**Spec:** `docs/superpowers/specs/2026-09-13-jms-adapter-design.md` (binding for this plan). Also constrained by the base contract `docs/superpowers/specs/2026-08-18-messaging-lib-design.md` (§A–§M, referenced throughout) and `docs/superpowers/specs/2026-09-13-sample-app-design.md` (sample app conventions this plan extends).

## Global Constraints

- Always use MAXIMUM thinking effort; always use Opus for all agents/subagents (per `CLAUDE.md`).
- Keep code minimal, viable, simple but clean — YAGNI, DRY, KISS. Do not add abstractions the spec doesn't call for.
- Run `/simplify` at the end of this plan (Task 41), before any commit of the final state.
- No UI in this work, so chrome-devtools MCP verification is N/A.
- Update all relevant documentation (spec amendments, sample-app README, `PLAN.md`, the superseded 2026-08-21 plan, `architecture.json`/`architecture.html`) as part of Task 40 — do not leave docs stale after a feature lands.
- Do not commit anything until verification (`gradlew.bat test` and `gradlew.bat :messaging-jms:integrationTest`) passes. Verification happens in Task 42, before the final commit.
- Always fix pre-existing issues encountered along the way, even when unrelated to the immediate step (e.g. the `:-1` port bug, the dead `transportProperties` code) — this plan already schedules the ones found during design; if another turns up mid-task, fix it in that task rather than deferring it.
- **The #1 Rule of E2E Tests:** a test must fail when the feature it tests is broken. No exceptions, no "fixing the app inside the test." Every new/changed scenario in this plan must be run once against a deliberately broken implementation (a break-check) to prove it can fail, per the spec's Verification section.
- Every other `messaging.jms.*` key not explicitly named is applied as a bean setter on the connection factory (§I amendment) — never hard-code a provider class name or provider constant anywhere in `messaging-jms/src/main`.
- Commit after every task (each task ends with its own `git add` + `git commit` step).

---

## File Structure

```
messaging-core/src/main/java/com/messaging/
  config/MessagingConfig.java              MODIFY: fix key-order bug, delete transportProperties, fix toString
  internal/SafeListener.java               NEW: extracted listener-isolation wrapper
  internal/DefaultMessageBus.java          MODIFY: use SafeListener, drop duplicate try/catch
  Messaging.java                           MODIFY: use SafeListener.wrap instead of private wrapSafe

messaging-conformance/src/testFixtures/java/com/messaging/conformance/
  AbstractMessagingConformanceTest.java    MODIFY: createBus(BusSettings), new/changed scenarios
  AbstractMessagingConnectivityTest.java   MODIFY: publishOutOfBand hook, fixed/new scenarios
  BusSettings.java                         NEW: record BusSettings(MessagingListener, Duration, int)
  InMemoryConformance.java                 MODIFY: build BusSettings-aware bus
  faulty/InMemoryTransport.java            MODIFY: constructor takes listener/closeTimeout/concurrency, §J close, ConsumerUnit.settle hook
  faulty/AcksBeforeHandlerTransport.java   MODIFY: override only settle()

messaging-jms/src/main/java/com/messaging/jms/
  JmsTransportProvider.java                NEW
  ConnectionFactoryBuilder.java            NEW (package-private)
  JmsTransport.java                        NEW
  JmsConsumerUnit.java                     NEW
messaging-jms/src/main/resources/META-INF/services/
  com.messaging.spi.TransportProvider      NEW: registers JmsTransportProvider

messaging-jms/src/test/java/com/messaging/jms/
  ConnectionFactoryBuilderTest.java        NEW
  StubConnectionFactory.java               NEW (test stub, no broker)

messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/
  BrokerAdmin.java                         NEW
  ArtemisAdmin.java                        NEW
  IbmMqAdmin.java                          NEW
  ArtemisBroker.java                       NEW
  IbmMqBroker.java                         NEW
  JmsRedeliveredScenario.java              NEW
  ExternalArtemisConformance.java          NEW
  ExternalIbmMqConformance.java            NEW
messaging-jms/src/testFixtures/resources/artemis/broker.xml   NEW

messaging-jms/src/integrationTest/java/com/messaging/jms/
  ArtemisConformanceTest.java              NEW
  IbmMqConformanceTest.java                NEW

messaging-sample-app/src/main/java/com/messaging/sample/
  SampleApp.java                           MODIFY: broker targets, --url/--admin-url
messaging-sample-app/src/test/java/com/messaging/sample/
  SampleAppTest.java                       MODIFY: broker-target arg validation tests

Docs:
  docs/superpowers/specs/2026-08-18-messaging-lib-design.md   MODIFY: §C, §F, §H, §I amendments
  docs/superpowers/specs/2026-09-13-sample-app-design.md      MODIFY: adapter slot decided
  messaging-sample-app/README.md                              MODIFY: targets, options, docker run
  PLAN.md                                                      MODIFY: verification lines
  docs/superpowers/plans/2026-08-21-messaging-lib.md          MODIFY: mark Tasks 5-6 superseded
  architecture.json, architecture.html                         MODIFY if present and structurally stale
```

---

## Phase 1 — Core fixes (`messaging-core`)

### Task 1: Fix `messaging.url` resolution order in `fromProperties`

**Files:**
- Modify: `messaging-core/src/main/java/com/messaging/config/MessagingConfig.java:55-79`
- Test: `messaging-core/src/test/java/com/messaging/config/MessagingConfigTest.java`

**Interfaces:**
- Consumes: `MessagingConfig.Builder` (existing, unchanged signatures)
- Produces: `MessagingConfig.fromProperties(Properties)` now resolves `messaging.url` before any scheme-prefixed key, regardless of `Properties` iteration order.

- [ ] **Step 1: Write the failing test**

Add to `MessagingConfigTest.java`:

```java
@Test void fromPropertiesResolvesUrlBeforeSchemeKeysRegardlessOfOrder() {
    // java.util.Properties iterates by hash order; this subclass forces the
    // scheme-prefixed key to be seen before messaging.url to reproduce the bug.
    var props = new Properties() {
        @Override public synchronized java.util.Set<String> stringPropertyNames() {
            var ordered = new java.util.LinkedHashSet<String>();
            ordered.add("messaging.jms.connection-factory");
            ordered.add("messaging.url");
            return ordered;
        }
        @Override public String getProperty(String key) {
            return switch (key) {
                case "messaging.jms.connection-factory" -> "com.example.Factory";
                case "messaging.url" -> "jms://localhost:61616";
                default -> null;
            };
        }
    };
    var config = MessagingConfig.fromProperties(props);
    assertThat(config.scheme()).isEqualTo("jms");
    assertThat(config.passthroughProperties()).containsEntry("connection-factory", "com.example.Factory");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gradlew.bat :messaging-core:test --tests "com.messaging.config.MessagingConfigTest.fromPropertiesResolvesUrlBeforeSchemeKeysRegardlessOfOrder"`
Expected: FAIL — `messaging.jms.connection-factory` is rejected as an unknown key because `scheme` is still null when it's processed.

- [ ] **Step 3: Fix `fromProperties` to resolve the URL first**

Replace lines 55-79 of `MessagingConfig.java`:

```java
public static MessagingConfig fromProperties(Properties props) {
    var b = builder();
    String urlValue = props.getProperty("messaging.url");
    if (urlValue != null) b.url(urlValue);
    String scheme = urlValue != null ? URI.create(urlValue).getScheme() : null;
    for (String key : props.stringPropertyNames()) {
        if (key.equals("messaging.url")) continue;
        String value = props.getProperty(key);
        switch (key) {
            case "messaging.client-id"            -> b.clientId(value);
            case "messaging.consumer.concurrency" -> b.concurrency(Integer.parseInt(value));
            case "messaging.connect-timeout"      -> b.connectTimeout(parseDuration(key, value));
            case "messaging.close-timeout"        -> b.closeTimeout(parseDuration(key, value));
            default -> {
                if (!key.startsWith("messaging.")) continue;
                if (scheme != null && key.startsWith("messaging." + scheme + ".")) {
                    String nativeKey = key.substring(("messaging." + scheme + ".").length());
                    b.property(nativeKey, value);
                } else if (!CORE_KEYS.contains(key)) {
                    throw new MessagingException("Unknown messaging key: " + key
                        + ". Known keys: " + CORE_KEYS);
                }
            }
        }
    }
    return b.build();
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gradlew.bat :messaging-core:test --tests "com.messaging.config.MessagingConfigTest"`
Expected: PASS, all tests in the class including the new one and the pre-existing `fromProperties`, `unknownCoreKeyFailsFast`, `passthroughProperties`, `invalidDurationRejects`.

- [ ] **Step 5: Commit**

```bash
git add messaging-core/src/main/java/com/messaging/config/MessagingConfig.java messaging-core/src/test/java/com/messaging/config/MessagingConfigTest.java
git commit -m "fix(core): resolve messaging.url before scheme-prefixed keys in fromProperties"
```

---

### Task 2: Delete dead `transportProperties`/`transportProperty` code

**Files:**
- Modify: `messaging-core/src/main/java/com/messaging/config/MessagingConfig.java`

**Interfaces:**
- Consumes: nothing new
- Produces: `MessagingConfig` no longer exposes `transportProperties()`; `Builder` no longer exposes `transportProperty(String,String)`.

- [ ] **Step 1: Confirm nothing else references it**

Run: `grep -rn "transportProperty" messaging-core messaging-conformance messaging-jms messaging-kafka messaging-sample-app messaging-codec-json`
Expected: only matches inside `MessagingConfig.java` itself (already confirmed during plan research — no test or other module references it).

- [ ] **Step 2: Delete the field, accessor, and builder method**

In `MessagingConfig.java`, remove:
- field `private final Map<String, String> transportProperties;` (line 26)
- its assignment in the constructor: `this.transportProperties = Collections.unmodifiableMap(new LinkedHashMap<>(b.transport));`
- accessor `public Map<String, String> transportProperties() { return transportProperties; }`
- in `Builder`: field `private final Map<String, String> transport = new LinkedHashMap<>();` and method `public Builder transportProperty(String key, String value) { this.transport.put(key, value); return this; }`

- [ ] **Step 3: Compile and run the full core test module**

Run: `gradlew.bat :messaging-core:test`
Expected: PASS — no test referenced the deleted members, so nothing breaks.

- [ ] **Step 4: Commit**

```bash
git add messaging-core/src/main/java/com/messaging/config/MessagingConfig.java
git commit -m "refactor(core): delete unused transportProperties/transportProperty"
```

---

### Task 3: Fix `toString()` printing `:-1` when the URL has no port

**Files:**
- Modify: `messaging-core/src/main/java/com/messaging/config/MessagingConfig.java:86-97`
- Test: `messaging-core/src/test/java/com/messaging/config/MessagingConfigTest.java`

- [ ] **Step 1: Write the failing test**

```java
@Test void toStringOmitsPortWhenAbsent() {
    var config = MessagingConfig.builder().url("test://user:secret@localhost").build();
    assertThat(config.toString()).doesNotContain(":-1");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gradlew.bat :messaging-core:test --tests "com.messaging.config.MessagingConfigTest.toStringOmitsPortWhenAbsent"`
Expected: FAIL — output contains `localhost:-1`.

- [ ] **Step 3: Fix `toString()`**

Replace the `toString()` body:

```java
@Override public String toString() {
    var sb = new StringBuilder("MessagingConfig{url=");
    if (url.getUserInfo() != null) {
        sb.append(url.getScheme()).append("://***@").append(url.getHost());
        if (url.getPort() != -1) sb.append(":").append(url.getPort());
        sb.append(url.getPath());
    } else { sb.append(url); }
    sb.append(", clientId=").append(clientId).append(", concurrency=").append(concurrency)
      .append(", passthrough={");
    passthroughProperties.forEach((k, v) -> sb.append(k).append("=")
        .append(SENSITIVE_KEY.matcher(k).matches() ? "***" : v).append(", "));
    return sb.append("}}").toString();
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gradlew.bat :messaging-core:test --tests "com.messaging.config.MessagingConfigTest"`
Expected: PASS, including the pre-existing `toStringRedactsPassword` test.

- [ ] **Step 5: Commit**

```bash
git add messaging-core/src/main/java/com/messaging/config/MessagingConfig.java messaging-core/src/test/java/com/messaging/config/MessagingConfigTest.java
git commit -m "fix(core): omit :-1 port suffix in MessagingConfig.toString() when URL has no port"
```

---

### Task 4: Extract `SafeListener.wrap` and use it everywhere listener isolation is needed

**Files:**
- Create: `messaging-core/src/main/java/com/messaging/internal/SafeListener.java`
- Modify: `messaging-core/src/main/java/com/messaging/Messaging.java`
- Modify: `messaging-core/src/main/java/com/messaging/internal/DefaultMessageBus.java`
- Test: `messaging-core/src/test/java/com/messaging/internal/DefaultMessageBusTest.java` (add a wrap-idempotence check)

**Interfaces:**
- Produces: `com.messaging.internal.SafeListener.wrap(MessagingListener raw) -> MessagingListener` — catches and logs (WARN) any exception thrown by any callback; returns `raw` unchanged if it is already a `SafeListener`-wrapped instance.
- Consumes (by `DefaultMessageBus`, `Messaging`): this static method.

- [ ] **Step 1: Write `SafeListener`**

```java
package com.messaging.internal;

import com.messaging.ConnectionState;
import com.messaging.Destination;
import com.messaging.MessagingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Isolates the library from a throwing {@link MessagingListener} (§H): a listener that
 * throws is caught, logged at WARN, and never changes a publish or ack outcome. */
public final class SafeListener implements MessagingListener {
    private static final Logger log = LoggerFactory.getLogger(SafeListener.class);
    private final MessagingListener raw;

    private SafeListener(MessagingListener raw) { this.raw = raw; }

    public static MessagingListener wrap(MessagingListener raw) {
        if (raw instanceof SafeListener) return raw;
        return new SafeListener(raw);
    }

    @Override public void onPublished(Destination d) {
        try { raw.onPublished(d); } catch (Exception e) { log.warn("Listener threw on onPublished", e); }
    }
    @Override public void onConsumed(Destination d) {
        try { raw.onConsumed(d); } catch (Exception e) { log.warn("Listener threw on onConsumed", e); }
    }
    @Override public void onError(Destination d, Throwable err) {
        try { raw.onError(d, err); } catch (Exception e) { log.warn("Listener threw on onError", e); }
    }
    @Override public void onConnectionStateChanged(ConnectionState s) {
        try { raw.onConnectionStateChanged(s); } catch (Exception e) { log.warn("Listener threw on onConnectionStateChanged", e); }
    }
}
```

- [ ] **Step 2: Write a unit test proving idempotence and isolation**

Create `messaging-core/src/test/java/com/messaging/internal/SafeListenerTest.java`:

```java
package com.messaging.internal;

import com.messaging.ConnectionState;
import com.messaging.MessagingListener;
import com.messaging.Topic;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class SafeListenerTest {

    @Test void wrapIsIdempotent() {
        MessagingListener wrapped = SafeListener.wrap(new MessagingListener() {});
        assertThat(SafeListener.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test void throwingListenerDoesNotPropagate() {
        MessagingListener throwing = new MessagingListener() {
            @Override public void onPublished(com.messaging.Destination d) { throw new RuntimeException("boom"); }
            @Override public void onConsumed(com.messaging.Destination d) { throw new RuntimeException("boom"); }
            @Override public void onError(com.messaging.Destination d, Throwable e) { throw new RuntimeException("boom"); }
            @Override public void onConnectionStateChanged(ConnectionState s) { throw new RuntimeException("boom"); }
        };
        MessagingListener safe = SafeListener.wrap(throwing);
        Topic t = Topic.of("x");
        assertThatCode(() -> {
            safe.onPublished(t);
            safe.onConsumed(t);
            safe.onError(t, new RuntimeException());
            safe.onConnectionStateChanged(ConnectionState.CONNECTED);
        }).doesNotThrowAnyException();
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `gradlew.bat :messaging-core:test --tests "com.messaging.internal.SafeListenerTest"`
Expected: FAIL if run before Step 1's `SafeListener.java` exists (compile error: symbol not found). If Step 1 was already applied, this step instead proves nothing new — in that case just proceed to Step 4.

- [ ] **Step 4: Confirm the class from Step 1 compiles and the test passes**

Run: `gradlew.bat :messaging-core:test --tests "com.messaging.internal.SafeListenerTest"`
Expected: PASS.

- [ ] **Step 5: Rewire `Messaging.connect` to use `SafeListener.wrap`**

In `Messaging.java`, replace the private `wrapSafe` method and its call site:

```java
public static MessageBus connect(MessagingConfig config) {
    String scheme = config.scheme();
    MessagingListener listener = com.messaging.internal.SafeListener.wrap(config.listener());
    TransportProvider provider = resolveProvider(scheme);
    Transport transport = provider.open(config, listener);
    return new DefaultMessageBus(transport, config, listener);
}
```

Delete the entire private `wrapSafe(MessagingListener raw)` method (lines 47-62) and its now-unused `ConnectionState`/`Destination` imports if no longer used elsewhere in the file (check remaining usages first).

- [ ] **Step 6: Rewire `DefaultMessageBus` to wrap its own listener and drop duplicate try/catch**

Replace `DefaultMessageBus.java`:

```java
package com.messaging.internal;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;

public final class DefaultMessageBus implements MessageBus {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessageBus.class);
    private final Transport transport;
    private final MessagingConfig config;
    private final MessagingListener listener;
    private volatile boolean closed;

    public DefaultMessageBus(Transport transport, MessagingConfig config, MessagingListener listener) {
        this.transport = transport; this.config = config; this.listener = SafeListener.wrap(listener);
    }

    @Override public CompletableFuture<Void> publish(Destination dest, Message msg) {
        checkNotClosed();
        HeaderValidator.validateForPublish(msg.headers());
        return transport.publish(dest, msg).whenComplete((v, ex) -> {
            if (ex != null) listener.onError(dest, ex);
            else listener.onPublished(dest);
        });
    }

    @Override public CompletableFuture<Subscription> subscribe(Destination dest, MessageHandler handler) {
        checkNotClosed(); return transport.subscribe(dest, handler);
    }

    @Override public <T> TypedChannel<T> typed(Destination dest, Codec<T> codec) {
        checkNotClosed(); return new TypedChannel<>(this, dest, codec);
    }

    @Override public void close() {
        if (closed) return; closed = true;
        try { transport.close(config.closeTimeout()); } catch (Exception e) { log.warn("Error closing transport", e); }
    }

    private void checkNotClosed() { if (closed) throw new IllegalStateException("MessageBus is closed"); }
}
```

(`SafeListener.wrap` guarantees `onError`/`onPublished` calls here can never throw, so the old inline try/catch is redundant and removed.)

- [ ] **Step 7: Run the full core test suite**

Run: `gradlew.bat :messaging-core:test`
Expected: PASS — `DefaultMessageBusTest`, `MessagingFactoryTest`, `MessagingListenerTest`, and the new `SafeListenerTest` all green.

- [ ] **Step 8: Commit**

```bash
git add messaging-core/src/main/java/com/messaging/internal/SafeListener.java messaging-core/src/main/java/com/messaging/Messaging.java messaging-core/src/main/java/com/messaging/internal/DefaultMessageBus.java messaging-core/src/test/java/com/messaging/internal/SafeListenerTest.java
git commit -m "refactor(core): extract SafeListener.wrap for listener isolation, used by Messaging.connect and DefaultMessageBus"
```

---

## Phase 2 — Conformance suite hooks, `InMemoryTransport` rewrite, new scenarios

### Task 5: Add `BusSettings` and switch `createBus()` to `createBus(BusSettings)`

**Files:**
- Create: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/BusSettings.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/InMemoryConformance.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/AcksBeforeHandlerConformance.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/DropsHeadersConformance.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/QueueFansOutConformance.java`

**Interfaces:**
- Produces: `record BusSettings(MessagingListener listener, Duration closeTimeout, int concurrency)`; `AbstractMessagingConformanceTest.createBus(BusSettings)` (replaces the no-arg version); `InMemoryConformance.createTransport(BusSettings)` (replaces the no-arg version).
- Consumes (later tasks): every scenario that needs non-default settings calls `createBus(new BusSettings(...))` directly.

This task alone will not compile cleanly until Task 6 updates `InMemoryTransport`'s constructor — do both tasks' code changes before running tests, but commit them separately as scoped here; use `git stash` is unnecessary since Task 6 immediately follows in this plan and both are done before the first build in Task 6's Step 4.

- [ ] **Step 1: Add `BusSettings`**

```java
package com.messaging.conformance;

import com.messaging.MessagingListener;
import java.time.Duration;

/** Settings a transport is opened with, mirroring what a real adapter reads from
 * {@code MessagingConfig} via {@code open(config, listener)}. */
public record BusSettings(MessagingListener listener, Duration closeTimeout, int concurrency) {}
```

- [ ] **Step 2: Change the abstract hook**

In `AbstractMessagingConformanceTest.java`, replace:

```java
protected MessageBus bus;

@BeforeEach
void setUp() {
    bus = createBus();
}
```

with:

```java
protected MessageBus bus;

private static final BusSettings DEFAULT_SETTINGS =
    new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(1), 1);

@BeforeEach
void setUp() {
    bus = createBus(DEFAULT_SETTINGS);
}
```

and replace:

```java
/** Create the MessageBus under test. */
protected abstract MessageBus createBus();
```

with:

```java
/** Create the MessageBus under test with the given settings. */
protected abstract MessageBus createBus(BusSettings settings);
```

- [ ] **Step 3: Update `InMemoryConformance`**

Replace its body:

```java
package com.messaging.conformance;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.conformance.faulty.InMemoryTransport;
import com.messaging.internal.DefaultMessageBus;

public class InMemoryConformance extends AbstractMessagingConformanceTest {

    private InMemoryTransport transport;

    /** The transport under test; faulty variants return a deliberately broken one. */
    protected InMemoryTransport createTransport(BusSettings settings) {
        return new InMemoryTransport(settings.listener(), settings.closeTimeout(), settings.concurrency());
    }

    @Override
    protected final MessageBus createBus(BusSettings settings) {
        transport = createTransport(settings);
        return new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(),
            settings.listener());
    }

    @Override
    protected final Destination provisionTopic(String name) {
        Topic topic = Topic.of(name);
        transport.provision(topic);
        return topic;
    }

    @Override
    protected final Destination provisionQueue(String name) {
        Queue queue = Queue.of(name);
        transport.provision(queue);
        return queue;
    }
}
```

`transport` now always points at the most recently created transport — the one from `setUp()`'s default bus, or from any extra bus a scenario creates by calling `createBus(...)` again. `provisionTopic`/`provisionQueue` therefore always provision against whichever bus was created last, which is exactly the bus a scenario calls them against (see Task 9 for the pattern).

- [ ] **Step 4: Update the three faulty `*Conformance` classes**

`AcksBeforeHandlerConformance.java`:

```java
package com.messaging.conformance.faulty;

import com.messaging.conformance.BusSettings;
import com.messaging.conformance.InMemoryConformance;

public class AcksBeforeHandlerConformance extends InMemoryConformance {

    public static final String EXPECTED_FAILURE = "redeliveryOnExceptionalFuture";

    @Override
    protected InMemoryTransport createTransport(BusSettings settings) {
        return new AcksBeforeHandlerTransport(settings.listener(), settings.closeTimeout(), settings.concurrency());
    }
}
```

`DropsHeadersConformance.java`:

```java
package com.messaging.conformance.faulty;

import com.messaging.conformance.BusSettings;
import com.messaging.conformance.InMemoryConformance;

public class DropsHeadersConformance extends InMemoryConformance {

    public static final String EXPECTED_FAILURE = "headersRoundTrip";

    @Override
    protected InMemoryTransport createTransport(BusSettings settings) {
        return new DropsHeadersTransport(settings.listener(), settings.closeTimeout(), settings.concurrency());
    }
}
```

`QueueFansOutConformance.java`:

```java
package com.messaging.conformance.faulty;

import com.messaging.conformance.BusSettings;
import com.messaging.conformance.InMemoryConformance;

public class QueueFansOutConformance extends InMemoryConformance {

    public static final String EXPECTED_FAILURE = "queueCompetingConsumers";

    @Override
    protected InMemoryTransport createTransport(BusSettings settings) {
        return new QueueFansOutTransport(settings.listener(), settings.closeTimeout(), settings.concurrency());
    }
}
```

- [ ] **Step 5: Do not run the build yet**

The module will not compile until Task 6 gives `InMemoryTransport`, `AcksBeforeHandlerTransport`, `DropsHeadersTransport`, and `QueueFansOutTransport` matching three-argument constructors. Proceed directly to Task 6 before building.

- [ ] **Step 6: Commit**

```bash
git add messaging-conformance/src/testFixtures/java/com/messaging/conformance/BusSettings.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/InMemoryConformance.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/AcksBeforeHandlerConformance.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/DropsHeadersConformance.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/QueueFansOutConformance.java
git commit -m "feat(conformance): createBus takes BusSettings(listener, closeTimeout, concurrency)"
```

(This commit will not build in isolation — that's expected and resolved by Task 6's commit landing immediately after. If your workflow requires every commit to build, squash Tasks 5 and 6 into one commit instead; do not run `gradlew.bat test` between them.)

---

### Task 6: Rewrite `InMemoryTransport` for concurrency, §J close semantics, and the `settle` hook

**Files:**
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/InMemoryTransport.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/AcksBeforeHandlerTransport.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/DropsHeadersTransport.java`
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/QueueFansOutTransport.java`

**Interfaces:**
- Consumes: `BusSettings` fields via the constructor `InMemoryTransport(MessagingListener, Duration, int)` (Task 5 already calls this).
- Produces: `protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler, MessagingListener listener, Destination destination)` (signature gains `listener` and `destination`); `protected void ConsumerUnit.settle(Message message, boolean succeeded)` — the sole override point for faulty variants that want to break settlement without touching the receive loop.

- [ ] **Step 1: Replace `InMemoryTransport.java`**

```java
package com.messaging.conformance.faulty;

import com.messaging.*;
import com.messaging.spi.Transport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A correct in-memory transport, used both directly (InMemoryConformance) and as the
 * base class for deliberately faulty variants that prove the conformance suite catches
 * real violations of the delivery contract (§A, §C, §J).
 *
 * <p>{@code concurrency} consumer units per subscription share one deque: a private one
 * per subscriber for a Topic, the destination's single backing deque for a Queue (so
 * separate {@code subscribe} calls on the same queue still compete). Each unit runs its
 * own thread with at most one message in flight. A failed, null, cancelled, or throwing
 * handler outcome re-offers the message to the front of its deque for redelivery (§A).
 * {@code close} (§J) stops accepting new work, waits for in-flight outcomes against one
 * shared deadline, then cancels whatever is still running.
 */
public class InMemoryTransport implements Transport {

    private final MessagingListener listener;
    private final Duration closeTimeout;
    private final int concurrency;

    private final Set<Destination> provisioned = ConcurrentHashMap.newKeySet();
    private final Map<Destination, LinkedBlockingDeque<Message>> queueBacking = new ConcurrentHashMap<>();
    private final Map<Destination, List<LinkedBlockingDeque<Message>>> fanOutDeques = new ConcurrentHashMap<>();
    private final List<ConsumerUnit> consumerUnits = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    public InMemoryTransport(MessagingListener listener, Duration closeTimeout, int concurrency) {
        this.listener = listener;
        this.closeTimeout = closeTimeout;
        this.concurrency = concurrency;
    }

    public final void provision(Destination destination) {
        provisioned.add(destination);
        if (destination instanceof Queue) queueBacking.putIfAbsent(destination, new LinkedBlockingDeque<>());
    }

    @Override
    public CompletableFuture<Void> publish(Destination destination, Message message) {
        Exception unusable = unusable(destination);
        if (unusable != null) return CompletableFuture.failedFuture(unusable);
        deliver(destination, message);
        return CompletableFuture.completedFuture(null);
    }

    private void deliver(Destination destination, Message message) {
        if (fansOut(destination)) {
            for (var q : fanOutDeques.getOrDefault(destination, List.of())) q.add(message);
        } else {
            queueBacking.get(destination).add(message);
        }
    }

    @Override
    public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
        Exception unusable = unusable(destination);
        if (unusable != null) return CompletableFuture.failedFuture(unusable);

        LinkedBlockingDeque<Message> sharedDeque;
        Runnable onClose;
        if (fansOut(destination)) {
            sharedDeque = new LinkedBlockingDeque<>();
            fanOutDeques.computeIfAbsent(destination, d -> new CopyOnWriteArrayList<>()).add(sharedDeque);
            onClose = () -> fanOutDeques.get(destination).remove(sharedDeque);
        } else {
            sharedDeque = queueBacking.get(destination);
            onClose = () -> {};
        }

        List<ConsumerUnit> units = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            ConsumerUnit unit = createConsumerUnit(sharedDeque, handler, listener, destination);
            units.add(unit);
            consumerUnits.add(unit);
            unit.start();
        }

        return CompletableFuture.completedFuture(() -> {
            long deadline = System.nanoTime() + closeTimeout.toNanos();
            units.forEach(ConsumerUnit::requestStop);
            for (ConsumerUnit unit : units) {
                unit.awaitStop(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
                consumerUnits.remove(unit);
            }
            onClose.run();
        });
    }

    /** Whether every subscriber gets its own copy (Topic) rather than competing (Queue). Faulty variants override this. */
    protected boolean fansOut(Destination destination) {
        return destination instanceof Topic;
    }

    /** Extension point for faulty variants that need a different handler or a different settle rule. */
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler,
                                               MessagingListener listener, Destination destination) {
        return new ConsumerUnit(deque, handler, listener, destination);
    }

    @Override
    public void close(Duration timeout) {
        closed = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        consumerUnits.forEach(ConsumerUnit::requestStop);
        for (ConsumerUnit unit : consumerUnits) {
            unit.awaitStop(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        }
        consumerUnits.clear();
        queueBacking.clear();
        fanOutDeques.clear();
    }

    /** Why {@code destination} cannot be published to or subscribed to right now, or null if it can. */
    private Exception unusable(Destination destination) {
        if (closed) return new IllegalStateException("Transport is closed");
        if (!provisioned.contains(destination)) {
            return new MessagingException("No such destination (not provisioned): " + destination);
        }
        return null;
    }

    /** One thread, one message in flight, per §B/§C. Consumes, runs the handler on this
     * thread, then settles — success stops redelivery, failure re-offers the message. */
    protected static class ConsumerUnit {
        private final LinkedBlockingDeque<Message> deque;
        private final MessageHandler handler;
        private final MessagingListener listener;
        private final Destination destination;
        private volatile Thread thread;
        private volatile boolean stopping;
        private volatile CompletableFuture<Void> currentOutcome;

        ConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler,
                     MessagingListener listener, Destination destination) {
            this.deque = deque;
            this.handler = handler;
            this.listener = listener;
            this.destination = destination;
        }

        void start() {
            thread = new Thread(this::run, "in-memory-consumer");
            thread.setDaemon(true);
            thread.start();
        }

        /** Signal this unit to stop taking new messages; does not wait. */
        void requestStop() {
            stopping = true;
        }

        /** §J: wait up to {@code remaining} for the in-flight handler. If it's still
         * running when time is up, cancel its outcome (whose failure path re-offers the
         * message via {@link #settle}) and give the thread a short grace period to exit. */
        void awaitStop(Duration remaining) {
            if (thread == null) return;
            try {
                thread.join(Math.max(0, remaining.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (thread.isAlive()) {
                CompletableFuture<Void> outcome = currentOutcome;
                if (outcome != null) outcome.cancel(true);
                try {
                    thread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void run() {
            while (!stopping) {
                Message message;
                try {
                    message = deque.poll(200, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (message == null) continue;
                processOneMessage(message);
            }
        }

        private void processOneMessage(Message message) {
            CompletableFuture<Void> outcome;
            try {
                outcome = handler.handle(message);
                if (outcome == null) outcome = CompletableFuture.failedFuture(
                    new MessagingException("Handler returned null future"));
            } catch (Throwable synchronousThrow) {
                outcome = CompletableFuture.failedFuture(synchronousThrow);
            }
            currentOutcome = outcome;
            boolean succeeded;
            try {
                outcome.get();
                succeeded = true;
            } catch (Exception failedOrCancelled) {
                succeeded = false;
            }
            currentOutcome = null;
            settle(message, succeeded);
        }

        /** Success: report {@code onConsumed}, do not redeliver. Failure: re-offer the
         * message to the front of the deque and report {@code onError}. Faulty variants
         * override this single method to break settlement deliberately. */
        protected void settle(Message message, boolean succeeded) {
            if (succeeded) {
                listener.onConsumed(destination);
            } else {
                deque.offerFirst(message);
                listener.onError(destination, new MessagingException("Handler did not settle: " + message));
            }
        }
    }
}
```

- [ ] **Step 2: Update `AcksBeforeHandlerTransport.java`**

```java
package com.messaging.conformance.faulty;

import com.messaging.Destination;
import com.messaging.Message;
import com.messaging.MessageHandler;
import com.messaging.MessagingListener;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Faulty variant: never re-offers a failed message, regardless of outcome — as if it had
 * already been acked at dequeue time. Violates §A. Used only by
 * {@code AcksBeforeHandlerConformance} to prove the suite catches it.
 */
public final class AcksBeforeHandlerTransport extends InMemoryTransport {

    public AcksBeforeHandlerTransport(MessagingListener listener, java.time.Duration closeTimeout, int concurrency) {
        super(listener, closeTimeout, concurrency);
    }

    @Override
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler,
                                               MessagingListener listener, Destination destination) {
        return new ConsumerUnit(deque, handler, listener, destination) {
            @Override
            protected void settle(Message message, boolean succeeded) {
                // Bug under test: never re-offer and never report onError, whatever the outcome.
            }
        };
    }
}
```

- [ ] **Step 3: Update `DropsHeadersTransport.java`**

```java
package com.messaging.conformance.faulty;

import com.messaging.Destination;
import com.messaging.Message;
import com.messaging.MessageHandler;
import com.messaging.MessagingListener;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Faulty variant: strips headers before handing the message to the handler. Violates
 * §G (headers round-trip). Used only by {@code DropsHeadersConformance} to prove the
 * suite catches it.
 */
public final class DropsHeadersTransport extends InMemoryTransport {

    public DropsHeadersTransport(MessagingListener listener, java.time.Duration closeTimeout, int concurrency) {
        super(listener, closeTimeout, concurrency);
    }

    @Override
    protected ConsumerUnit createConsumerUnit(LinkedBlockingDeque<Message> deque, MessageHandler handler,
                                               MessagingListener listener, Destination destination) {
        MessageHandler strippingHandler = message -> handler.handle(new Message(message.body(), Map.of()));
        return new ConsumerUnit(deque, strippingHandler, listener, destination);
    }
}
```

- [ ] **Step 4: Update `QueueFansOutTransport.java`**

```java
package com.messaging.conformance.faulty;

import com.messaging.Destination;
import com.messaging.MessagingListener;

/**
 * Faulty variant: delivers every Queue message to ALL subscribers instead of exactly one
 * (fan-out instead of competing-consumers). Used only by {@code QueueFansOutConformance}
 * to prove the suite catches it.
 */
public final class QueueFansOutTransport extends InMemoryTransport {

    public QueueFansOutTransport(MessagingListener listener, java.time.Duration closeTimeout, int concurrency) {
        super(listener, closeTimeout, concurrency);
    }

    @Override
    protected boolean fansOut(Destination destination) {
        return true;
    }
}
```

- [ ] **Step 5: Build and run the control suite plus the meta-test**

Run: `gradlew.bat :messaging-conformance:test`
Expected: PASS for `InMemoryConformanceTest` (still exercises every existing scenario against the rewritten transport) and `MetaConformanceTest` (all three faulty transports still caught).

- [ ] **Step 6: Break-check — prove `settle` failure still redelivers**

Temporarily change `InMemoryTransport.ConsumerUnit.settle` to remove the `deque.offerFirst(message);` line on the failure branch, run `gradlew.bat :messaging-conformance:test --tests "com.messaging.conformance.InMemoryConformanceTest.redeliveryOnExceptionalFuture"`, confirm it FAILS, then revert the change.

- [ ] **Step 7: Commit (together with Task 5's staged files if not already committed)**

```bash
git add messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/InMemoryTransport.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/AcksBeforeHandlerTransport.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/DropsHeadersTransport.java messaging-conformance/src/testFixtures/java/com/messaging/conformance/faulty/QueueFansOutTransport.java
git commit -m "feat(conformance): InMemoryTransport supports concurrency, §J close semantics, and a settle() hook"
```

---

### Task 7: Make `listenerThrowDoesNotChangeDelivery` a real test

**Files:**
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java`

**Interfaces:**
- Consumes: `createBus(BusSettings)` (Task 5), `BusSettings` record (Task 5).

- [ ] **Step 1: Replace the vacuous scenario**

Replace the existing `listenerThrowDoesNotChangeDelivery` method with:

```java
@Test
void listenerThrowDoesNotChangeDelivery() {
    MessagingListener throwing = new MessagingListener() {
        @Override public void onPublished(Destination d) { throw new RuntimeException("boom"); }
        @Override public void onConsumed(Destination d) { throw new RuntimeException("boom"); }
        @Override public void onError(Destination d, Throwable err) { throw new RuntimeException("boom"); }
        @Override public void onConnectionStateChanged(ConnectionState s) { throw new RuntimeException("boom"); }
    };
    try (MessageBus throwingBus = createBus(new BusSettings(throwing, Duration.ofSeconds(1), 1))) {
        Destination queue = provisionQueue("listener-throw");
        var received = new CopyOnWriteArrayList<String>();
        throwingBus.subscribe(queue, recordingHandler(received)).join();

        assertThatCode(() -> throwingBus.publish(queue, "m1".getBytes()).join()).doesNotThrowAnyException();
        assertThatCode(() -> throwingBus.publish(queue, "m2".getBytes()).join()).doesNotThrowAnyException();

        await().atMost(TIMEOUT).untilAsserted(() ->
            assertThat(received).containsExactlyInAnyOrder("m1", "m2"));
    }
}
```

Add the needed import: `import com.messaging.ConnectionState;` (if not already present in the file's `com.messaging.*` wildcard import — check first; the file already has `import com.messaging.*;`, so no new import is needed).

- [ ] **Step 2: Run it against the in-memory control**

Run: `gradlew.bat :messaging-conformance:test --tests "com.messaging.conformance.InMemoryConformanceTest.listenerThrowDoesNotChangeDelivery"`
Expected: PASS — `SafeListener` (Task 4) isolates the throw, and the rewritten `InMemoryTransport` (Task 6) fires `onConsumed`/`onError` through that listener.

- [ ] **Step 3: Break-check — prove the test can fail**

Temporarily make `DefaultMessageBus`'s constructor skip the `SafeListener.wrap` call (pass `listener` through raw), rerun the same test, confirm it FAILS (the thrown exception propagates and the publish future fails, so `assertThatCode(...).doesNotThrowAnyException()` fails), then revert.

- [ ] **Step 4: Run the full conformance module test**

Run: `gradlew.bat :messaging-conformance:test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java
git commit -m "fix(conformance): listenerThrowDoesNotChangeDelivery now installs a real throwing listener"
```

---

### Task 8: Split missing-destination scenarios into Topic and Queue variants

**Files:**
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java`

- [ ] **Step 1: Replace the two existing scenarios with four**

Remove `publishToMissingDestinationFails` and `subscribeToMissingDestinationFails`. Add:

```java
@Test
void publishToMissingTopicFails() {
    Topic topic = Topic.of("missing-topic-" + System.nanoTime());
    assertThatThrownBy(() -> bus.publish(topic, new byte[0]).join())
        .hasCauseInstanceOf(MessagingException.class);
}

@Test
void publishToMissingQueueFails() {
    Queue queue = Queue.of("missing-queue-" + System.nanoTime());
    assertThatThrownBy(() -> bus.publish(queue, new byte[0]).join())
        .hasCauseInstanceOf(MessagingException.class);
}

@Test
void subscribeToMissingTopicFails() {
    Topic topic = Topic.of("missing-topic-sub-" + System.nanoTime());
    assertThatThrownBy(() ->
        bus.subscribe(topic, message -> CompletableFuture.completedFuture(null)).join())
        .hasCauseInstanceOf(MessagingException.class);
}

@Test
void subscribeToMissingQueueFails() {
    Queue queue = Queue.of("missing-queue-sub-" + System.nanoTime());
    assertThatThrownBy(() ->
        bus.subscribe(queue, message -> CompletableFuture.completedFuture(null)).join())
        .hasCauseInstanceOf(MessagingException.class);
}
```

- [ ] **Step 2: Run the control suite**

Run: `gradlew.bat :messaging-conformance:test --tests "com.messaging.conformance.InMemoryConformanceTest"`
Expected: PASS — all four new scenarios pass against the in-memory transport's existing `unusable()` check, which already covers both destination types.

- [ ] **Step 3: Commit**

```bash
git add messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java
git commit -m "test(conformance): split missing-destination scenarios into Topic and Queue variants"
```

---

### Task 9: Add the in-flight close scenarios

**Files:**
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java`

- [ ] **Step 1: Add three new scenarios**

```java
@Test
void subscriptionCloseWaitsForInFlightHandler() throws Exception {
    try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(2), 1))) {
        Destination queue = provisionQueue("close-waits-in-flight");
        var received = new CopyOnWriteArrayList<String>();
        var handlerStarted = new CompletableFuture<Void>();
        Subscription subscription = extraBus.subscribe(queue, message -> {
            handlerStarted.complete(null);
            return CompletableFuture.runAsync(() -> {
                try { Thread.sleep(300); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                received.add(new String(message.body()));
            });
        }).join();

        extraBus.publish(queue, "in-flight".getBytes()).join();
        handlerStarted.get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);

        long startNanos = System.nanoTime();
        subscription.close();
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertThat(elapsedMs).isGreaterThanOrEqualTo(250);
        assertThat(received).containsExactly("in-flight");

        var redelivered = new CopyOnWriteArrayList<String>();
        extraBus.subscribe(queue, recordingHandler(redelivered)).join();
        Thread.sleep(300);
        assertThat(redelivered).isEmpty();
    }
}

@Test
void subscriptionCloseCancelsStuckHandlerUnsettled() throws Exception {
    try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofMillis(500), 1))) {
        Destination queue = provisionQueue("close-cancels-stuck");
        var neverCompletes = new CompletableFuture<Void>();
        Subscription subscription = extraBus.subscribe(queue, message -> neverCompletes).join();

        extraBus.publish(queue, "stuck".getBytes()).join();
        Thread.sleep(100);

        long startNanos = System.nanoTime();
        subscription.close();
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertThat(elapsedMs).isLessThan(2500);
        assertThat(neverCompletes).isCancelled();

        var redelivered = new CopyOnWriteArrayList<String>();
        extraBus.subscribe(queue, recordingHandler(redelivered)).join();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(redelivered).containsExactly("stuck"));
    }
}

@Test
void busCloseCancelsStuckHandler() throws Exception {
    MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofMillis(500), 1));
    Destination queue = provisionQueue("bus-close-cancels-stuck");
    var neverCompletes = new CompletableFuture<Void>();
    extraBus.subscribe(queue, message -> neverCompletes).join();
    extraBus.publish(queue, "stuck".getBytes()).join();
    Thread.sleep(100);

    long startNanos = System.nanoTime();
    extraBus.close();
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertThat(elapsedMs).isBetween(400L, 2500L);
    assertThat(neverCompletes).isCancelled();
}
```

Add the missing import `java.util.concurrent.TimeUnit` if not already present (check the file's imports; it currently imports `java.util.concurrent.CompletableFuture`, `CopyOnWriteArrayList`, `AtomicInteger` — add `TimeUnit` alongside them, or fully qualify as shown above and skip the import).

- [ ] **Step 2: Run the new scenarios against the in-memory control**

Run: `gradlew.bat :messaging-conformance:test --tests "com.messaging.conformance.InMemoryConformanceTest.subscriptionCloseWaitsForInFlightHandler" --tests "com.messaging.conformance.InMemoryConformanceTest.subscriptionCloseCancelsStuckHandlerUnsettled" --tests "com.messaging.conformance.InMemoryConformanceTest.busCloseCancelsStuckHandler"`
Expected: PASS.

- [ ] **Step 3: Break-check — prove the stuck-handler scenarios can fail**

Temporarily change `ConsumerUnit.awaitStop` to skip cancelling `currentOutcome` (just `thread.join(remaining.toMillis())` and return). Rerun `subscriptionCloseCancelsStuckHandlerUnsettled` and `busCloseCancelsStuckHandler`; confirm both FAIL (they hang past the assertion bound or the future is never cancelled). Revert the change.

- [ ] **Step 4: Run the full conformance module test**

Run: `gradlew.bat :messaging-conformance:test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java
git commit -m "test(conformance): add in-flight close scenarios (subscription wait/cancel, bus close cancel)"
```

---

### Task 10: Add the concurrency scenarios

**Files:**
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java`

- [ ] **Step 1: Add two new scenarios**

```java
@Test
void topicConcurrencyDeliversOncePerSubscription() {
    try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(1), 2))) {
        Destination topic = provisionTopic("topic-concurrency");
        var received = new CopyOnWriteArrayList<String>();
        extraBus.subscribe(topic, recordingHandler(received)).join();

        for (int i = 0; i < 4; i++) extraBus.publish(topic, ("m" + i).getBytes()).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).hasSize(4));
        assertThat(received).containsExactlyInAnyOrder("m0", "m1", "m2", "m3");
    }
}

@Test
void queueConcurrencyRunsUnitsInParallel() throws Exception {
    try (MessageBus extraBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(2), 2))) {
        Destination queue = provisionQueue("queue-concurrency-parallel");
        var bothStarted = new java.util.concurrent.CountDownLatch(2);
        var releaseHandlers = new CompletableFuture<Void>();
        extraBus.subscribe(queue, message -> {
            bothStarted.countDown();
            return releaseHandlers;
        }).join();

        extraBus.publish(queue, "m1".getBytes()).join();
        extraBus.publish(queue, "m2".getBytes()).join();

        assertThat(bothStarted.await(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        releaseHandlers.complete(null);
    }
}
```

- [ ] **Step 2: Run the new scenarios against the in-memory control**

Run: `gradlew.bat :messaging-conformance:test --tests "com.messaging.conformance.InMemoryConformanceTest.topicConcurrencyDeliversOncePerSubscription" --tests "com.messaging.conformance.InMemoryConformanceTest.queueConcurrencyRunsUnitsInParallel"`
Expected: PASS — the rewritten `InMemoryTransport` (Task 6) starts `concurrency` units sharing one deque per subscription.

- [ ] **Step 3: Break-check — prove `topicConcurrencyDeliversOncePerSubscription` can fail**

Temporarily change `InMemoryTransport.subscribe` so each of the `concurrency` units for a Topic gets its **own** private deque (instead of sharing `sharedDeque`) and instead deliver each published message to every one of that subscription's per-unit deques. Rerun the test; confirm it FAILS (8 deliveries instead of 4, or duplicates). Revert.

- [ ] **Step 4: Run the full conformance module test**

Run: `gradlew.bat :messaging-conformance:test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java
git commit -m "test(conformance): add topic/queue concurrency scenarios (§C)"
```

---

### Task 11: Fix the outage-publish scenario and add `publishDuringOutageFails` / `busCloseRedeliversInFlightOnNextStart`

**Files:**
- Modify: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConnectivityTest.java`

**Interfaces:**
- Produces: `protected abstract void publishOutOfBand(Destination destination, byte[] body);` — implemented later by `ArtemisConformanceTest`/`IbmMqConformanceTest` (Phase 5/6) using a bus wired to the `oob` Toxiproxy link.

- [ ] **Step 1: Add the abstract hook and fix the existing scenario**

Add near the other abstract methods:

```java
/** Publish over a link that bypasses whatever {@link #cutNetwork()} cuts, so a test can
 * put a message on a broker-backed destination while the adapter under test is offline. */
protected abstract void publishOutOfBand(Destination destination, byte[] body);
```

Replace the body of `queueMessagesPublishedDuringOutageDeliveredAfterRecovery`:

```java
@Test
void queueMessagesPublishedDuringOutageDeliveredAfterRecovery() {
    Destination queue = provisionQueue("outage-delivery");
    var received = new CopyOnWriteArrayList<String>();
    awaitSubscribed(queue, message -> {
        received.add(new String(message.body()));
        return CompletableFuture.completedFuture(null);
    });

    cutNetwork();
    publishOutOfBand(queue, "during-outage".getBytes());
    restoreNetwork();

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("during-outage"));
}
```

- [ ] **Step 2: Add `publishDuringOutageFails`**

```java
private static final Duration OUTAGE_PUBLISH_BOUND = Duration.ofSeconds(60);

@Test
void publishDuringOutageFails() {
    Destination queue = provisionQueue("outage-publish-fails");
    cutNetwork();
    try {
        long startNanos = System.nanoTime();
        assertThatThrownBy(() -> bus.publish(queue, "during-outage".getBytes()).join())
            .hasCauseInstanceOf(com.messaging.MessagingException.class);
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        assertThat(elapsedMs).isLessThan(OUTAGE_PUBLISH_BOUND.toMillis());
    } finally {
        restoreNetwork();
    }
}
```

- [ ] **Step 3: Add `busCloseRedeliversInFlightOnNextStart`**

```java
@Test
void busCloseRedeliversInFlightOnNextStart() throws Exception {
    Destination queue = provisionQueue("bus-close-redelivers");
    var neverCompletes = new CompletableFuture<Void>();
    bus.subscribe(queue, message -> neverCompletes).join();
    publish(queue, "stuck".getBytes());
    Thread.sleep(300);

    bus.close();

    try (MessageBus freshBus = createBus(new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(1), 1))) {
        var received = new CopyOnWriteArrayList<String>();
        freshBus.subscribe(queue, recordingHandler(received)).join();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("stuck"));
    }
}
```

Add imports if missing: `com.messaging.MessagingListener`, `com.messaging.BusSettings` is actually `com.messaging.conformance.BusSettings` (same package, no import needed), `java.time.Duration` (already imported).

- [ ] **Step 4: This module cannot yet build**

`AbstractMessagingConnectivityTest` has no concrete subclass until Phase 5 (`ArtemisConformanceTest`) exists — there is currently no compile-time consumer of the new abstract `publishOutOfBand` method in this module, so this compiles fine on its own (it's an abstract class). Confirm with:

Run: `gradlew.bat :messaging-conformance:test`
Expected: PASS (this class has no concrete implementors in `messaging-conformance` itself, so nothing new actually executes here — the real proof comes in Phase 5's `ArtemisConformanceTest`).

- [ ] **Step 5: Commit**

```bash
git add messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConnectivityTest.java
git commit -m "fix(conformance): outage-publish scenario now actually publishes during the cut; add publishDuringOutageFails and busCloseRedeliversInFlightOnNextStart"
```

---

## Phase 3 — `ConnectionFactoryBuilder` (no broker required)

### Task 12: `ConnectionFactoryBuilder` — FQCN instantiation, bean-setter passthrough, §E rejection

**Files:**
- Create: `messaging-jms/src/main/java/com/messaging/jms/ConnectionFactoryBuilder.java`
- Create: `messaging-jms/src/test/java/com/messaging/jms/StubConnectionFactory.java`
- Create: `messaging-jms/src/test/java/com/messaging/jms/ConnectionFactoryBuilderTest.java`

**Interfaces:**
- Produces: `static jakarta.jms.ConnectionFactory ConnectionFactoryBuilder.build(MessagingConfig config)` — package-private, used later by `JmsTransport.connect` (Task 13).
- Consumes: `MessagingConfig.passthroughProperties()` (existing), `MessagingConfig.url()` (existing).

- [ ] **Step 1: Write the stub factory (no broker involved)**

```java
package com.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

/** A minimal ConnectionFactory used only to test ConnectionFactoryBuilder without a real broker. */
public class StubConnectionFactory implements ConnectionFactory {
    String brokerUrl;
    int port;
    long timeout;
    boolean useSsl;

    public void setBrokerURL(String url) { this.brokerUrl = url; }
    public void setPort(int port) { this.port = port; }
    public void setTimeout(long timeout) { this.timeout = timeout; }
    public void setUseSsl(boolean useSsl) { this.useSsl = useSsl; }

    @Override public Connection createConnection() { return null; }
    @Override public Connection createConnection(String userName, String password) { return null; }
    @Override public JMSContext createContext() { return null; }
    @Override public JMSContext createContext(String userName, String password) { return null; }
    @Override public JMSContext createContext(String userName, String password, int sessionMode) { return null; }
    @Override public JMSContext createContext(int sessionMode) { return null; }
}
```

- [ ] **Step 2: Write the failing tests**

```java
package com.messaging.jms;

import com.messaging.MessagingException;
import com.messaging.config.MessagingConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionFactoryBuilderTest {

    @Test void instantiatesByClassNameAndAppliesSetters() {
        var config = MessagingConfig.builder()
            .url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("brokerURL", "tcp://localhost:61616")
            .property("port", "61616")
            .property("timeout", "5000")
            .property("useSsl", "true")
            .build();

        var factory = (StubConnectionFactory) ConnectionFactoryBuilder.build(config);

        assertThat(factory.brokerUrl).isEqualTo("tcp://localhost:61616");
        assertThat(factory.port).isEqualTo(61616);
        assertThat(factory.timeout).isEqualTo(5000L);
        assertThat(factory.useSsl).isTrue();
    }

    @Test void missingConnectionFactoryKeyFails() {
        var config = MessagingConfig.builder().url("jms://localhost").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("connection-factory");
    }

    @Test void unknownClassNameFails() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", "com.example.DoesNotExist").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("com.example.DoesNotExist");
    }

    @Test void unknownSetterFailsNamingTheKey() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("noSuchProperty", "value").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("noSuchProperty");
    }

    @Test void deliveryModeNonPersistentIsRejectedBeforeSetterLookup() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("deliveryMode", "NON_PERSISTENT").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("deliveryMode")
            .hasMessageContaining("PERSISTENT");
    }

    @Test void deliveryModeNumericOneIsRejected() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("deliveryMode", "1").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("deliveryMode");
    }

    @Test void uncoercibleValueFailsNamingTheKey() {
        var config = MessagingConfig.builder().url("jms://localhost")
            .property("connection-factory", StubConnectionFactory.class.getName())
            .property("port", "not-a-number").build();
        assertThatThrownBy(() -> ConnectionFactoryBuilder.build(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("port");
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `gradlew.bat :messaging-jms:test`
Expected: FAIL — compile error, `ConnectionFactoryBuilder` does not exist yet.

- [ ] **Step 4: Write `ConnectionFactoryBuilder`**

```java
package com.messaging.jms;

import com.messaging.MessagingException;
import com.messaging.config.MessagingConfig;
import jakarta.jms.ConnectionFactory;

import java.lang.reflect.Method;
import java.util.Map;

/** Builds a provider's {@link ConnectionFactory} purely from {@code messaging.jms.*}
 * passthrough config: {@code connection-factory} names the class by FQCN, and every other
 * key is applied as a public single-argument bean setter (§I amendment). No provider
 * class or constant is referenced anywhere in this adapter. */
final class ConnectionFactoryBuilder {

    private static final String CONNECTION_FACTORY_KEY = "connection-factory";

    private ConnectionFactoryBuilder() {}

    static ConnectionFactory build(MessagingConfig config) {
        Map<String, String> props = config.passthroughProperties();
        String className = props.get(CONNECTION_FACTORY_KEY);
        if (className == null) {
            throw new MessagingException("Missing required key 'messaging.jms." + CONNECTION_FACTORY_KEY + "'");
        }
        ConnectionFactory factory = instantiate(className);
        for (var entry : props.entrySet()) {
            if (entry.getKey().equals(CONNECTION_FACTORY_KEY)) continue;
            applySetter(factory, className, entry.getKey(), entry.getValue());
        }
        return factory;
    }

    private static ConnectionFactory instantiate(String className) {
        try {
            Class<?> factoryClass = Class.forName(className);
            Object instance = factoryClass.getDeclaredConstructor().newInstance();
            if (!(instance instanceof ConnectionFactory factory)) {
                throw new MessagingException("Class named by 'messaging.jms." + CONNECTION_FACTORY_KEY
                    + "' (" + className + ") does not implement jakarta.jms.ConnectionFactory");
            }
            return factory;
        } catch (ReflectiveOperationException e) {
            throw new MessagingException("Cannot instantiate connection factory class named by key '"
                + CONNECTION_FACTORY_KEY + "': " + className, e);
        }
    }

    private static void applySetter(ConnectionFactory factory, String className, String key, String value) {
        if (key.equals("deliveryMode") && (value.equals("NON_PERSISTENT") || value.equals("1"))) {
            throw new MessagingException("messaging.jms.deliveryMode=" + value + " is rejected: this "
                + "library enforces PERSISTENT delivery (§E) and will not let a config file weaken it");
        }
        String setterName = "set" + Character.toUpperCase(key.charAt(0)) + key.substring(1);
        Method setter = findSetter(factory.getClass(), setterName);
        if (setter == null) {
            throw new MessagingException("No public setter '" + setterName + "' on " + className
                + " for key 'messaging.jms." + key + "'");
        }
        Object coerced = coerce(key, setter.getParameterTypes()[0], value);
        try {
            setter.invoke(factory, coerced);
        } catch (ReflectiveOperationException e) {
            throw new MessagingException("Failed to apply 'messaging.jms." + key + "' via " + setterName, e);
        }
    }

    private static Method findSetter(Class<?> factoryClass, String setterName) {
        for (Method m : factoryClass.getMethods()) {
            if (m.getName().equals(setterName) && m.getParameterCount() == 1) return m;
        }
        return null;
    }

    private static Object coerce(String key, Class<?> paramType, String value) {
        try {
            if (paramType == String.class) return value;
            if (paramType == int.class || paramType == Integer.class) return Integer.parseInt(value);
            if (paramType == long.class || paramType == Long.class) return Long.parseLong(value);
            if (paramType == boolean.class || paramType == Boolean.class) return Boolean.parseBoolean(value);
        } catch (NumberFormatException e) {
            throw new MessagingException("Cannot coerce 'messaging.jms." + key + "'=" + value
                + " to " + paramType.getSimpleName(), e);
        }
        throw new MessagingException("Unsupported setter parameter type " + paramType.getSimpleName()
            + " for key 'messaging.jms." + key + "'");
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `gradlew.bat :messaging-jms:test`
Expected: PASS, all seven tests in `ConnectionFactoryBuilderTest`.

- [ ] **Step 6: Break-check — prove the §E rejection actually runs before setter lookup**

Temporarily delete the `if (key.equals("deliveryMode") && ...)` block in `applySetter`. Rerun `deliveryModeNonPersistentIsRejectedBeforeSetterLookup`; confirm it now FAILS with a "No public setter 'setDeliveryMode'" message instead of the §E message (since `StubConnectionFactory` has no `setDeliveryMode`), proving the test actually distinguishes the two failure reasons. Revert the deletion.

- [ ] **Step 7: Commit**

```bash
git add messaging-jms/src/main/java/com/messaging/jms/ConnectionFactoryBuilder.java messaging-jms/src/test/java/com/messaging/jms/StubConnectionFactory.java messaging-jms/src/test/java/com/messaging/jms/ConnectionFactoryBuilderTest.java
git commit -m "feat(jms): ConnectionFactoryBuilder — FQCN instantiation, bean-setter passthrough, §E deliveryMode rejection"
```

---

## Phase 4 — `JmsTransportProvider`, `JmsTransport`, `JmsConsumerUnit`

### Task 13: `JmsTransportProvider` + `JmsTransport` connect and publish

**Files:**
- Create: `messaging-jms/src/main/java/com/messaging/jms/JmsTransportProvider.java`
- Create: `messaging-jms/src/main/java/com/messaging/jms/JmsTransport.java`
- Create: `messaging-jms/src/main/resources/META-INF/services/com.messaging.spi.TransportProvider`

**Interfaces:**
- Produces: `com.messaging.jms.JmsTransportProvider` (scheme `"jms"`); `com.messaging.jms.JmsTransport` implementing `com.messaging.spi.Transport`; package-visible `static jakarta.jms.Destination JmsTransport.resolveDestination(Session, com.messaging.Destination)` for reuse by `JmsConsumerUnit` (Task 14).
- Consumes: `ConnectionFactoryBuilder.build(MessagingConfig)` (Task 12).

This task covers connect and publish only; subscribe is stubbed to throw `UnsupportedOperationException` until Task 14 supplies `JmsConsumerUnit`. There is no unit test for this task — connect/publish are proven end-to-end against a real broker in Phase 5's `ArtemisConformanceTest`. That is consistent with the spec: `ConnectionFactoryBuilder` is the only piece unit-tested without a broker.

- [ ] **Step 1: Write `JmsTransportProvider`**

```java
package com.messaging.jms;

import com.messaging.MessagingListener;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;

public final class JmsTransportProvider implements TransportProvider {
    @Override public String scheme() { return "jms"; }

    @Override public Transport open(MessagingConfig config, MessagingListener listener) {
        return JmsTransport.connect(config, listener);
    }
}
```

- [ ] **Step 2: Register it via `META-INF/services`**

Create `messaging-jms/src/main/resources/META-INF/services/com.messaging.spi.TransportProvider` containing exactly:

```
com.messaging.jms.JmsTransportProvider
```

- [ ] **Step 3: Write `JmsTransport` (connect + publish; subscribe stubbed)**

```java
package com.messaging.jms;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import jakarta.jms.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

/**
 * The JMS adapter's {@link Transport}. Connects with the URL's userinfo (never its host or
 * port — the broker address lives in factory properties, §I amendment). Publishes through
 * one lock-guarded, non-transacted session (§E); consumer units never touch it. Subscribing
 * spawns {@code config.concurrency()} {@link JmsConsumerUnit}s per call.
 */
public final class JmsTransport implements Transport {

    private final Connection connection;
    private final Session publishSession;
    private final MessageProducer publishProducer;
    private final Object publishLock = new Object();
    private final MessagingListener listener;
    private final List<JmsConsumerUnit> consumerUnits = new CopyOnWriteArrayList<>();
    private final int concurrency;
    private final String clientId;
    private volatile boolean closed;

    private JmsTransport(Connection connection, Session publishSession, MessageProducer publishProducer,
                          MessagingListener listener, int concurrency, String clientId) {
        this.connection = connection;
        this.publishSession = publishSession;
        this.publishProducer = publishProducer;
        this.listener = listener;
        this.concurrency = concurrency;
        this.clientId = clientId;
    }

    static JmsTransport connect(MessagingConfig config, MessagingListener listener) {
        ConnectionFactory factory = ConnectionFactoryBuilder.build(config);
        String userInfo = config.url().getUserInfo();

        ExecutorService connectExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "jms-connect");
            t.setDaemon(true);
            return t;
        });
        Future<Connection> future = connectExecutor.submit(() ->
            userInfo != null ? createWithUserInfo(factory, userInfo) : factory.createConnection());

        Connection connection;
        try {
            connection = future.get(config.connectTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            future.cancel(true);
            connectExecutor.execute(() -> { try { closeQuietly(future.get()); } catch (Exception ignored) {} });
            connectExecutor.shutdown();
            throw new MessagingException("Failed to connect to JMS broker within " + config.connectTimeout(), e);
        }
        connectExecutor.shutdown();

        try {
            connection.setClientID(config.clientId());
            connection.setExceptionListener(ex -> listener.onConnectionStateChanged(ConnectionState.DISCONNECTED));
            // Non-transacted publish session (§B): the publish path never commits/rolls
            // back — it is a single synchronous send guarded by publishLock.
            Session publishSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer publishProducer = publishSession.createProducer(null);
            publishProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();
            listener.onConnectionStateChanged(ConnectionState.CONNECTED);
            return new JmsTransport(connection, publishSession, publishProducer, listener,
                config.concurrency(), config.clientId());
        } catch (Exception e) {
            closeQuietly(connection);
            throw new MessagingException("Failed to initialise JMS connection", e);
        }
    }

    private static Connection createWithUserInfo(ConnectionFactory factory, String userInfo) throws JMSException {
        String[] parts = userInfo.split(":", 2);
        String user = parts[0];
        String password = parts.length > 1 ? parts[1] : "";
        return factory.createConnection(user, password);
    }

    private static void closeQuietly(Connection connection) {
        try { connection.close(); } catch (Exception ignored) {}
    }

    @Override
    public CompletableFuture<Void> publish(Destination destination, Message message) {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("Transport is closed"));
        try {
            synchronized (publishLock) {
                jakarta.jms.Destination jmsDestination = resolveDestination(publishSession, destination);
                BytesMessage bytesMessage = publishSession.createBytesMessage();
                bytesMessage.writeBytes(message.body());
                for (var entry : message.headers().entrySet()) {
                    bytesMessage.setStringProperty(entry.getKey(), entry.getValue());
                }
                publishProducer.send(jmsDestination, bytesMessage);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new MessagingException("Failed to publish to " + destination, e));
        }
    }

    /** Shared by publish (this class) and each consumer unit's own session (Task 14). */
    static jakarta.jms.Destination resolveDestination(Session session, Destination destination) throws JMSException {
        return switch (destination) {
            case Topic topic -> session.createTopic(topic.name());
            case Queue queue -> session.createQueue(queue.name());
        };
    }

    String clientId() { return clientId; }
    Connection connection() { return connection; }
    MessagingListener listener() { return listener; }

    @Override
    public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
        throw new UnsupportedOperationException("implemented in Task 14");
    }

    @Override
    public void close(Duration timeout) {
        if (closed) return;
        closed = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        for (JmsConsumerUnit unit : consumerUnits) {
            unit.stop(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        }
        consumerUnits.clear();
        try { synchronized (publishLock) { publishSession.close(); } } catch (Exception ignored) {}
        closeQuietly(connection);
    }
}
```

- [ ] **Step 4: Compile the module**

Run: `gradlew.bat :messaging-jms:compileJava`
Expected: succeeds. `JmsConsumerUnit` doesn't exist yet, but nothing in this file references it except the `List<JmsConsumerUnit>` field and `close()`'s loop — both compile fine once `JmsConsumerUnit` is created in Task 14. If the compiler complains `JmsConsumerUnit` is undefined, that's expected: proceed directly to Task 14 before building again (mirrors the Task 5/6 pairing in Phase 2).

- [ ] **Step 5: Commit**

```bash
git add messaging-jms/src/main/java/com/messaging/jms/JmsTransportProvider.java messaging-jms/src/main/java/com/messaging/jms/JmsTransport.java messaging-jms/src/main/resources/META-INF/services/com.messaging.spi.TransportProvider
git commit -m "feat(jms): JmsTransportProvider + JmsTransport connect/publish (subscribe pending JmsConsumerUnit)"
```

---

### Task 14: `JmsConsumerUnit` — the owner loop, and wiring `JmsTransport.subscribe`

**Files:**
- Create: `messaging-jms/src/main/java/com/messaging/jms/JmsConsumerUnit.java`
- Modify: `messaging-jms/src/main/java/com/messaging/jms/JmsTransport.java`

**Interfaces:**
- Produces: `JmsConsumerUnit(Connection, Destination, String sharedSubscriptionName, MessageHandler, MessagingListener)`, `CompletableFuture<Void> start()`, `void stop(Duration timeout)`.
- Consumes: `JmsTransport.resolveDestination(Session, Destination)` (Task 13).

- [ ] **Step 1: Write `JmsConsumerUnit`**

```java
package com.messaging.jms;

import com.messaging.*;
import jakarta.jms.*;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * One JMS consumer's owner loop (§B). Its own thread creates the transacted {@link Session}
 * and {@link MessageConsumer}, completes the "ready" future, then loops on
 * {@code receive(500ms)}: map the message, run the handler on this thread, commit on
 * success or rollback on failure. The only cross-thread calls are {@link #stop}'s signal,
 * a bounded {@code Thread.join}, and, as a last resort past the deadline, {@code close()}.
 */
final class JmsConsumerUnit {

    private static final long RECEIVE_TIMEOUT_MS = 500;

    private final Connection connection;
    private final Destination destination;
    private final String sharedSubscriptionName;
    private final MessageHandler handler;
    private final MessagingListener listener;

    private volatile Session session;
    private volatile MessageConsumer consumer;
    private volatile Thread thread;
    private volatile boolean stopping;
    private volatile CompletableFuture<Void> currentOutcome;

    JmsConsumerUnit(Connection connection, Destination destination, String sharedSubscriptionName,
                     MessageHandler handler, MessagingListener listener) {
        this.connection = connection;
        this.destination = destination;
        this.sharedSubscriptionName = sharedSubscriptionName;
        this.handler = handler;
        this.listener = listener;
    }

    /** Starts the owner thread; the returned future completes once the consumer exists. */
    CompletableFuture<Void> start() {
        CompletableFuture<Void> ready = new CompletableFuture<>();
        thread = new Thread(() -> run(ready), "jms-consumer-" + destination);
        thread.setDaemon(true);
        thread.start();
        return ready;
    }

    private void run(CompletableFuture<Void> ready) {
        try {
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            jakarta.jms.Destination jmsDestination = JmsTransport.resolveDestination(session, destination);
            if (destination instanceof Topic && sharedSubscriptionName != null) {
                consumer = session.createSharedConsumer((jakarta.jms.Topic) jmsDestination, sharedSubscriptionName);
            } else {
                consumer = session.createConsumer(jmsDestination);
            }
        } catch (Exception e) {
            ready.completeExceptionally(new MessagingException("Failed to create consumer for " + destination, e));
            return;
        }
        ready.complete(null);

        while (!stopping) {
            jakarta.jms.Message received;
            try {
                received = consumer.receive(RECEIVE_TIMEOUT_MS);
            } catch (Exception e) {
                listener.onError(destination, new MessagingException("receive() failed for " + destination, e));
                sleepQuietly(RECEIVE_TIMEOUT_MS);
                continue;
            }
            if (received == null) continue;
            processOneMessage(received);
        }
        closeQuietly();
    }

    private void processOneMessage(jakarta.jms.Message received) {
        CompletableFuture<Void> outcome;
        if (!(received instanceof BytesMessage bytesMessage)) {
            outcome = CompletableFuture.failedFuture(
                new MessagingException("Unsupported JMS message type: " + received.getClass()));
        } else {
            try {
                Message message = toMessage(bytesMessage);
                outcome = invokeHandler(message);
            } catch (Exception mappingFailed) {
                outcome = CompletableFuture.failedFuture(mappingFailed);
            }
        }
        currentOutcome = outcome;
        boolean succeeded;
        try {
            outcome.get();
            succeeded = true;
        } catch (Exception failedOrCancelled) {
            succeeded = false;
        }
        currentOutcome = null;
        settle(succeeded);
    }

    private CompletableFuture<Void> invokeHandler(Message message) {
        try {
            CompletableFuture<Void> outcome = handler.handle(message);
            return outcome != null ? outcome
                : CompletableFuture.failedFuture(new MessagingException("Handler returned null future"));
        } catch (Throwable synchronousThrow) {
            return CompletableFuture.failedFuture(synchronousThrow);
        }
    }

    private Message toMessage(BytesMessage bytesMessage) throws JMSException {
        byte[] body = new byte[(int) bytesMessage.getBodyLength()];
        bytesMessage.readBytes(body);
        var headers = new LinkedHashMap<String, String>();
        var names = bytesMessage.getPropertyNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            headers.put(name, String.valueOf(bytesMessage.getObjectProperty(name)));
        }
        if (bytesMessage.getJMSRedelivered()) headers.put("messaging.redelivered", "true");
        return new Message(body, headers);
    }

    private void settle(boolean succeeded) {
        try {
            if (succeeded) {
                session.commit();
                listener.onConsumed(destination);
            } else {
                session.rollback();
                listener.onError(destination, new MessagingException("Handler failed for " + destination));
            }
        } catch (Exception commitOrRollbackFailed) {
            listener.onError(destination, new MessagingException(
                "Settlement failed for " + destination, commitOrRollbackFailed));
        }
    }

    void stop(Duration timeout) {
        stopping = true;
        Thread t = thread;
        if (t == null) return;
        try {
            t.join(Math.max(0, timeout.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            CompletableFuture<Void> outcome = currentOutcome;
            if (outcome != null) outcome.cancel(true);
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) closeQuietly();
        }
    }

    private void closeQuietly() {
        try { if (consumer != null) consumer.close(); } catch (Exception ignored) {}
        try { if (session != null) session.close(); } catch (Exception ignored) {}
    }

    private static void sleepQuietly(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
```

- [ ] **Step 2: Wire `JmsTransport.subscribe`**

Replace the stub `subscribe` method in `JmsTransport.java`:

```java
@Override
public CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler) {
    if (closed) return CompletableFuture.failedFuture(new IllegalStateException("Transport is closed"));

    String sharedSubscriptionName = destination instanceof Topic
        ? clientId + "-" + java.util.UUID.randomUUID() : null;

    List<JmsConsumerUnit> units = new CopyOnWriteArrayList<>();
    List<CompletableFuture<Void>> ready = new java.util.ArrayList<>();
    for (int i = 0; i < concurrency; i++) {
        JmsConsumerUnit unit = new JmsConsumerUnit(connection, destination, sharedSubscriptionName, handler, listener);
        units.add(unit);
        consumerUnits.add(unit);
        ready.add(unit.start());
    }

    return CompletableFuture.allOf(ready.toArray(CompletableFuture[]::new)).handle((v, ex) -> {
        if (ex != null) {
            units.forEach(u -> u.stop(Duration.ofSeconds(5)));
            units.forEach(consumerUnits::remove);
            throw new MessagingException("Failed to subscribe to " + destination, ex);
        }
        Subscription subscription = () -> {
            units.forEach(u -> u.stop(Duration.ofSeconds(30)));
            units.forEach(consumerUnits::remove);
        };
        return subscription;
    });
}
```

Delete the now-unused `String clientId()`, `Connection connection()`, `MessagingListener listener()` accessor stubs added in Task 13 if `JmsConsumerUnit`'s constructor call above reads the fields directly instead (it does, via `clientId`, `connection`, `listener` — all already private fields on `JmsTransport`, accessible from within the same class's `subscribe` method without accessors). Remove those three one-line accessor methods now that nothing calls them.

- [ ] **Step 3: Compile the module**

Run: `gradlew.bat :messaging-jms:compileJava`
Expected: succeeds.

- [ ] **Step 4: Run the existing unit tests (no broker)**

Run: `gradlew.bat :messaging-jms:test`
Expected: PASS — `ConnectionFactoryBuilderTest` unaffected.

- [ ] **Step 5: Commit**

```bash
git add messaging-jms/src/main/java/com/messaging/jms/JmsConsumerUnit.java messaging-jms/src/main/java/com/messaging/jms/JmsTransport.java
git commit -m "feat(jms): JmsConsumerUnit owner loop (§B) and JmsTransport.subscribe wiring (§C, §D)"
```

Full broker-backed proof of this task's correctness (connect, publish, subscribe, redelivery, concurrency, shared topic subscriptions, close semantics) happens in Phase 5 once `ArtemisConformanceTest` exists — there is intentionally no mocked JMS unit test here, matching the spec's explicit call-out that only `ConnectionFactoryBuilder` is unit-tested without a broker.

---

## Phase 5 — Test fixtures and the Artemis integration test

**Prerequisite for this phase and Phase 6: Docker Desktop must be running.**

### Task 15: `BrokerAdmin`, `ArtemisAdmin`, `ArtemisBroker`, `broker.xml`, `JmsRedeliveredScenario`, `ExternalArtemisConformance`

**Files:**
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/BrokerAdmin.java`
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/ArtemisAdmin.java`
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/ArtemisBroker.java`
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/JmsRedeliveredScenario.java`
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/ExternalArtemisConformance.java`
- Create: `messaging-jms/src/testFixtures/resources/artemis/broker.xml`
- Modify: `messaging-jms/build.gradle` (add the `java-test-fixtures` plugin and its dependencies — the module currently has none)

**Interfaces:**
- Produces: `BrokerAdmin.createQueue(String)`/`createTopic(String)` returning `com.messaging.Destination`; `ArtemisBroker.admin(String adminUrl)`; `JmsRedeliveredScenario` default test method, consumed by implementers via `jmsBus()`/`jmsQueue(String)`.
- Consumes: `ConnectionFactoryBuilder`'s expected passthrough keys (Task 12) — this fixture builds the exact `messaging.jms.*` property set the adapter must accept.

- [ ] **Step 1: Add the `java-test-fixtures` plugin to `messaging-jms/build.gradle`**

The module currently only declares `api project(':messaging-core')`, `api libs.jakarta.jms.api`, and the `integrationTest` source set. Add at the top:

```groovy
plugins {
    id 'java-test-fixtures'
}
```

And add to the `dependencies` block:

```groovy
    testFixturesApi project(':messaging-core')
    testFixturesApi testFixtures(project(':messaging-conformance'))
    testFixturesRuntimeOnly libs.artemis.jakarta.client
```

(`testFixturesApi testFixtures(project(':messaging-conformance'))` gives fixture code access to `AbstractMessagingConformanceTest`, `BusSettings`, and `AbstractMessagingConnectivityTest`. `libs.artemis.jakarta.client` is `runtimeOnly` here — the fixtures reference the connection factory only by its FQCN string, never its class, matching "no provider classes, no provider constants.")

- [ ] **Step 2: `BrokerAdmin`**

```java
package com.messaging.jms.fixtures;

import com.messaging.Destination;

/** Provisions destinations outside the library API (§F) — the suite never creates one
 * itself. Every implementation gives each name a per-run unique suffix. */
public interface BrokerAdmin {
    Destination createQueue(String name);
    Destination createTopic(String name);
}
```

- [ ] **Step 3: `ArtemisAdmin`**

```java
package com.messaging.jms.fixtures;

import com.messaging.Destination;
import com.messaging.MessagingException;
import com.messaging.Queue;
import com.messaging.Topic;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provisions Artemis destinations over Jolokia (JMX-over-HTTP) using only
 * {@code java.net.http}, so no Jolokia client library is needed. Discovers the broker
 * MBean name via a Jolokia {@code search}, then calls {@code createQueue} (ANYCAST) or
 * {@code createAddress} (MULTICAST). Every destination name gets a per-run unique suffix
 * (§F) so tests never share destinations.
 *
 * <p><b>Verify against the running image (Risk, spec §Risks):</b> Artemis 2.43's Jolokia
 * endpoint may require the {@code Origin} header to match an allowed value, or reject
 * cross-origin calls outright depending on {@code jolokia-access.xml}. If calls are
 * rejected with 403, inspect the container's Jolokia access policy and adjust the
 * {@code Origin} value or the admin container's Jolokia config accordingly — do not weaken
 * a test to work around it; record a finding per the spec's Risks section instead.
 */
public final class ArtemisAdmin implements BrokerAdmin {

    private final HttpClient client = HttpClient.newHttpClient();
    private final URI jolokiaBase;
    private final String authHeader;
    private final String brokerObjectName;
    private final String runSuffix = "-" + System.nanoTime();

    public ArtemisAdmin(String adminUrl) {
        URI uri = URI.create(adminUrl);
        this.jolokiaBase = URI.create(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + "/console/jolokia");
        this.authHeader = uri.getUserInfo() != null
            ? "Basic " + Base64.getEncoder().encodeToString(uri.getUserInfo().getBytes(StandardCharsets.UTF_8))
            : null;
        this.brokerObjectName = discoverBrokerObjectName();
    }

    private String discoverBrokerObjectName() {
        String body = get("/search/org.apache.activemq.artemis:broker=*");
        Matcher m = Pattern.compile("\"(org\\.apache\\.activemq\\.artemis:broker=[^\"]+)\"").matcher(body);
        if (!m.find()) {
            throw new MessagingException("Could not discover Artemis broker MBean via Jolokia search: " + body);
        }
        return m.group(1);
    }

    @Override
    public Destination createQueue(String name) {
        String queueName = name + runSuffix;
        exec("createQueue", "[\"" + queueName + "\",\"ANYCAST\",\"" + queueName + "\",null,true,-1,false,true]");
        return Queue.of(queueName);
    }

    @Override
    public Destination createTopic(String name) {
        String addressName = name + runSuffix;
        exec("createAddress", "[\"" + addressName + "\",\"MULTICAST\"]");
        return Topic.of(addressName);
    }

    private void exec(String operation, String argsJson) {
        get("/exec/" + brokerObjectName + "/" + operation + "/" + argsJson);
    }

    private String get(String path) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(jolokiaBase.resolve(jolokiaBase.getPath() + path))
                .header("Origin", jolokiaBase.getScheme() + "://" + jolokiaBase.getHost() + ":" + jolokiaBase.getPort())
                .GET();
            if (authHeader != null) builder.header("Authorization", authHeader);
            HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new MessagingException("Jolokia call to " + path + " failed: HTTP "
                    + response.statusCode() + " " + response.body());
            }
            return response.body();
        } catch (MessagingException e) {
            throw e;
        } catch (Exception e) {
            throw new MessagingException("Jolokia call to " + path + " failed", e);
        }
    }
}
```

- [ ] **Step 4: `ArtemisBroker`**

```java
package com.messaging.jms.fixtures;

import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;

import java.util.UUID;

/** Builds the {@code MessagingConfig} plus the matching {@link BrokerAdmin} for an
 * Artemis broker at {@code host:port}. */
public final class ArtemisBroker {
    private ArtemisBroker() {}

    public static MessagingConfig config(String host, int port, String user, String password, BusSettings settings) {
        return MessagingConfig.builder()
            .url("jms://" + user + ":" + password + "@" + host + ":" + port)
            .clientId("artemis-" + UUID.randomUUID())
            .concurrency(settings.concurrency())
            .closeTimeout(settings.closeTimeout())
            .listener(settings.listener())
            .property("connection-factory", "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory")
            .property("brokerURL", "tcp://" + host + ":" + port
                + "?reconnectAttempts=-1&callFailoverTimeout=30000&callTimeout=30000")
            .build();
    }

    public static BrokerAdmin admin(String adminUrl) {
        return new ArtemisAdmin(adminUrl);
    }
}
```

- [ ] **Step 5: `broker.xml` override**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration xmlns="urn:activemq" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="urn:activemq /schema/artemis-configuration.xsd">
   <core xmlns="urn:activemq:core">
      <address-settings>
         <address-setting match="#">
            <auto-create-queues>false</auto-create-queues>
            <auto-create-addresses>false</auto-create-addresses>
            <dead-letter-address>DLQ</dead-letter-address>
            <expiry-address>ExpiryQueue</expiry-address>
         </address-setting>
      </address-settings>
   </core>
</configuration>
```

Note for Task 16: confirm the exact mount path the `apache/activemq-artemis` image expects for a config override (its documented `etc-override` directory) once the container is running — mount this file there.

- [ ] **Step 6: `JmsRedeliveredScenario`**

```java
package com.messaging.jms.fixtures;

import com.messaging.Destination;
import com.messaging.MessageBus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** JMS-only scenario (§G amendment): proves {@code messaging.redelivered} is set on
 * redelivery. Both fixture families implement this so the test isn't duplicated. */
public interface JmsRedeliveredScenario {

    MessageBus jmsBus();
    Destination jmsQueue(String name);

    @Test
    default void redeliveredHeaderSetOnRedelivery() {
        Destination queue = jmsQueue("redelivered-header");
        var seenHeaders = new CopyOnWriteArrayList<Map<String, String>>();
        var attempt = new AtomicInteger();
        jmsBus().subscribe(queue, message -> {
            seenHeaders.add(message.headers());
            if (attempt.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(new RuntimeException("force redelivery"));
            }
            return CompletableFuture.completedFuture(null);
        }).join();

        jmsBus().publish(queue, "payload".getBytes()).join();

        await().atMost(Duration.ofSeconds(30))
            .untilAsserted(() -> assertThat(seenHeaders).hasSizeGreaterThanOrEqualTo(2));
        assertThat(seenHeaders.get(0)).doesNotContainKey("messaging.redelivered");
        assertThat(seenHeaders.get(1)).containsEntry("messaging.redelivered", "true");
    }
}
```

- [ ] **Step 7: `ExternalArtemisConformance`**

```java
package com.messaging.jms.fixtures;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.conformance.BusSettings;

import java.net.URI;
import java.util.UUID;

/** Runs {@link AbstractMessagingConformanceTest} against an external Artemis broker named
 * by system properties — used by the sample app's {@code jms-artemis} target. */
public class ExternalArtemisConformance extends AbstractMessagingConformanceTest implements JmsRedeliveredScenario {

    private BrokerAdmin admin;
    private MessageBus lastBus;

    @Override
    protected MessageBus createBus(BusSettings settings) {
        String url = System.getProperty("messaging.sample.url");
        String adminUrl = System.getProperty("messaging.sample.admin-url");
        if (admin == null) admin = ArtemisBroker.admin(adminUrl);

        URI uri = URI.create(url);
        String[] userInfo = uri.getUserInfo() != null
            ? uri.getUserInfo().split(":", 2) : new String[]{"artemis", "artemis"};
        var config = ArtemisBroker.config(uri.getHost(), uri.getPort(),
            userInfo[0], userInfo.length > 1 ? userInfo[1] : "", settings);
        lastBus = Messaging.connect(config);
        return lastBus;
    }

    @Override protected Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override public MessageBus jmsBus() { return lastBus; }
    @Override public Destination jmsQueue(String name) { return provisionQueue(name); }
}
```

- [ ] **Step 8: Compile the new source set**

Run: `gradlew.bat :messaging-jms:compileTestFixturesJava`
Expected: succeeds. No broker call happens at compile time.

- [ ] **Step 9: Commit**

```bash
git add messaging-jms/build.gradle messaging-jms/src/testFixtures
git commit -m "feat(jms): Artemis test fixtures (BrokerAdmin, ArtemisAdmin, ArtemisBroker, broker.xml, JmsRedeliveredScenario, ExternalArtemisConformance)"
```

---

### Task 16: `ArtemisConformanceTest` — Testcontainers + Toxiproxy integration test

**Files:**
- Create: `messaging-jms/src/integrationTest/java/com/messaging/jms/ArtemisConformanceTest.java`

**Interfaces:**
- Consumes: `AbstractMessagingConnectivityTest` (Task 11), `ArtemisAdmin` (Task 15), `JmsRedeliveredScenario` (Task 15), `Messaging.connect` (existing).

`messaging-jms/build.gradle` already declares the `integrationTest` source set with `testFixtures(project(':messaging-conformance'))`, the Testcontainers BOM, `testcontainers-junit-jupiter`, `testcontainers-toxiproxy`, and `runtimeOnly` Artemis/IBM MQ clients (confirmed present — no build file change needed for this task). Add one dependency it's missing: the module's own test fixtures.

- [ ] **Step 1: Add the missing dependency**

In `messaging-jms/build.gradle`, inside the `dependencies` block, add:

```groovy
    integrationTestImplementation testFixtures(project(':messaging-jms'))
```

- [ ] **Step 2: Write `ArtemisConformanceTest`**

```java
package com.messaging.jms;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConnectivityTest;
import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;
import com.messaging.jms.fixtures.ArtemisAdmin;
import com.messaging.jms.fixtures.BrokerAdmin;
import com.messaging.jms.fixtures.JmsRedeliveredScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

/**
 * Proves the JMS adapter against a real Artemis broker (§B, §C, §D, §E, §F, §G, §J, §K).
 * All host access goes through named Toxiproxy proxies so container ports stay stable
 * across {@link #restartBroker()}: {@code bus} (the only one {@link #cutNetwork()} cuts),
 * {@code oob} (never cut — the out-of-band bus uses it), and {@code admin} (HTTP).
 */
@Testcontainers
class ArtemisConformanceTest extends AbstractMessagingConnectivityTest implements JmsRedeliveredScenario {

    private static final Network NETWORK = Network.newNetwork();

    private static final GenericContainer<?> ARTEMIS = new GenericContainer<>(
            DockerImageName.parse("apache/activemq-artemis:2.43.0"))
        .withNetwork(NETWORK)
        .withNetworkAliases("artemis")
        .withExposedPorts(61616, 8161)
        .withEnv("ARTEMIS_USER", "artemis")
        .withEnv("ARTEMIS_PASSWORD", "artemis")
        .withClasspathResourceMapping("artemis/broker.xml",
            "/var/lib/artemis-instance/etc-override/broker.xml", BindMode.READ_ONLY)
        .waitingFor(Wait.forListeningPort());

    private static final ToxiproxyContainer TOXIPROXY = new ToxiproxyContainer(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0"))
        .withNetwork(NETWORK);

    private static ToxiproxyContainer.ContainerProxy busProxy;
    private static ToxiproxyContainer.ContainerProxy oobProxy;
    private static ToxiproxyContainer.ContainerProxy adminProxy;
    private static BrokerAdmin admin;
    private static MessageBus outOfBandBus;

    @BeforeAll
    static void startContainers() {
        ARTEMIS.start();
        TOXIPROXY.start();
        busProxy = TOXIPROXY.getProxy(ARTEMIS, 61616);
        oobProxy = TOXIPROXY.getProxy(ARTEMIS, 61616);
        adminProxy = TOXIPROXY.getProxy(ARTEMIS, 8161);

        admin = new ArtemisAdmin("http://artemis:artemis@"
            + adminProxy.getContainerIpAddress() + ":" + adminProxy.getProxyPort());
        outOfBandBus = Messaging.connect(brokerConfig(oobProxy, new BusSettings(
            MessagingListener.noOp(), java.time.Duration.ofSeconds(30), 1), "artemis-oob"));
    }

    @AfterAll
    static void stopContainers() {
        if (outOfBandBus != null) outOfBandBus.close();
        TOXIPROXY.stop();
        ARTEMIS.stop();
    }

    private static MessagingConfig brokerConfig(ToxiproxyContainer.ContainerProxy proxy, BusSettings settings, String clientIdPrefix) {
        String host = proxy.getContainerIpAddress();
        int port = proxy.getProxyPort();
        return MessagingConfig.builder()
            .url("jms://artemis:artemis@" + host + ":" + port)
            .clientId(clientIdPrefix + "-" + UUID.randomUUID())
            .concurrency(settings.concurrency())
            .closeTimeout(settings.closeTimeout())
            .listener(settings.listener())
            .property("connection-factory", "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory")
            .property("brokerURL", "tcp://" + host + ":" + port
                + "?reconnectAttempts=-1&callFailoverTimeout=30000&callTimeout=30000")
            .build();
    }

    @Override
    protected MessageBus createBus(BusSettings settings) {
        return Messaging.connect(brokerConfig(busProxy, settings, "artemis-test"));
    }

    @Override protected Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override
    protected void publishOutOfBand(Destination destination, byte[] body) {
        outOfBandBus.publish(destination, body).join();
    }

    @Override
    protected void restartBroker() {
        ARTEMIS.getDockerClient().stopContainerCmd(ARTEMIS.getContainerId()).exec();
        ARTEMIS.getDockerClient().startContainerCmd(ARTEMIS.getContainerId()).exec();
        Wait.forListeningPort().waitUntilReady(ARTEMIS);
    }

    @Override protected void cutNetwork() { setCut(busProxy, true); }
    @Override protected void restoreNetwork() { setCut(busProxy, false); }

    private void setCut(ToxiproxyContainer.ContainerProxy proxy, boolean cut) {
        try {
            if (cut) proxy.toxics().bandwidth("cut", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, 0);
            else proxy.toxics().get("cut").remove();
        } catch (Exception e) {
            throw new RuntimeException("Failed to toggle network on proxy", e);
        }
    }

    @Override public MessageBus jmsBus() { return bus; }
    @Override public Destination jmsQueue(String name) { return provisionQueue(name); }
}
```

**Executor note — verify against the installed library versions before trusting this file to compile as-is:** the exact `ToxiproxyContainer`/`eu.rekawek.toxiproxy` API (in particular `ContainerProxy.toxics()`, `ToxicDirection`, and whether a "cut" is best expressed as a zero-bandwidth toxic vs. `Proxy.disable()`) can differ across Testcontainers 1.21.4's toxiproxy module. Before writing this file for real: run `gradlew.bat :messaging-jms:dependencies --configuration integrationTestRuntimeClasspath` to see the resolved `toxiproxy-java` version, check its javadoc/source on the classpath (or the Testcontainers docs for that version) for the exact cut/restore call, and adjust `setCut` accordingly — the rest of the class (container wiring, proxy roles, config building) is not expected to need changes.

- [ ] **Step 3: Run the Artemis integration test**

Run: `gradlew.bat :messaging-jms:integrationTest --tests "com.messaging.jms.ArtemisConformanceTest"`
Expected: PASS, zero skips, for every inherited scenario from `AbstractMessagingConformanceTest` and `AbstractMessagingConnectivityTest`, plus `redeliveredHeaderSetOnRedelivery`.

If a scenario fails, treat it as real signal per the #1 E2E rule — do not weaken the scenario. Common early failures and their likely fix:
- Shared non-durable topic subscription errors → confirm `createSharedConsumer` is supported on Artemis 2.43 with `auto-create-queues=false` (spec Risk); if genuinely unsupported, record a finding, do not loosen the test.
- Jolokia 403/404 → see the `ArtemisAdmin` class javadoc note (Task 15, Step 3).
- `queueMessagesPublishedDuringOutageDeliveredAfterRecovery` or `publishDuringOutageFails` timing out → confirm the `busProxy` toxic actually blocks traffic (add a temporary log of `proxy.toxics().getAll()` while debugging), not a JMS reconnect setting.

- [ ] **Step 4: Break-check — prove redelivery scenarios can fail against a real broker**

Temporarily change `JmsConsumerUnit.settle` to call `session.commit()` unconditionally (even on failure). Rerun `gradlew.bat :messaging-jms:integrationTest --tests "com.messaging.jms.ArtemisConformanceTest.redeliveryOnExceptionalFuture"`; confirm it FAILS. Revert.

- [ ] **Step 5: Break-check — prove the stuck-handler scenario can fail**

Temporarily change `JmsConsumerUnit.stop` to skip cancelling `currentOutcome`. Rerun `gradlew.bat :messaging-jms:integrationTest --tests "com.messaging.jms.ArtemisConformanceTest.busCloseCancelsStuckHandler"`; confirm it FAILS (times out instead of returning near the deadline). Revert.

- [ ] **Step 6: Break-check — prove `topicConcurrencyDeliversOncePerSubscription` can fail**

Temporarily change `JmsTransport.subscribe` to pass a distinct `sharedSubscriptionName` per unit instead of one shared name per `subscribe` call (i.e., `UUID.randomUUID()` generated inside the loop instead of once outside it). Rerun `gradlew.bat :messaging-jms:integrationTest --tests "com.messaging.jms.ArtemisConformanceTest.topicConcurrencyDeliversOncePerSubscription"`; confirm it FAILS (each unit gets its own copy — 8 deliveries, not 4). Revert.

- [ ] **Step 7: Run the full integration test class one more time clean**

Run: `gradlew.bat :messaging-jms:integrationTest --tests "com.messaging.jms.ArtemisConformanceTest"`
Expected: PASS, zero skips.

- [ ] **Step 8: Commit**

```bash
git add messaging-jms/build.gradle messaging-jms/src/integrationTest/java/com/messaging/jms/ArtemisConformanceTest.java
git commit -m "test(jms): Artemis integration test via Testcontainers + Toxiproxy, full suite green"
```

---

## Phase 6 — IBM MQ fixtures, proven against the **unchanged** adapter

**Rule for this phase:** if a scenario fails against IBM MQ and the fix would require touching `messaging-jms/src/main`, do not make that change silently. Stop, write the failure and the suspected cause into the "Risks" section of `docs/superpowers/specs/2026-09-13-jms-adapter-design.md` as a recorded finding, and raise it before proceeding — per the spec, the adapter must pass both providers with zero code differences between them.

### Task 17: `IbmMqAdmin`, `IbmMqBroker`, `ExternalIbmMqConformance`

**Files:**
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/IbmMqAdmin.java`
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/IbmMqBroker.java`
- Create: `messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/ExternalIbmMqConformance.java`
- Modify: `messaging-jms/build.gradle` (add `testFixturesRuntimeOnly libs.ibm.mq.jakarta.client`)

- [ ] **Step 1: Add the IBM MQ client to test fixtures**

In `messaging-jms/build.gradle`'s `dependencies` block, add:

```groovy
    testFixturesRuntimeOnly libs.ibm.mq.jakarta.client
```

- [ ] **Step 2: `IbmMqAdmin`**

```java
package com.messaging.jms.fixtures;

import com.messaging.Destination;
import com.messaging.MessagingException;
import com.messaging.Queue;
import com.messaging.Topic;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Base64;

/**
 * Provisions IBM MQ destinations over the MQ REST {@code mqsc} endpoint. Queue names are
 * sanitised (uppercase, {@code -} to {@code _}, capped at 48 characters) so they fit the
 * dev image's {@code DEV.**} authority. Topics need no admin call — they map to the topic
 * string {@code dev/<name>}, the dev image's own default tree. Uses a trust-all HTTP
 * client because the dev image ships a self-signed certificate; used only here and by the
 * sample app, never in `src/main`.
 */
public final class IbmMqAdmin implements BrokerAdmin {

    private final HttpClient client;
    private final URI restBase;
    private final String authHeader;
    private final String runSuffix;

    public IbmMqAdmin(String adminUrl) {
        URI uri = URI.create(adminUrl);
        this.restBase = URI.create(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort());
        this.authHeader = uri.getUserInfo() != null
            ? "Basic " + Base64.getEncoder().encodeToString(uri.getUserInfo().getBytes(StandardCharsets.UTF_8))
            : null;
        this.client = trustAllClient();
        this.runSuffix = "_" + System.nanoTime();
    }

    private static HttpClient trustAllClient() {
        try {
            TrustManager[] trustAll = { new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] c, String a) {}
                public void checkServerTrusted(X509Certificate[] c, String a) {}
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }};
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new SecureRandom());
            return HttpClient.newBuilder().sslContext(ctx).build();
        } catch (Exception e) {
            throw new MessagingException("Failed to build trust-all HTTP client for IBM MQ admin", e);
        }
    }

    private static String sanitise(String name) {
        String upper = name.toUpperCase().replace('-', '_');
        return upper.length() > 48 ? upper.substring(0, 48) : upper;
    }

    @Override
    public Destination createQueue(String name) {
        String queueName = sanitise("DEV." + name + runSuffix);
        runMqsc("DEFINE QLOCAL('" + queueName + "') REPLACE");
        return Queue.of(queueName);
    }

    @Override
    public Destination createTopic(String name) {
        return Topic.of("dev/" + name + runSuffix);
    }

    private void runMqsc(String command) {
        try {
            String body = "{\"type\":\"runCommand\",\"parameters\":{\"command\":\"" + escape(command) + "\"}}";
            HttpRequest request = HttpRequest.newBuilder(restBase.resolve("/api/v2/admin/action/qmgr/QM1/mqsc"))
                .header("Content-Type", "application/json")
                .header("ibm-mq-rest-csrf-token", "value")
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new MessagingException("IBM MQ mqsc call failed: HTTP "
                    + response.statusCode() + " " + response.body());
            }
        } catch (MessagingException e) {
            throw e;
        } catch (Exception e) {
            throw new MessagingException("IBM MQ mqsc call failed for command: " + command, e);
        }
    }

    private static String escape(String s) { return s.replace("\"", "\\\""); }
}
```

**Verify against the running dev image before trusting the exact REST path/body:** IBM MQ's REST admin API path and required headers (notably CSRF token handling) vary slightly by MQ version; confirm `/api/v2/admin/action/qmgr/QM1/mqsc` and the `ibm-mq-rest-csrf-token` header against the pinned `icr.io/ibm-messaging/mq` image version's own REST API documentation once Task 18 stands the container up, and adjust `runMqsc` if the dev image reports 400/401.

- [ ] **Step 3: `IbmMqBroker`**

```java
package com.messaging.jms.fixtures;

import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;

import java.util.UUID;

/** Builds the {@code MessagingConfig} plus the matching {@link BrokerAdmin} for an IBM MQ
 * broker at {@code host:port}, queue manager {@code QM1}, channel {@code DEV.APP.SVRCONN}. */
public final class IbmMqBroker {
    private IbmMqBroker() {}

    public static MessagingConfig config(String host, int port, String user, String password, BusSettings settings) {
        return MessagingConfig.builder()
            .url("jms://" + user + ":" + password + "@" + host + ":" + port)
            .clientId("ibmmq-" + UUID.randomUUID())
            .concurrency(settings.concurrency())
            .closeTimeout(settings.closeTimeout())
            .listener(settings.listener())
            .property("connection-factory", "com.ibm.mq.jakarta.jms.MQConnectionFactory")
            .property("hostName", host)
            .property("port", String.valueOf(port))
            .property("channel", "DEV.APP.SVRCONN")
            .property("queueManager", "QM1")
            .property("transportType", "1")
            .property("clientReconnectOptions", "16777216")
            .property("clientReconnectTimeout", "30")
            .build();
    }

    public static BrokerAdmin admin(String adminUrl) {
        return new IbmMqAdmin(adminUrl);
    }
}
```

- [ ] **Step 4: `ExternalIbmMqConformance`**

```java
package com.messaging.jms.fixtures;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConformanceTest;
import com.messaging.conformance.BusSettings;

import java.net.URI;

public class ExternalIbmMqConformance extends AbstractMessagingConformanceTest implements JmsRedeliveredScenario {

    private BrokerAdmin admin;
    private MessageBus lastBus;

    @Override
    protected MessageBus createBus(BusSettings settings) {
        String url = System.getProperty("messaging.sample.url");
        String adminUrl = System.getProperty("messaging.sample.admin-url");
        if (admin == null) admin = IbmMqBroker.admin(adminUrl);

        URI uri = URI.create(url);
        String[] userInfo = uri.getUserInfo() != null
            ? uri.getUserInfo().split(":", 2) : new String[]{"app", "passw0rd"};
        var config = IbmMqBroker.config(uri.getHost(), uri.getPort(),
            userInfo[0], userInfo.length > 1 ? userInfo[1] : "", settings);
        lastBus = Messaging.connect(config);
        return lastBus;
    }

    @Override protected Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override public MessageBus jmsBus() { return lastBus; }
    @Override public Destination jmsQueue(String name) { return provisionQueue(name); }
}
```

- [ ] **Step 5: Compile**

Run: `gradlew.bat :messaging-jms:compileTestFixturesJava`
Expected: succeeds.

- [ ] **Step 6: Commit**

```bash
git add messaging-jms/build.gradle messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/IbmMqAdmin.java messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/IbmMqBroker.java messaging-jms/src/testFixtures/java/com/messaging/jms/fixtures/ExternalIbmMqConformance.java
git commit -m "feat(jms): IBM MQ test fixtures (IbmMqAdmin, IbmMqBroker, ExternalIbmMqConformance)"
```

---

### Task 18: `IbmMqConformanceTest` — Testcontainers integration test against the unchanged adapter

**Files:**
- Create: `messaging-jms/src/integrationTest/java/com/messaging/jms/IbmMqConformanceTest.java`

- [ ] **Step 1: Write `IbmMqConformanceTest`**

```java
package com.messaging.jms;

import com.messaging.*;
import com.messaging.conformance.AbstractMessagingConnectivityTest;
import com.messaging.conformance.BusSettings;
import com.messaging.config.MessagingConfig;
import com.messaging.jms.fixtures.BrokerAdmin;
import com.messaging.jms.fixtures.IbmMqAdmin;
import com.messaging.jms.fixtures.JmsRedeliveredScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;

/**
 * Proves the JMS adapter — with **zero code changes from the Artemis run** — against a
 * real IBM MQ broker. Same three-proxy topology as {@link ArtemisConformanceTest}: {@code
 * bus} (cut by {@link #cutNetwork()}), {@code oob} (never cut), {@code admin} (HTTPS REST).
 */
@Testcontainers
class IbmMqConformanceTest extends AbstractMessagingConnectivityTest implements JmsRedeliveredScenario {

    private static final Network NETWORK = Network.newNetwork();

    private static final GenericContainer<?> IBM_MQ = new GenericContainer<>(
            DockerImageName.parse("icr.io/ibm-messaging/mq:9.4.5.1-r1"))
        .withNetwork(NETWORK)
        .withNetworkAliases("ibmmq")
        .withExposedPorts(1414, 9443)
        .withEnv("LICENSE", "accept")
        .withEnv("MQ_QMGR_NAME", "QM1")
        .withEnv("MQ_APP_PASSWORD", "passw0rd")
        .waitingFor(Wait.forLogMessage(".*QMNAME\\(QM1\\).*STATUS\\(Running\\).*\\n", 1));

    private static final ToxiproxyContainer TOXIPROXY = new ToxiproxyContainer(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0"))
        .withNetwork(NETWORK);

    private static ToxiproxyContainer.ContainerProxy busProxy;
    private static ToxiproxyContainer.ContainerProxy oobProxy;
    private static ToxiproxyContainer.ContainerProxy adminProxy;
    private static BrokerAdmin admin;
    private static MessageBus outOfBandBus;

    @BeforeAll
    static void startContainers() {
        IBM_MQ.start();
        TOXIPROXY.start();
        busProxy = TOXIPROXY.getProxy(IBM_MQ, 1414);
        oobProxy = TOXIPROXY.getProxy(IBM_MQ, 1414);
        adminProxy = TOXIPROXY.getProxy(IBM_MQ, 9443);

        admin = new IbmMqAdmin("https://app:passw0rd@"
            + adminProxy.getContainerIpAddress() + ":" + adminProxy.getProxyPort());
        outOfBandBus = Messaging.connect(brokerConfig(oobProxy,
            new BusSettings(MessagingListener.noOp(), Duration.ofSeconds(30), 1), "ibmmq-oob"));
    }

    @AfterAll
    static void stopContainers() {
        if (outOfBandBus != null) outOfBandBus.close();
        TOXIPROXY.stop();
        IBM_MQ.stop();
    }

    private static MessagingConfig brokerConfig(ToxiproxyContainer.ContainerProxy proxy, BusSettings settings, String clientIdPrefix) {
        return MessagingConfig.builder()
            .url("jms://app:passw0rd@" + proxy.getContainerIpAddress() + ":" + proxy.getProxyPort())
            .clientId(clientIdPrefix + "-" + UUID.randomUUID())
            .concurrency(settings.concurrency())
            .closeTimeout(settings.closeTimeout())
            .listener(settings.listener())
            .property("connection-factory", "com.ibm.mq.jakarta.jms.MQConnectionFactory")
            .property("hostName", proxy.getContainerIpAddress())
            .property("port", String.valueOf(proxy.getProxyPort()))
            .property("channel", "DEV.APP.SVRCONN")
            .property("queueManager", "QM1")
            .property("transportType", "1")
            .property("clientReconnectOptions", "16777216")
            .property("clientReconnectTimeout", "30")
            .build();
    }

    @Override
    protected MessageBus createBus(BusSettings settings) {
        return Messaging.connect(brokerConfig(busProxy, settings, "ibmmq-test"));
    }

    @Override protected Destination provisionTopic(String name) { return admin.createTopic(name); }
    @Override protected Destination provisionQueue(String name) { return admin.createQueue(name); }

    @Override
    protected void publishOutOfBand(Destination destination, byte[] body) {
        outOfBandBus.publish(destination, body).join();
    }

    @Override
    protected void restartBroker() {
        IBM_MQ.getDockerClient().stopContainerCmd(IBM_MQ.getContainerId()).exec();
        IBM_MQ.getDockerClient().startContainerCmd(IBM_MQ.getContainerId()).exec();
        Wait.forLogMessage(".*QMNAME\\(QM1\\).*STATUS\\(Running\\).*\\n", 1).waitUntilReady(IBM_MQ);
    }

    @Override protected void cutNetwork() { setCut(busProxy, true); }
    @Override protected void restoreNetwork() { setCut(busProxy, false); }

    private void setCut(ToxiproxyContainer.ContainerProxy proxy, boolean cut) {
        try {
            if (cut) proxy.toxics().bandwidth("cut", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, 0);
            else proxy.toxics().get("cut").remove();
        } catch (Exception e) {
            throw new RuntimeException("Failed to toggle network on proxy", e);
        }
    }

    @Override public MessageBus jmsBus() { return bus; }
    @Override public Destination jmsQueue(String name) { return provisionQueue(name); }
}
```

- [ ] **Step 2: Run the IBM MQ integration test**

Run: `gradlew.bat :messaging-jms:integrationTest --tests "com.messaging.jms.IbmMqConformanceTest"`
Expected: PASS, zero skips, using the exact same `messaging-jms/src/main` code that already passed Artemis (Task 16) — **no adapter code changes for this task.**

If any scenario fails:
1. Confirm it's not a fixture bug (wrong REST path, wrong image tag, wrong wait strategy) before suspecting the adapter.
2. If it genuinely requires an adapter change, **stop** — do not make the change. Add a row to the spec's Risks section (`docs/superpowers/specs/2026-08-18-messaging-lib-design.md` has no such section; add one to `2026-09-13-jms-adapter-design.md`'s existing "Risks" section instead) describing the failure, the suspected cause, and that it was deliberately left unfixed per this plan's Phase 6 rule. Then bring it to the user before continuing to Phase 7.
3. Specific risks to check first, per the spec: whether `createSharedConsumer` works on the dev image (topic concurrency scenarios), and whether `clientReconnectTimeout=30` actually bounds `publishDuringOutageFails` under `OUTAGE_PUBLISH_BOUND` (60s) — IBM MQ's default `MQReconnectTimeout` is 1800s if the client-side override isn't taking effect, which would make that scenario legitimately time out.

- [ ] **Step 3: Commit only if Step 2 passed with no adapter changes**

```bash
git add messaging-jms/src/integrationTest/java/com/messaging/jms/IbmMqConformanceTest.java
git commit -m "test(jms): IBM MQ integration test — adapter proven unchanged against both providers"
```

---

## Phase 7 — Sample app: broker targets

### Task 19: `jms-artemis`/`jms-ibm-mq` targets, `--url`/`--admin-url` options

**Files:**
- Modify: `messaging-sample-app/build.gradle`
- Modify: `messaging-sample-app/src/main/java/com/messaging/sample/SampleApp.java`
- Modify: `messaging-sample-app/src/test/java/com/messaging/sample/SampleAppTest.java`

**Interfaces:**
- Consumes: `com.messaging.jms.fixtures.ExternalArtemisConformance`/`ExternalIbmMqConformance` (Tasks 15, 17).
- Produces: `SampleApp.Target(Class<?>, String, boolean)` — the two-arg constructor from before is kept, defaulting `broker` to `false`, so every existing target and test is unaffected.

- [ ] **Step 1: Add dependencies**

In `messaging-sample-app/build.gradle`'s `dependencies` block, add:

```groovy
    implementation testFixtures(project(':messaging-jms'))
    runtimeOnly libs.artemis.jakarta.client
    runtimeOnly libs.ibm.mq.jakarta.client
```

- [ ] **Step 2: Write the failing tests first**

Add to `SampleAppTest.java`, and replace the existing `unknownArgumentIsRejected`:

```java
@Test
void unknownArgumentIsRejected() {
    int exit = run("--bogus-flag");

    assertThat(exit).isEqualTo(1);
    assertThat(err.toString()).contains("Unknown argument: --bogus-flag");
}

@Test
void brokerTargetWithoutUrlExitsOne() {
    int exit = run("--transport", "jms-artemis");

    assertThat(exit).isEqualTo(1);
    assertThat(err.toString()).contains("--url").contains("--admin-url");
}

@Test
void brokerTargetWithoutAdminUrlExitsOne() {
    int exit = run("--transport", "jms-artemis", "--url", "jms://user:pass@localhost:61616");

    assertThat(exit).isEqualTo(1);
}

@Test
void urlWithNonBrokerTargetIsAccepted() {
    int exit = run("--transport", "in-memory", "--url", "jms://user:pass@localhost:61616");

    assertThat(exit).isEqualTo(0);
}
```

- [ ] **Step 3: Run to verify the new tests fail and the changed one fails**

Run: `gradlew.bat :messaging-sample-app:test`
Expected: `unknownArgumentIsRejected` FAILS (the old `--url` flag is currently rejected as unknown, matching the old assertion text, but the new assertion text `--bogus-flag` won't appear); the three new tests FAIL to compile/run since `jms-artemis` isn't a known transport yet and `--url`/`--admin-url` aren't recognized flags.

- [ ] **Step 4: Rewrite `SampleApp.java`**

```java
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

        if (!all && TARGETS.get(transport).broker() && (url == null || adminUrl == null)) {
            return usage(err, "Broker target '" + transport + "' requires --url and --admin-url");
        }
        if (!all && TARGETS.get(transport).broker()) {
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `gradlew.bat :messaging-sample-app:test`
Expected: PASS — all of `SampleAppTest`, including `inMemoryPassesEveryScenarioInTheSuite` (unaffected, still counts `AbstractMessagingConformanceTest`'s `@Test` methods — which now also include the Phase 2 additions, so this test's expected count has moved and it self-adjusts via reflection rather than a hard-coded number).

- [ ] **Step 6: Commit**

```bash
git add messaging-sample-app/build.gradle messaging-sample-app/src/main/java/com/messaging/sample/SampleApp.java messaging-sample-app/src/test/java/com/messaging/sample/SampleAppTest.java
git commit -m "feat(sample-app): jms-artemis/jms-ibm-mq broker targets with required --url/--admin-url"
```

---

## Phase 8 — Docs, `/simplify`, full verification

### Task 20: Update all affected documentation

**Files:**
- Modify: `docs/superpowers/specs/2026-08-18-messaging-lib-design.md` (§C, §F, §H, §I amendments)
- Modify: `docs/superpowers/specs/2026-09-13-sample-app-design.md` (adapter slot decided)
- Modify: `messaging-sample-app/README.md` (targets, options, docker run commands)
- Modify: `PLAN.md` (verification lines)
- Modify: `docs/superpowers/plans/2026-08-21-messaging-lib.md` (mark Tasks 5-6 superseded)
- Modify (if structurally stale): `architecture.json`, `architecture.html`

- [ ] **Step 1: Amend §C in `2026-08-18-messaging-lib-design.md`**

Find the `## C. Concurrency and ordering` section's table (`| Adapter | A consumer unit is | Cap |`) and add a line directly below it:

```markdown
**JMS topics (added by the JMS adapter spec):** a topic subscription is one shared
non-durable subscription (`createSharedConsumer`); its `concurrency` consumer units
compete inside that one subscription, so the subscription sees each message once
regardless of concurrency, while separate `subscribe` calls on the same topic still each
get their own copy.
```

- [ ] **Step 2: Amend §F**

At the end of the `## F. Provisioning and destination naming` section, add:

```markdown
**What "missing" means per JMS provider (JMS adapter spec):**

| Provider | Missing queue | Missing/unauthorised topic |
|---|---|---|
| Artemis (auto-create disabled) | Rejected | Rejected |
| IBM MQ | Rejected — reason code 2085, or 2035 outside `DEV.**` | Rejected — reason code 2035 if the topic string is outside the app's authorised tree |

Either way, the publish or subscribe future fails with a `MessagingException` naming the
destination, with the provider's own exception as cause.
```

- [ ] **Step 3: Amend §H**

At the end of the `## H. Listener contract` section, add:

```markdown
**JMS reconnection (JMS adapter spec):** the Jakarta Messaging API exposes no reconnect
event, so the JMS adapter never reports `RECONNECTED`. `DISCONNECTED` fires only when the
client's own reconnect logic (Artemis `reconnectAttempts`, IBM MQ `WMQ_CLIENT_RECONNECT`)
gives up and calls the registered `ExceptionListener`.
```

- [ ] **Step 4: Amend §I**

In the `## I. Provider discovery and configuration` section's key table, add a row and a following paragraph:

```markdown
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
```

- [ ] **Step 5: Update `2026-09-13-sample-app-design.md`'s "Adapter slot" section**

Replace the `## Adapter slot (sub-projects 2+)` section's body with:

```markdown
**Decided by the JMS adapter spec (`2026-09-13-jms-adapter-design.md`):** `SampleApp.Target`
gained a `broker` flag. Broker targets (`jms-artemis`, `jms-ibm-mq`) require both `--url
<jms://user:pass@host:port>` and `--admin-url <http(s)://user:pass@host:port>` — missing
either exits 1 — and are excluded from `--transport all`, so the no-argument run stays
Docker-free. Each broker target's suite class (`ExternalArtemisConformance`,
`ExternalIbmMqConformance`, both in `messaging-jms`'s test fixtures) reads those two values
back from the `messaging.sample.url` / `messaging.sample.admin-url` system properties that
`SampleApp` sets before running the suite, and provisions destinations through the
matching `BrokerAdmin`. Kafka's target will follow the same shape when its spec lands.
```

- [ ] **Step 6: Update `messaging-sample-app/README.md`**

Add a new `## Broker targets` section after the existing `## Targets` table:

```markdown
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
```

Also update the top-of-file targets table to add the two broker rows, and update the "Adding a broker adapter" section at the bottom to say it's now done for JMS (point at Kafka as the remaining item).

- [ ] **Step 7: Update `PLAN.md`'s verification section and module description**

Update the `messaging-jms/` line in the module layout table (currently reads `jakarta.jms-api only.  Fixtures: Artemis AND IBM MQ`) to note it's now implemented: `jakarta.jms-api only. Fixtures: Artemis AND IBM MQ — implemented, see 2026-09-13-jms-adapter-design.md`.

- [ ] **Step 8: Mark Tasks 5-6 superseded in the 2026-08-21 plan**

At the top of `docs/superpowers/plans/2026-08-21-messaging-lib.md`, add:

```markdown
> **Tasks 5-6 (JMS adapter) are superseded** by `docs/superpowers/specs/2026-09-13-jms-adapter-design.md`
> and `docs/superpowers/plans/2026-09-14-jms-adapter-plan.md`, which implement the JMS
> adapter without the provider classes/constants this file's sketch used. Do not follow
> Tasks 5-6 below.
```

- [ ] **Step 9: Check and update `architecture.json`/`architecture.html` if structurally stale**

Open `architecture.json`; if it lists `messaging-jms` as an empty/planned module or omits the `messaging-jms` test fixtures and `integrationTest` source set, update its module/dependency entries to match the new structure (new classes in `com.messaging.jms`, the `testFixtures` and `integrationTest` source sets, the new `messaging-sample-app` broker targets). Regenerate or hand-edit `architecture.html` consistently with whatever process created it originally (check for a generation script before hand-editing; if none exists, hand-edit both files to match).

- [ ] **Step 10: Commit**

```bash
git add docs/superpowers/specs/2026-08-18-messaging-lib-design.md docs/superpowers/specs/2026-09-13-sample-app-design.md messaging-sample-app/README.md PLAN.md docs/superpowers/plans/2026-08-21-messaging-lib.md architecture.json architecture.html
git commit -m "docs: JMS adapter contract amendments (§C, §F, §H, §I), sample-app README, PLAN.md, supersede notice"
```

---

### Task 21: Run `/simplify`

**Files:** whatever `/simplify` touches — review its diff before accepting.

- [ ] **Step 1: Run the slash command**

Run the `/simplify` command (per `CLAUDE.md`'s top rule: "Always run slash command `/simplify` at the end of every extensive refactor or new feature phase, before any commit"). This reviews all code changed in this plan for reuse, simplification, efficiency, and consistency, then applies fixes directly.

- [ ] **Step 2: Review the diff it produced**

Run: `git diff`
Confirm every change is a genuine simplification (no behavior change) — pay particular attention to `JmsConsumerUnit`/`InMemoryTransport.ConsumerUnit`, which are structurally similar and may be flagged for further extraction; only extract shared logic between them if it doesn't blur the owner-loop/§B distinction that keeps JMS and the in-memory control transport independently understandable.

- [ ] **Step 3: Re-run the full unit test suite (not the integration test — that's Task 22)**

Run: `gradlew.bat test`
Expected: PASS. If `/simplify` broke anything, fix it before committing.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: apply /simplify pass across the JMS adapter, conformance suite, and sample app"
```

(If `/simplify` made no changes, skip this task's commit — there is nothing to commit.)

---

### Task 22: Full verification and break-check sweep

**Files:** none new — this task only runs commands and confirms output, per `superpowers:verification-before-completion`.

**Prerequisite: Docker Desktop running.**

- [ ] **Step 1: Full unit test run**

Run: `gradlew.bat test`
Expected: PASS — every module, including `messaging-core`, `ConnectionFactoryBuilderTest`, the in-memory control run with every Phase 2 scenario, `MetaConformanceTest`, and `messaging-sample-app`'s tests.

- [ ] **Step 2: Full integration test run**

Run: `gradlew.bat :messaging-jms:integrationTest`
Expected: PASS — every scenario on both Artemis and IBM MQ, zero skips (the build's skip-is-failure listener enforces this mechanically).

- [ ] **Step 3: Re-run the four spec break-checks end to end, each reverted immediately after**

These were already exercised per-task (Tasks 6, 9, 10, 16) — this step re-confirms all four still hold together after `/simplify`:

1. Remove `rollback()` from `JmsConsumerUnit.settle`'s failure branch → `gradlew.bat :messaging-jms:integrationTest --tests "*.ArtemisConformanceTest.redeliveryOnExceptionalFuture"` FAILs. Revert.
2. Skip cancelling the handler future in `JmsConsumerUnit.stop` → the stuck-handler integration scenarios FAIL. Revert.
3. Make `SafeListener.wrap` return the raw listener unchanged → `gradlew.bat :messaging-conformance:test --tests "*.InMemoryConformanceTest.listenerThrowDoesNotChangeDelivery"` FAILs. Revert.
4. Use a fresh `UUID` per unit instead of one shared subscription name in `JmsTransport.subscribe` → `gradlew.bat :messaging-jms:integrationTest --tests "*.ArtemisConformanceTest.topicConcurrencyDeliversOncePerSubscription"` FAILs. Revert.

After each revert, confirm the corresponding test passes again before moving to the next.

- [ ] **Step 4: Manual sample-app run against both brokers**

Start both brokers per the README commands from Task 20, Step 6, then:

```sh
./gradlew :messaging-sample-app:run --args="--transport jms-artemis --url jms://artemis:artemis@localhost:61616 --admin-url http://artemis:artemis@localhost:8161"
./gradlew :messaging-sample-app:run --args="--transport jms-ibm-mq --url jms://app:passw0rd@localhost:1414 --admin-url https://app:passw0rd@localhost:9443"
```

Expected: both exit 0. Stop one broker and re-run its command; expected: exit 1.

- [ ] **Step 5: Final full build**

Run: `gradlew.bat build`
Expected: PASS (runs `test`, `check` — which includes `integrationTest` per `messaging-jms/build.gradle`'s `check.dependsOn integrationTest` — and assembles artifacts).

- [ ] **Step 6: Report completion**

Do not commit anything beyond what Tasks 1-21 already committed until this step's verification is green — per `CLAUDE.md`'s "Do not commit anything until verification of work is done." If everything in Steps 1-5 passed, the plan is complete; hand off to `superpowers:finishing-a-development-branch` for the branch-completion decision, per the executing-plans/subagent-driven-development skill's own Step 3.

---

## Self-review notes (spec coverage, placeholders, type consistency)

**Spec coverage:**
- §A (settlement) — Tasks 6, 9, 14, 16 (redelivery break-checks on both InMemoryTransport and JmsConsumerUnit).
- §B (owner-loop) — Task 14 (`JmsConsumerUnit`), Task 6 (`InMemoryTransport.ConsumerUnit`).
- §C (concurrency) — Tasks 10, 14, 16 (topic/queue concurrency scenarios + shared subscription name).
- §D (subscription readiness/lifetime) — Task 14 (`ready` future per unit), Task 9 (close semantics).
- §E (publish/persistence) — Task 12 (§E rejection), Task 13 (`DeliveryMode.PERSISTENT`, non-transacted publish session).
- §F (provisioning, missing destinations) — Task 8 (Topic/Queue split), Task 15/17 (`BrokerAdmin`), Task 20 Step 2 (contract amendment).
- §G (message model, headers, redelivered flag) — Task 14 (`toMessage`), Task 15 (`JmsRedeliveredScenario`).
- §H (listener contract) — Task 4 (`SafeListener`), Task 7 (real throw test), Task 20 Step 3 (RECONNECTED amendment).
- §I (config/discovery) — Task 1 (URL-first resolution), Task 12 (connection-factory + passthrough), Task 20 Step 4 (amendment).
- §J (shutdown) — Task 6, Task 9, Task 14 (both transports' `close`/`stop` semantics), Task 16 Step 5 (break-check).
- §K (adapter-owned reconnection) — Task 13 (`ExceptionListener` → `DISCONNECTED` only; no `Reconnector`).
- Sample app decisions — Task 19.
- Contract amendments (§C, §F, §H, §I) — Task 20.
- Risks section items — flagged inline in Tasks 15, 16, 17, 18 rather than deferred silently.

**Placeholder scan:** every code step above contains complete, real code; no `TBD`/`TODO`/"similar to Task N" remain (the one such phrase from an earlier draft of Task 4 was found and removed during self-review).

**Type consistency:** `BusSettings(MessagingListener, Duration, int)` is used identically in Tasks 5, 6, 7, 9, 10, 11, 15, 16, 17, 18. `ConsumerUnit.settle(Message, boolean)` (Task 6) and `JmsConsumerUnit`'s inline settle logic (Task 14) follow the same success/failure shape. `BrokerAdmin.createQueue/createTopic(String) -> Destination` (Task 15) is implemented identically by `ArtemisAdmin` (Task 15) and `IbmMqAdmin` (Task 17). `JmsTransport.resolveDestination(Session, Destination)` (Task 13) is the single implementation reused by both publish (Task 13) and `JmsConsumerUnit` (Task 14) — not duplicated.

