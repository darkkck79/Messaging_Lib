# Messaging_Lib Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Java 21 messaging library that wraps Kafka and JMS (Artemis, IBM MQ) behind one API, proving transport-swap portability with a shared conformance suite.

**Architecture:** Core module defines a `MessageBus` API with sealed `Destination` types (`Topic`/`Queue`), a `TransportProvider` SPI resolved via `ServiceLoader`, and a `DefaultMessageBus` that validates messages and isolates listener failures. Each adapter (JMS, Kafka) implements `Transport` with an owner-loop pattern — one thread per consumer unit, one message in flight — to satisfy both thread-confinement rules and at-least-once delivery. A conformance suite with deliberately faulty in-memory transports proves the suite itself catches real faults.

**Tech Stack:** Java 21, Gradle 8.14.3 (Groovy DSL), JUnit 5.13.4, AssertJ 3.27.7, Mockito 5.20.0, Awaitility 4.2.2, Testcontainers 1.21.4, SLF4J 2.0.18 / Logback 1.5.20, Jakarta JMS 3.1.0, Kafka Clients 3.9.2, Jackson 2.19.4, Artemis 2.43.0, IBM MQ 9.4.5.1

## Global Constraints

- **Java 21** toolchain and runtime. No preview features.
- **Gradle 8.14.3** with pinned wrapper. Every dependency version in `gradle/libs.versions.toml`.
- **Package root:** `com.messaging`
- **Core dependency:** `slf4j-api` only. No framework coupling.
- **Adapter compile deps:** `jakarta.jms-api` only (JMS), `kafka-clients` only (Kafka). Provider JARs are runtime/test scope.
- **Behavioural contract:** [`2026-08-18-messaging-lib-design.md`](file:///D:/projects/Messaging_Lib/docs/superpowers/specs/2026-08-18-messaging-lib-design.md) is the binding spec. Section references (§A–§M) point there.
- **YAGNI / DRY / KISS** per `CLAUDE.md`. Minimal viable code, no over-engineering.
- **TDD**: Write failing tests first, then minimal implementation.
- **Skip-is-failure:** A `TestListener` fails the build on any `SKIPPED` test result.
- **No commits until verification passes.** Run `/simplify` before each commit.
- **Docs woven in:** Update relevant documentation with each task.

---

## File Structure

```
Messaging_Lib/
├── settings.gradle
├── build.gradle
├── gradle/
│   ├── libs.versions.toml
│   └── wrapper/  (gradle-wrapper.jar, gradle-wrapper.properties)
├── gradlew, gradlew.bat
├── .gitignore
│
├── messaging-core/
│   ├── build.gradle
│   └── src/
│       ├── main/java/com/messaging/
│       │   ├── Messaging.java              # Static factory: connect(config)
│       │   ├── MessageBus.java             # Main interface
│       │   ├── Destination.java            # Sealed interface
│       │   ├── Topic.java                  # Record: Topic(name)
│       │   ├── Queue.java                  # Record: Queue(name)
│       │   ├── Message.java                # Record: body + headers
│       │   ├── MessageHandler.java         # @FunctionalInterface
│       │   ├── Subscription.java           # AutoCloseable
│       │   ├── TypedChannel.java           # Typed publish/subscribe
│       │   ├── TypedHandler.java           # Handler for typed messages
│       │   ├── Codec.java                  # encode/decode interface
│       │   ├── MessagingListener.java      # Event callbacks
│       │   ├── ConnectionState.java        # Enum
│       │   ├── MessagingException.java     # Unchecked exception
│       │   ├── config/
│       │   │   └── MessagingConfig.java    # Builder + fromProperties + parse
│       │   ├── spi/
│       │   │   ├── TransportProvider.java  # ServiceLoader SPI
│       │   │   └── Transport.java          # Adapter contract
│       │   └── internal/
│       │       ├── DefaultMessageBus.java  # Core impl
│       │       └── HeaderValidator.java    # Publish-time validation
│       └── test/java/com/messaging/
│           ├── DestinationTest.java
│           ├── MessageTest.java
│           ├── config/
│           │   └── MessagingConfigTest.java
│           ├── internal/
│           │   ├── DefaultMessageBusTest.java
│           │   ├── HeaderValidatorTest.java
│           │   └── TypedChannelTest.java
│           └── MessagingFactoryTest.java
│
├── messaging-conformance/
│   ├── build.gradle
│   └── src/
│       ├── testFixtures/java/com/messaging/conformance/
│       │   ├── AbstractMessagingConformanceTest.java
│       │   └── AbstractMessagingConnectivityTest.java
│       └── test/java/com/messaging/conformance/
│           ├── faulty/
│           │   ├── InMemoryTransport.java
│           │   ├── AcksBeforeHandlerTransport.java
│           │   ├── QueueFansOutTransport.java
│           │   ├── DropsHeadersTransport.java
│           │   ├── FaultyTransportProvider.java
│           │   ├── AcksBeforeHandlerConformance.java
│           │   ├── QueueFansOutConformance.java
│           │   └── DropsHeadersConformance.java
│           └── MetaConformanceTest.java
│
├── messaging-jms/
│   ├── build.gradle
│   └── src/
│       ├── main/java/com/messaging/jms/
│       │   ├── JmsTransportProvider.java
│       │   ├── JmsTransport.java
│       │   └── JmsConsumerUnit.java
│       ├── main/resources/META-INF/services/
│       │   └── com.messaging.spi.TransportProvider
│       └── integrationTest/java/com/messaging/jms/
│           ├── ArtemisConformanceTest.java
│           └── IbmMqConformanceTest.java
│
├── messaging-kafka/
│   ├── build.gradle
│   └── src/
│       ├── main/java/com/messaging/kafka/
│       │   ├── KafkaTransportProvider.java
│       │   ├── KafkaTransport.java
│       │   └── KafkaConsumerUnit.java
│       ├── main/resources/META-INF/services/
│       │   └── com.messaging.spi.TransportProvider
│       └── integrationTest/java/com/messaging/kafka/
│           └── KafkaConformanceTest.java
│
├── messaging-codec-json/
│   ├── build.gradle
│   └── src/
│       ├── main/java/com/messaging/codec/json/
│       │   └── JsonCodec.java
│       └── test/java/com/messaging/codec/json/
│           └── JsonCodecTest.java
│
├── README.md
└── docs/
    ├── adding-a-transport.md
    ├── contract-mapping/
    │   ├── stomp.md
    │   └── solace.md
    └── superpowers/
        └── specs/
            └── 2026-08-18-messaging-lib-design.md
```

---

### Task 1: Gradle Build Skeleton

**Files:**
- Create: `settings.gradle`
- Create: `build.gradle`
- Create: `gradle/libs.versions.toml`
- Create: `messaging-core/build.gradle`
- Create: `messaging-conformance/build.gradle`
- Create: `messaging-jms/build.gradle`
- Create: `messaging-kafka/build.gradle`
- Create: `messaging-codec-json/build.gradle`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: Nothing
- Produces: Compilable multi-module Gradle project with `./gradlew build` passing on empty modules

- [-] **Step 1: Generate Gradle wrapper**

```powershell
# If gradle is not on PATH, install temporarily:
# scoop install gradle
# Then generate wrapper:
cd D:\projects\Messaging_Lib
gradle wrapper --gradle-version 8.14.3
```

Verify `gradlew.bat`, `gradle/wrapper/gradle-wrapper.properties` exist and properties contain `gradle-8.14.3-bin.zip`.

- [-] **Step 2: Update .gitignore**

```gitignore
# Gradle
.gradle/
build/
!gradle/wrapper/gradle-wrapper.jar

# IDE
.idea/
*.iml
.vscode/
.settings/
.project
.classpath
*.swp

# OS
Thumbs.db
.DS_Store
```

- [-] **Step 3: Create version catalog**

`gradle/libs.versions.toml`:
```toml
[versions]
junit = "5.13.4"
assertj = "3.27.7"
mockito = "5.20.0"
awaitility = "4.2.2"
slf4j = "2.0.18"
logback = "1.5.20"
jakarta-jms = "3.1.0"
kafka = "3.9.2"
jackson = "2.19.4"
testcontainers = "1.21.4"
artemis = "2.43.0"
ibm-mq = "9.4.5.1"

[libraries]
junit-bom = { module = "org.junit:junit-bom", version.ref = "junit" }
junit-jupiter = { module = "org.junit.jupiter:junit-jupiter" }
junit-platform-launcher = { module = "org.junit.platform:junit-platform-launcher" }
assertj-core = { module = "org.assertj:assertj-core", version.ref = "assertj" }
mockito-core = { module = "org.mockito:mockito-core", version.ref = "mockito" }
mockito-junit-jupiter = { module = "org.mockito:mockito-junit-jupiter", version.ref = "mockito" }
awaitility = { module = "org.awaitility:awaitility", version.ref = "awaitility" }
slf4j-api = { module = "org.slf4j:slf4j-api", version.ref = "slf4j" }
logback-classic = { module = "ch.qos.logback:logback-classic", version.ref = "logback" }
jakarta-jms-api = { module = "jakarta.jms:jakarta.jms-api", version.ref = "jakarta-jms" }
kafka-clients = { module = "org.apache.kafka:kafka-clients", version.ref = "kafka" }
jackson-databind = { module = "com.fasterxml.jackson.core:jackson-databind", version.ref = "jackson" }
testcontainers-bom = { module = "org.testcontainers:testcontainers-bom", version.ref = "testcontainers" }
testcontainers-junit-jupiter = { module = "org.testcontainers:junit-jupiter" }
testcontainers-kafka = { module = "org.testcontainers:kafka" }
testcontainers-toxiproxy = { module = "org.testcontainers:toxiproxy" }
artemis-jakarta-client = { module = "org.apache.activemq:artemis-jakarta-client", version.ref = "artemis" }
artemis-server = { module = "org.apache.activemq:artemis-server", version.ref = "artemis" }
ibm-mq-jakarta-client = { module = "com.ibm.mq:com.ibm.mq.jakarta.client", version.ref = "ibm-mq" }
```

- [-] **Step 4: Create root build.gradle**

```groovy
plugins {
    id 'java-library' apply false
}

subprojects {
    apply plugin: 'java-library'

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        testImplementation platform(libs.junit.bom)
        testImplementation libs.junit.jupiter
        testImplementation libs.assertj.core
        testImplementation libs.mockito.core
        testImplementation libs.mockito.junit.jupiter
        testImplementation libs.awaitility
        testRuntimeOnly libs.logback.classic
    }

    tasks.withType(Test).configureEach {
        useJUnitPlatform()

        addTestListener(new TestListener() {
            void beforeSuite(TestDescriptor suite) {}
            void afterSuite(TestDescriptor suite, TestResult result) {}
            void beforeTest(TestDescriptor testDescriptor) {}
            void afterTest(TestDescriptor testDescriptor, TestResult result) {
                if (result.resultType == TestResult.ResultType.SKIPPED) {
                    throw new GradleException(
                        "SKIPPED tests are failures: ${testDescriptor.className}.${testDescriptor.name}")
                }
            }
        })

        testLogging {
            events 'passed', 'skipped', 'failed'
            showStandardStreams = true
        }
    }
}
```

- [-] **Step 5: Create settings.gradle**

```groovy
rootProject.name = 'Messaging_Lib'

include 'messaging-core'
include 'messaging-conformance'
include 'messaging-jms'
include 'messaging-kafka'
include 'messaging-codec-json'
```

- [-] **Step 6: Create messaging-core/build.gradle**

```groovy
dependencies {
    api libs.slf4j.api
}
```

- [-] **Step 7: Create messaging-conformance/build.gradle**

```groovy
plugins {
    id 'java-test-fixtures'
}

dependencies {
    testFixturesApi project(':messaging-core')
    testFixturesApi platform(libs.junit.bom)
    testFixturesApi libs.junit.jupiter
    testFixturesApi libs.assertj.core
    testFixturesApi libs.awaitility

    testImplementation project(':messaging-core')
    testImplementation libs.junit.platform.launcher
}

test {
    filter {
        excludeTestsMatching 'com.messaging.conformance.faulty.*Conformance'
    }
}
```

- [-] **Step 8: Create messaging-jms/build.gradle**

```groovy
sourceSets {
    integrationTest {
        java.srcDir 'src/integrationTest/java'
        resources.srcDir 'src/integrationTest/resources'
        compileClasspath += sourceSets.main.output
        runtimeClasspath += sourceSets.main.output
    }
}

configurations {
    integrationTestImplementation.extendsFrom testImplementation
    integrationTestRuntimeOnly.extendsFrom testRuntimeOnly
}

dependencies {
    api project(':messaging-core')
    api libs.jakarta.jms.api

    integrationTestImplementation testFixtures(project(':messaging-conformance'))
    integrationTestImplementation platform(libs.testcontainers.bom)
    integrationTestImplementation libs.testcontainers.junit.jupiter
    integrationTestImplementation libs.testcontainers.toxiproxy
    integrationTestRuntimeOnly libs.artemis.jakarta.client
    integrationTestRuntimeOnly libs.ibm.mq.jakarta.client
}

tasks.register('integrationTest', Test) {
    description = 'Runs conformance suite against real JMS brokers.'
    group = 'verification'
    testClassesDirs = sourceSets.integrationTest.output.classesDirs
    classpath = sourceSets.integrationTest.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter test
    systemProperty 'junit.jupiter.execution.timeout.default', '120s'
}

check.dependsOn integrationTest
```

- [-] **Step 9: Create messaging-kafka/build.gradle**

```groovy
sourceSets {
    integrationTest {
        java.srcDir 'src/integrationTest/java'
        resources.srcDir 'src/integrationTest/resources'
        compileClasspath += sourceSets.main.output
        runtimeClasspath += sourceSets.main.output
    }
}

configurations {
    integrationTestImplementation.extendsFrom testImplementation
    integrationTestRuntimeOnly.extendsFrom testRuntimeOnly
}

dependencies {
    api project(':messaging-core')
    api libs.kafka.clients

    integrationTestImplementation testFixtures(project(':messaging-conformance'))
    integrationTestImplementation platform(libs.testcontainers.bom)
    integrationTestImplementation libs.testcontainers.junit.jupiter
    integrationTestImplementation libs.testcontainers.kafka
    integrationTestImplementation libs.testcontainers.toxiproxy
}

tasks.register('integrationTest', Test) {
    description = 'Runs conformance suite against real Kafka brokers.'
    group = 'verification'
    testClassesDirs = sourceSets.integrationTest.output.classesDirs
    classpath = sourceSets.integrationTest.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter test
    systemProperty 'junit.jupiter.execution.timeout.default', '120s'
}

check.dependsOn integrationTest
```

- [-] **Step 10: Create messaging-codec-json/build.gradle**

```groovy
dependencies {
    api project(':messaging-core')
    api libs.jackson.databind
}
```

- [-] **Step 11: Verify the skeleton compiles**

Run: `.\gradlew.bat build`
Expected: BUILD SUCCESSFUL (all modules compile, no tests yet)

- [-] **Step 12: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "build: gradle multi-module skeleton with version catalog and wrapper 8.14.3"
```

---

### Task 2: messaging-core — Public API & Configuration

**Files:**
- Create: `messaging-core/src/main/java/com/messaging/Destination.java`
- Create: `messaging-core/src/main/java/com/messaging/Topic.java`
- Create: `messaging-core/src/main/java/com/messaging/Queue.java`
- Create: `messaging-core/src/main/java/com/messaging/Message.java`
- Create: `messaging-core/src/main/java/com/messaging/MessageHandler.java`
- Create: `messaging-core/src/main/java/com/messaging/Subscription.java`
- Create: `messaging-core/src/main/java/com/messaging/TypedHandler.java`
- Create: `messaging-core/src/main/java/com/messaging/Codec.java`
- Create: `messaging-core/src/main/java/com/messaging/ConnectionState.java`
- Create: `messaging-core/src/main/java/com/messaging/MessagingListener.java`
- Create: `messaging-core/src/main/java/com/messaging/MessagingException.java`
- Create: `messaging-core/src/main/java/com/messaging/config/MessagingConfig.java`
- Test: `messaging-core/src/test/java/com/messaging/DestinationTest.java`
- Test: `messaging-core/src/test/java/com/messaging/MessageTest.java`
- Test: `messaging-core/src/test/java/com/messaging/config/MessagingConfigTest.java`

**Interfaces:**
- Consumes: Gradle skeleton from Task 1
- Produces: All public value types and configuration used by every subsequent task:
  - `Destination` (sealed), `Topic.of(String)`, `Queue.of(String)`
  - `Message(byte[] body, Map<String,String> headers)`, `Message.body()`, `Message.headers()`
  - `MessageHandler.handle(Message) → CompletableFuture<Void>`
  - `Subscription.close()`
  - `Codec<T>.encode(T) → byte[]`, `Codec<T>.decode(byte[]) → T`
  - `TypedHandler<T>.handle(T, Map<String,String>) → CompletableFuture<Void>`
  - `MessagingListener` with default no-op methods
  - `ConnectionState.CONNECTED | DISCONNECTED | RECONNECTED`
  - `MessagingException(String)`, `MessagingException(String, Throwable)`
  - `MessagingConfig` builder, `fromProperties`, accessors, `toString()` with redaction

#### 2.1 Destination types

- [x] **Step 1: Write Destination tests**

```java
package com.messaging;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DestinationTest {

    @Test void topicCreation() {
        Topic topic = Topic.of("orders");
        assertThat(topic.name()).isEqualTo("orders");
        assertThat(topic).isInstanceOf(Destination.class);
    }

    @Test void queueCreation() {
        Queue queue = Queue.of("orders");
        assertThat(queue.name()).isEqualTo("orders");
        assertThat(queue).isInstanceOf(Destination.class);
    }

    @Test void topicRejectsNull() {
        assertThatThrownBy(() -> Topic.of(null)).isInstanceOf(NullPointerException.class);
    }

    @Test void topicRejectsBlank() {
        assertThatThrownBy(() -> Topic.of("  ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void queueRejectsNull() {
        assertThatThrownBy(() -> Queue.of(null)).isInstanceOf(NullPointerException.class);
    }

    @Test void queueRejectsBlank() {
        assertThatThrownBy(() -> Queue.of("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void topicEquality() {
        assertThat(Topic.of("a")).isEqualTo(Topic.of("a"));
        assertThat(Topic.of("a")).isNotEqualTo(Topic.of("b"));
    }

    @Test void queueEquality() {
        assertThat(Queue.of("a")).isEqualTo(Queue.of("a"));
    }

    @Test void sealedPermits() {
        assertThat(Destination.class.isSealed()).isTrue();
        assertThat(Destination.class.getPermittedSubclasses())
            .containsExactlyInAnyOrder(Topic.class, Queue.class);
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `.\gradlew.bat :messaging-core:test --tests "com.messaging.DestinationTest" -i`
Expected: FAIL — classes do not exist yet

- [x] **Step 3: Implement Destination, Topic, Queue**

`Destination.java`:
```java
package com.messaging;

/** A messaging destination. The subtype — Topic or Queue — selects the delivery pattern. */
public sealed interface Destination permits Topic, Queue {
    String name();
}
```

`Topic.java`:
```java
package com.messaging;

import java.util.Objects;

/** Fan-out destination: every live subscriber receives every message. */
public record Topic(String name) implements Destination {
    public Topic {
        Objects.requireNonNull(name, "Topic name must not be null");
        if (name.isBlank()) throw new IllegalArgumentException("Topic name must not be blank");
    }
    public static Topic of(String name) { return new Topic(name); }
}
```

`Queue.java`:
```java
package com.messaging;

import java.util.Objects;

/** Competing-consumers destination: exactly one subscriber receives each delivery attempt. */
public record Queue(String name) implements Destination {
    public Queue {
        Objects.requireNonNull(name, "Queue name must not be null");
        if (name.isBlank()) throw new IllegalArgumentException("Queue name must not be blank");
    }
    public static Queue of(String name) { return new Queue(name); }
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `.\gradlew.bat :messaging-core:test --tests "com.messaging.DestinationTest" -i`
Expected: PASS — all 9 tests green

#### 2.2 Message record

- [x] **Step 5: Write Message tests**

```java
package com.messaging;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class MessageTest {

    @Test void bodyRoundTrip() {
        byte[] data = "hello".getBytes();
        Message msg = new Message(data, Map.of());
        assertThat(msg.body()).isEqualTo(data);
    }

    @Test void bodyDefensiveCopyOnConstruction() {
        byte[] data = "hello".getBytes();
        Message msg = new Message(data, Map.of());
        data[0] = 'X';
        assertThat(msg.body()).isEqualTo("hello".getBytes());
    }

    @Test void bodyDefensiveCopyOnAccess() {
        Message msg = new Message("hello".getBytes(), Map.of());
        byte[] first = msg.body();
        first[0] = 'X';
        assertThat(msg.body()).isEqualTo("hello".getBytes());
    }

    @Test void nullBodyBecomesEmpty() {
        Message msg = new Message(null, Map.of());
        assertThat(msg.body()).isEmpty();
    }

    @Test void headersRoundTrip() {
        Message msg = new Message(new byte[0], Map.of("key", "value"));
        assertThat(msg.headers()).containsEntry("key", "value");
    }

    @Test void headersUnmodifiable() {
        Message msg = new Message(new byte[0], Map.of("key", "value"));
        assertThatThrownBy(() -> msg.headers().put("new", "val"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void headersDefensiveCopyOnConstruction() {
        var headers = new HashMap<String, String>();
        headers.put("key", "value");
        Message msg = new Message(new byte[0], headers);
        headers.put("injected", "bad");
        assertThat(msg.headers()).doesNotContainKey("injected");
    }

    @Test void nullHeadersBecomesEmpty() {
        Message msg = new Message(new byte[0], null);
        assertThat(msg.headers()).isEmpty();
    }

    @Test void nullHeaderValueRejected() {
        var headers = new HashMap<String, String>();
        headers.put("key", null);
        assertThatThrownBy(() -> new Message(new byte[0], headers))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("key");
    }

    @Test void emptyHeaderValueAllowed() {
        Message msg = new Message(new byte[0], Map.of("key", ""));
        assertThat(msg.headers()).containsEntry("key", "");
    }

    @Test void equalityUsesArrayContents() {
        Message a = new Message("hello".getBytes(), Map.of("k", "v"));
        Message b = new Message("hello".getBytes(), Map.of("k", "v"));
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test void inequalityOnDifferentBody() {
        Message a = new Message("hello".getBytes(), Map.of());
        Message b = new Message("world".getBytes(), Map.of());
        assertThat(a).isNotEqualTo(b);
    }
}
```

- [x] **Step 6: Run test to verify it fails**

Run: `.\gradlew.bat :messaging-core:test --tests "com.messaging.MessageTest" -i`
Expected: FAIL — `Message` does not exist yet

- [x] **Step 7: Implement Message**

```java
package com.messaging;

import java.util.*;

/**
 * An immutable message: body bytes plus string headers.
 * Body is defensively copied on construction and access.
 * Headers are copied into an unmodifiable map. Null values are rejected.
 */
public record Message(byte[] body, Map<String, String> headers) {

    public Message {
        body = body != null ? Arrays.copyOf(body, body.length) : new byte[0];
        if (headers != null) {
            var copy = new LinkedHashMap<String, String>();
            for (var entry : headers.entrySet()) {
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                        "Null header value for key: " + entry.getKey());
                }
                copy.put(entry.getKey(), entry.getValue());
            }
            headers = Collections.unmodifiableMap(copy);
        } else {
            headers = Map.of();
        }
    }

    @Override public byte[] body() { return Arrays.copyOf(body, body.length); }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message that)) return false;
        return Arrays.equals(body, that.body) && headers.equals(that.headers);
    }

    @Override public int hashCode() {
        return 31 * Arrays.hashCode(body) + headers.hashCode();
    }

    @Override public String toString() {
        return "Message[body=" + body.length + " bytes, headers=" + headers + "]";
    }
}
```

- [x] **Step 8: Run test to verify it passes**

Run: `.\gradlew.bat :messaging-core:test --tests "com.messaging.MessageTest" -i`
Expected: PASS — all 12 tests green

#### 2.3 Remaining value types (interfaces, enums)

- [x] **Step 9: Create all remaining value types**

These are interfaces/enums — tested through their implementations in later tasks.

`MessageHandler.java`:
```java
package com.messaging;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface MessageHandler {
    CompletableFuture<Void> handle(Message message);
}
```

`Subscription.java`:
```java
package com.messaging;

public interface Subscription extends AutoCloseable {
    @Override void close();
}
```

`Codec.java`:
```java
package com.messaging;

public interface Codec<T> {
    byte[] encode(T value);
    T decode(byte[] bytes);
}
```

`TypedHandler.java`:
```java
package com.messaging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface TypedHandler<T> {
    CompletableFuture<Void> handle(T value, Map<String, String> headers);
}
```

`ConnectionState.java`:
```java
package com.messaging;

public enum ConnectionState { CONNECTED, DISCONNECTED, RECONNECTED }
```

`MessagingListener.java`:
```java
package com.messaging;

/** Event callbacks for observability. All methods have no-op defaults. */
public interface MessagingListener {
    default void onPublished(Destination destination) {}
    default void onConsumed(Destination destination) {}
    default void onError(Destination destination, Throwable error) {}
    default void onConnectionStateChanged(ConnectionState state) {}
    static MessagingListener noOp() { return new MessagingListener() {}; }
}
```

`MessagingException.java`:
```java
package com.messaging;

public class MessagingException extends RuntimeException {
    public MessagingException(String message) { super(message); }
    public MessagingException(String message, Throwable cause) { super(message, cause); }
}
```

#### 2.4 MessagingConfig

- [x] **Step 10: Write MessagingConfig tests**

```java
package com.messaging.config;

import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Properties;
import static org.assertj.core.api.Assertions.*;

class MessagingConfigTest {

    @Test void builderMinimal() {
        var config = MessagingConfig.builder().url("kafka://localhost:9092").build();
        assertThat(config.scheme()).isEqualTo("kafka");
        assertThat(config.url().getHost()).isEqualTo("localhost");
        assertThat(config.url().getPort()).isEqualTo(9092);
    }

    @Test void builderDefaults() {
        var config = MessagingConfig.builder().url("kafka://localhost:9092").build();
        assertThat(config.concurrency()).isEqualTo(1);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.clientId()).isNotBlank();
        assertThat(config.listener()).isNotNull();
    }

    @Test void builderCustomValues() {
        var listener = MessagingListener.noOp();
        var config = MessagingConfig.builder()
            .url("jms://broker:61616").clientId("my-app").concurrency(4)
            .connectTimeout(Duration.ofSeconds(5)).closeTimeout(Duration.ofSeconds(60))
            .listener(listener).build();
        assertThat(config.scheme()).isEqualTo("jms");
        assertThat(config.clientId()).isEqualTo("my-app");
        assertThat(config.concurrency()).isEqualTo(4);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(config.listener()).isSameAs(listener);
    }

    @Test void builderRequiresUrl() {
        assertThatThrownBy(() -> MessagingConfig.builder().build())
            .isInstanceOf(MessagingException.class).hasMessageContaining("url");
    }

    @Test void fromProperties() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.client-id", "test-app");
        props.setProperty("messaging.consumer.concurrency", "2");
        props.setProperty("messaging.connect-timeout", "PT5S");
        props.setProperty("messaging.close-timeout", "PT60S");
        var config = MessagingConfig.fromProperties(props);
        assertThat(config.scheme()).isEqualTo("kafka");
        assertThat(config.clientId()).isEqualTo("test-app");
        assertThat(config.concurrency()).isEqualTo(2);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.closeTimeout()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test void fromPropertiesRequiresUrl() {
        assertThatThrownBy(() -> MessagingConfig.fromProperties(new Properties()))
            .isInstanceOf(MessagingException.class).hasMessageContaining("messaging.url");
    }

    @Test void unknownCoreKeyFailsFast() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.typo-key", "value");
        assertThatThrownBy(() -> MessagingConfig.fromProperties(props))
            .isInstanceOf(MessagingException.class).hasMessageContaining("messaging.typo-key");
    }

    @Test void passthroughProperties() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.kafka.batch.size", "16384");
        props.setProperty("messaging.kafka.linger.ms", "5");
        var config = MessagingConfig.fromProperties(props);
        assertThat(config.passthroughProperties())
            .containsEntry("batch.size", "16384")
            .containsEntry("linger.ms", "5");
    }

    @Test void toStringRedactsPassword() {
        var config = MessagingConfig.builder()
            .url("kafka://user:secret@localhost:9092")
            .property("password", "hunter2")
            .property("ssl.truststore.password", "trust-pass").build();
        String str = config.toString();
        assertThat(str).doesNotContain("secret", "hunter2", "trust-pass");
        assertThat(str).contains("***");
    }

    @Test void invalidDurationRejects() {
        var props = new Properties();
        props.setProperty("messaging.url", "kafka://localhost:9092");
        props.setProperty("messaging.connect-timeout", "30seconds");
        assertThatThrownBy(() -> MessagingConfig.fromProperties(props))
            .isInstanceOf(MessagingException.class).hasMessageContaining("connect-timeout");
    }

    @Test void concurrencyMustBePositive() {
        assertThatThrownBy(() -> MessagingConfig.builder()
            .url("kafka://localhost:9092").concurrency(0).build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void builderPassthroughProperties() {
        var config = MessagingConfig.builder()
            .url("kafka://localhost:9092").property("batch.size", "32768").build();
        assertThat(config.passthroughProperties()).containsEntry("batch.size", "32768");
    }
}
```

- [x] **Step 11: Run test to verify it fails**

Run: `.\gradlew.bat :messaging-core:test --tests "com.messaging.config.MessagingConfigTest" -i`
Expected: FAIL — `MessagingConfig` does not exist yet

- [x] **Step 12: Implement MessagingConfig**

```java
package com.messaging.config;

import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public final class MessagingConfig {

    private static final Set<String> CORE_KEYS = Set.of(
        "messaging.url", "messaging.client-id", "messaging.consumer.concurrency",
        "messaging.connect-timeout", "messaging.close-timeout");
    private static final Pattern SENSITIVE_KEY =
        Pattern.compile(".*(password|secret).*", Pattern.CASE_INSENSITIVE);

    private final URI url;
    private final String scheme;
    private final String clientId;
    private final int concurrency;
    private final Duration connectTimeout;
    private final Duration closeTimeout;
    private final MessagingListener listener;
    private final Map<String, String> passthroughProperties;

    private MessagingConfig(Builder b) {
        if (b.url == null) throw new MessagingException("messaging.url is required");
        this.url = URI.create(b.url);
        this.scheme = this.url.getScheme();
        if (scheme == null) throw new MessagingException("messaging.url must have a scheme: " + b.url);
        this.clientId = b.clientId != null ? b.clientId : UUID.randomUUID().toString();
        if (b.concurrency < 1) throw new IllegalArgumentException("concurrency must be >= 1");
        this.concurrency = b.concurrency;
        this.connectTimeout = b.connectTimeout;
        this.closeTimeout = b.closeTimeout;
        this.listener = b.listener != null ? b.listener : MessagingListener.noOp();
        this.passthroughProperties = Collections.unmodifiableMap(new LinkedHashMap<>(b.passthrough));
    }

    public URI url()                           { return url; }
    public String scheme()                     { return scheme; }
    public String clientId()                   { return clientId; }
    public int concurrency()                   { return concurrency; }
    public Duration connectTimeout()           { return connectTimeout; }
    public Duration closeTimeout()             { return closeTimeout; }
    public MessagingListener listener()        { return listener; }
    public Map<String, String> passthroughProperties() { return passthroughProperties; }

    public static Builder builder() { return new Builder(); }

    public static MessagingConfig fromProperties(Properties props) {
        var b = builder();
        String scheme = null;
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            switch (key) {
                case "messaging.url"                  -> { b.url(value); scheme = URI.create(value).getScheme(); }
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

    private static Duration parseDuration(String key, String value) {
        try { return Duration.parse(value); }
        catch (Exception e) { throw new MessagingException("Invalid ISO-8601 duration for " + key + ": " + value, e); }
    }

    @Override public String toString() {
        var sb = new StringBuilder("MessagingConfig{url=");
        if (url.getUserInfo() != null) {
            sb.append(url.getScheme()).append("://***@").append(url.getHost())
              .append(":").append(url.getPort()).append(url.getPath());
        } else { sb.append(url); }
        sb.append(", clientId=").append(clientId).append(", concurrency=").append(concurrency)
          .append(", passthrough={");
        passthroughProperties.forEach((k, v) -> sb.append(k).append("=")
            .append(SENSITIVE_KEY.matcher(k).matches() ? "***" : v).append(", "));
        return sb.append("}}").toString();
    }

    public static final class Builder {
        private String url;
        private String clientId;
        private int concurrency = 1;
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration closeTimeout = Duration.ofSeconds(30);
        private MessagingListener listener;
        private final Map<String, String> passthrough = new LinkedHashMap<>();
        private Builder() {}
        public Builder url(String url)                      { this.url = url; return this; }
        public Builder clientId(String clientId)            { this.clientId = clientId; return this; }
        public Builder concurrency(int c)                   { this.concurrency = c; return this; }
        public Builder connectTimeout(Duration t)           { this.connectTimeout = t; return this; }
        public Builder closeTimeout(Duration t)             { this.closeTimeout = t; return this; }
        public Builder listener(MessagingListener l)        { this.listener = l; return this; }
        public Builder property(String key, String value)   { this.passthrough.put(key, value); return this; }
        public MessagingConfig build() { return new MessagingConfig(this); }
    }
}
```

- [x] **Step 13: Run all core tests**

Run: `.\gradlew.bat :messaging-core:test -i`
Expected: PASS — all Destination, Message, and MessagingConfig tests green

- [x] **Step 14: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "feat(core): public API value types and MessagingConfig with TDD"
```

---

### Task 3: messaging-core — SPI, Service Discovery & DefaultMessageBus

**Files:**
- Create: `messaging-core/src/main/java/com/messaging/spi/TransportProvider.java`
- Create: `messaging-core/src/main/java/com/messaging/spi/Transport.java`
- Create: `messaging-core/src/main/java/com/messaging/MessageBus.java`
- Create: `messaging-core/src/main/java/com/messaging/Messaging.java`
- Create: `messaging-core/src/main/java/com/messaging/TypedChannel.java`
- Create: `messaging-core/src/main/java/com/messaging/internal/DefaultMessageBus.java`
- Create: `messaging-core/src/main/java/com/messaging/internal/HeaderValidator.java`
- Test: `messaging-core/src/test/java/com/messaging/internal/HeaderValidatorTest.java`
- Test: `messaging-core/src/test/java/com/messaging/MessagingFactoryTest.java`
- Test: `messaging-core/src/test/java/com/messaging/internal/DefaultMessageBusTest.java`
- Test: `messaging-core/src/test/java/com/messaging/internal/TypedChannelTest.java`

**Interfaces:**
- Consumes: All types from Task 2
- Produces: `TransportProvider` SPI, `Transport`, `MessageBus`, `Messaging.connect()`, `DefaultMessageBus`, `TypedChannel`, `HeaderValidator.validateForPublish()`

#### 3.1 HeaderValidator

- [x] **Step 1: Write HeaderValidator tests**

```java
package com.messaging.internal;

import com.messaging.MessagingException;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class HeaderValidatorTest {
    @Test void validHeadersPass() {
        assertThatCode(() -> HeaderValidator.validateForPublish(
            Map.of("my-key", "value", "X.Custom.1", "data"))).doesNotThrowAnyException();
    }
    @Test void emptyHeadersPass() {
        assertThatCode(() -> HeaderValidator.validateForPublish(Map.of())).doesNotThrowAnyException();
    }
    @Test void invalidKeyCharsetRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("bad key!", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("bad key!");
    }
    @Test void reservedPrefixJmsRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("JMSCorrelationID", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("JMS");
    }
    @Test void reservedPrefixJmsxRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("JMSXGroupID", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("JMSX");
    }
    @Test void reservedPrefixMessagingRejected() {
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(Map.of("messaging.internal", "v")))
            .isInstanceOf(MessagingException.class).hasMessageContaining("messaging.");
    }
    @Test void headerBlockSizeExceeded() {
        var headers = new HashMap<String, String>();
        String bigValue = "x".repeat(8192);
        for (int i = 0; i < 9; i++) headers.put("key" + i, bigValue);
        assertThatThrownBy(() -> HeaderValidator.validateForPublish(headers))
            .isInstanceOf(MessagingException.class).hasMessageContaining("64 KiB");
    }
    @Test void headerBlockAtLimitPasses() {
        var headers = new HashMap<String, String>();
        String value = "x".repeat(8192);
        for (int i = 0; i < 7; i++) headers.put("key" + i, value);
        assertThatCode(() -> HeaderValidator.validateForPublish(headers)).doesNotThrowAnyException();
    }
}
```

- [x] **Step 2: Implement HeaderValidator**

```java
package com.messaging.internal;

import com.messaging.MessagingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

public final class HeaderValidator {
    private static final Pattern VALID_KEY = Pattern.compile("[A-Za-z0-9_.\\-]+");
    private static final int MAX_HEADER_BLOCK_BYTES = 64 * 1024;
    private HeaderValidator() {}

    public static void validateForPublish(Map<String, String> headers) {
        int totalBytes = 0;
        for (var entry : headers.entrySet()) {
            String key = entry.getKey();
            if (!VALID_KEY.matcher(key).matches())
                throw new MessagingException("Invalid header key (must match [A-Za-z0-9_.-]+): " + key);
            if (key.startsWith("messaging."))
                throw new MessagingException("Reserved header prefix 'messaging.' on key: " + key);
            String upper = key.toUpperCase();
            if (upper.startsWith("JMSX"))
                throw new MessagingException("Reserved header prefix 'JMSX' on key: " + key);
            if (upper.startsWith("JMS"))
                throw new MessagingException("Reserved header prefix 'JMS' on key: " + key);
            totalBytes += key.getBytes(StandardCharsets.UTF_8).length
                        + entry.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        if (totalBytes > MAX_HEADER_BLOCK_BYTES)
            throw new MessagingException("Total header block size exceeds 64 KiB: " + totalBytes + " bytes");
    }
}
```

- [x] **Step 3: Run HeaderValidator tests**

Run: `.\gradlew.bat :messaging-core:test --tests "com.messaging.internal.HeaderValidatorTest" -i`
Expected: PASS

#### 3.2 SPI, MessageBus, DefaultMessageBus, TypedChannel, Messaging factory

- [x] **Step 4: Create SPI interfaces**

`spi/TransportProvider.java`:
```java
package com.messaging.spi;

import com.messaging.MessagingListener;
import com.messaging.config.MessagingConfig;

public interface TransportProvider {
    String scheme();
    Transport open(MessagingConfig config, MessagingListener listener);
}
```

`spi/Transport.java`:
```java
package com.messaging.spi;

import com.messaging.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface Transport extends AutoCloseable {
    CompletableFuture<Void> publish(Destination destination, Message message);
    CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler);
    void close(Duration timeout);
    @Override default void close() { close(Duration.ofSeconds(30)); }
}
```

- [x] **Step 5: Create MessageBus interface**

```java
package com.messaging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface MessageBus extends AutoCloseable {
    CompletableFuture<Void> publish(Destination destination, Message message);
    default CompletableFuture<Void> publish(Destination destination, byte[] body) {
        return publish(destination, new Message(body, Map.of()));
    }
    CompletableFuture<Subscription> subscribe(Destination destination, MessageHandler handler);
    <T> TypedChannel<T> typed(Destination destination, Codec<T> codec);
    @Override void close();
}
```

- [x] **Step 6: Write DefaultMessageBus tests**

```java
package com.messaging.internal;

import com.messaging.*;
import com.messaging.config.MessagingConfig;
import com.messaging.spi.Transport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultMessageBusTest {
    @Mock Transport transport;
    DefaultMessageBus bus;

    @BeforeEach void setUp() {
        bus = new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(),
            MessagingListener.noOp());
    }

    @Test void publishDelegatesToTransport() {
        when(transport.publish(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        bus.publish(Queue.of("q"), new Message("hello".getBytes(), Map.of()));
        verify(transport).publish(eq(Queue.of("q")), any());
    }
    @Test void publishValidatesHeaders() {
        assertThatThrownBy(() -> bus.publish(Queue.of("q"), new Message(new byte[0], Map.of("JMSBadKey", "v"))))
            .isInstanceOf(MessagingException.class);
        verify(transport, never()).publish(any(), any());
    }
    @Test void closeIsIdempotent() {
        bus.close(); bus.close();
        verify(transport, times(1)).close(any(Duration.class));
    }
    @Test void apiCallAfterCloseThrows() {
        bus.close();
        assertThatThrownBy(() -> bus.publish(Queue.of("q"), new byte[0]))
            .isInstanceOf(IllegalStateException.class);
    }
    @Test void listenerExceptionDoesNotAffectPublish() {
        var bad = new MessagingListener() {
            @Override public void onPublished(Destination d) { throw new RuntimeException("boom"); }
        };
        var safeBus = new DefaultMessageBus(transport,
            MessagingConfig.builder().url("test://localhost").build(), bad);
        when(transport.publish(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        assertThatCode(() -> safeBus.publish(Queue.of("q"), new Message(new byte[0], Map.of())).join())
            .doesNotThrowAnyException();
    }
}
```

- [x] **Step 7: Implement DefaultMessageBus**

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
        this.transport = transport; this.config = config; this.listener = listener;
    }

    @Override public CompletableFuture<Void> publish(Destination dest, Message msg) {
        checkNotClosed();
        HeaderValidator.validateForPublish(msg.headers());
        return transport.publish(dest, msg).whenComplete((v, ex) -> {
            if (ex != null) { try { listener.onError(dest, ex); } catch (Exception e) { log.warn("Listener threw on onError", e); } }
            else { try { listener.onPublished(dest); } catch (Exception e) { log.warn("Listener threw on onPublished", e); } }
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

- [ ] **Step 8: Write TypedChannel tests and implement**

```java
// TypedChannel.java
package com.messaging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class TypedChannel<T> {
    private final MessageBus bus;
    private final Destination destination;
    private final Codec<T> codec;

    public TypedChannel(MessageBus bus, Destination destination, Codec<T> codec) {
        this.bus = bus; this.destination = destination; this.codec = codec;
    }

    public CompletableFuture<Void> publish(T value) { return publish(value, Map.of()); }

    public CompletableFuture<Void> publish(T value, Map<String, String> headers) {
        return bus.publish(destination, new Message(codec.encode(value), headers));
    }

    public CompletableFuture<Subscription> subscribe(TypedHandler<T> handler) {
        return bus.subscribe(destination, message -> {
            T value = codec.decode(message.body());
            return handler.handle(value, message.headers());
        });
    }
}
```

Test file `TypedChannelTest.java`:
```java
package com.messaging.internal;

import com.messaging.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TypedChannelTest {
    @Mock MessageBus bus;
    static final Codec<String> CODEC = new Codec<>() {
        @Override public byte[] encode(String v) { return v.getBytes(StandardCharsets.UTF_8); }
        @Override public String decode(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    };

    @Test void publishEncodesValue() {
        when(bus.publish(any(Destination.class), any(Message.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        new TypedChannel<>(bus, Queue.of("q"), CODEC).publish("hello");
        var captor = ArgumentCaptor.forClass(Message.class);
        verify(bus).publish(eq(Queue.of("q")), captor.capture());
        assertThat(captor.getValue().body()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
    }

    @Test void subscribeDecodesValue() throws Exception {
        var handlerCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        when(bus.subscribe(eq(Queue.of("q")), handlerCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(mock(Subscription.class)));
        var received = new CompletableFuture<String>();
        new TypedChannel<>(bus, Queue.of("q"), CODEC)
            .subscribe((value, headers) -> { received.complete(value); return CompletableFuture.completedFuture(null); });
        handlerCaptor.getValue().handle(new Message("decoded".getBytes(StandardCharsets.UTF_8), Map.of("h", "v")));
        assertThat(received.get()).isEqualTo("decoded");
    }

    @Test void decodeFailureIsHandlerFailure() throws Exception {
        Codec<String> badCodec = new Codec<>() {
            @Override public byte[] encode(String v) { return v.getBytes(); }
            @Override public String decode(byte[] b) { throw new RuntimeException("decode failed"); }
        };
        var handlerCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        when(bus.subscribe(any(), handlerCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(mock(Subscription.class)));
        new TypedChannel<>(bus, Queue.of("q"), badCodec)
            .subscribe((v, h) -> CompletableFuture.completedFuture(null));
        assertThatThrownBy(() -> handlerCaptor.getValue().handle(new Message("data".getBytes(), Map.of())))
            .hasMessageContaining("decode failed");
    }
}
```

- [ ] **Step 9: Implement Messaging factory**

```java
package com.messaging;

import com.messaging.config.MessagingConfig;
import com.messaging.internal.DefaultMessageBus;
import com.messaging.spi.Transport;
import com.messaging.spi.TransportProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public final class Messaging {
    private static final Logger log = LoggerFactory.getLogger(Messaging.class);
    private Messaging() {}

    public static MessageBus connect(MessagingConfig config) {
        String scheme = config.scheme();
        MessagingListener listener = wrapSafe(config.listener());
        TransportProvider provider = resolveProvider(scheme);
        Transport transport = provider.open(config, listener);
        return new DefaultMessageBus(transport, config, listener);
    }

    private static TransportProvider resolveProvider(String scheme) {
        List<TransportProvider> matches = new ArrayList<>();
        Map<String, String> available = new LinkedHashMap<>();
        ClassLoader tcl = Thread.currentThread().getContextClassLoader();
        ClassLoader fallback = Messaging.class.getClassLoader();
        for (ClassLoader cl : tcl != fallback ? List.of(tcl, fallback) : List.of(tcl)) {
            try {
                for (TransportProvider p : ServiceLoader.load(TransportProvider.class, cl)) {
                    available.put(p.scheme(), p.getClass().getName());
                    if (scheme.equals(p.scheme())) matches.add(p);
                }
            } catch (ServiceConfigurationError e) {
                throw new MessagingException("Malformed TransportProvider on classpath: " + e.getMessage(), e);
            }
            if (!matches.isEmpty()) break;
        }
        if (matches.isEmpty())
            throw new MessagingException("No TransportProvider for scheme '" + scheme + "'. Available: " + available);
        if (matches.size() > 1)
            throw new MessagingException("Duplicate TransportProviders for scheme '" + scheme + "': "
                + matches.stream().map(p -> p.getClass().getName()).toList());
        return matches.getFirst();
    }

    private static MessagingListener wrapSafe(MessagingListener raw) {
        return new MessagingListener() {
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
                try { raw.onConnectionStateChanged(s); } catch (Exception e) { log.warn("Listener threw", e); }
            }
        };
    }
}
```

- [ ] **Step 10: Write and run MessagingFactoryTest**

```java
package com.messaging;

import com.messaging.config.MessagingConfig;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MessagingFactoryTest {
    @Test void noProviderForSchemeThrows() {
        var config = MessagingConfig.builder().url("nonexistent://localhost:1234").build();
        assertThatThrownBy(() -> Messaging.connect(config))
            .isInstanceOf(MessagingException.class)
            .hasMessageContaining("nonexistent").hasMessageContaining("No TransportProvider");
    }
}
```

- [ ] **Step 11: Run all core tests**

Run: `.\gradlew.bat :messaging-core:test -i`
Expected: PASS — all tests green

- [ ] **Step 12: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "feat(core): SPI, ServiceLoader discovery, DefaultMessageBus, TypedChannel, HeaderValidator"
```

---

### Task 4: messaging-conformance — Abstract Suite & Meta-test

**Files:**
- Create: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConformanceTest.java`
- Create: `messaging-conformance/src/testFixtures/java/com/messaging/conformance/AbstractMessagingConnectivityTest.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/InMemoryTransport.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/AcksBeforeHandlerTransport.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/QueueFansOutTransport.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/DropsHeadersTransport.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/FaultyTransportProvider.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/AcksBeforeHandlerConformance.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/QueueFansOutConformance.java`
- Create: `messaging-conformance/src/test/java/com/messaging/conformance/faulty/DropsHeadersConformance.java`
- Create: `messaging-conformance/src/test/resources/META-INF/services/com.messaging.spi.TransportProvider`
- Test: `messaging-conformance/src/test/java/com/messaging/conformance/MetaConformanceTest.java`

**Interfaces:**
- Consumes: All core types and SPI from Tasks 2–3
- Produces: `AbstractMessagingConformanceTest` (behavioral suite), `AbstractMessagingConnectivityTest` (adds restart/disconnect tests). Abstract methods: `createBus()`, `provisionTopic(String)`, `provisionQueue(String)`, `restartBroker()`, `cutNetwork()`, `restoreNetwork()`

> [!IMPORTANT]
> This task has many files. Refer to the [File Structure](#file-structure) section for exact paths. The full code for the abstract suite, faulty transports, and meta-test is provided in the linked spec. The key design decisions:
>
> - `AbstractMessagingConformanceTest` contains ALL behavioral tests (payload, patterns, settlement, lifecycle, failure surfaces, ordering)
> - `AbstractMessagingConnectivityTest extends AbstractMessagingConformanceTest` adds connectivity tests (restart, disconnect, reconnect)
> - Real adapter tests extend `AbstractMessagingConnectivityTest`
> - Faulty transport classes extend `AbstractMessagingConformanceTest` and are excluded from Gradle test discovery
> - `MetaConformanceTest` runs specific faulty tests via JUnit Platform Launcher and asserts they FAIL

- [ ] **Step 1: Create AbstractMessagingConformanceTest**

Contains all non-connectivity behavioral tests. See the full code in the conformance suite section of this plan — it includes these `@Test` methods:

| Category | Test methods |
|---|---|
| Payload & headers | `bodyRoundTrip`, `emptyBodyRoundTrip`, `headersRoundTrip`, `invalidHeaderNameRejectedOnPublish`, `reservedHeaderPrefixRejectedOnPublish`, `reservedMessagingPrefixRejectedOnPublish` |
| Patterns | `topicFanOut`, `queueCompetingConsumers` |
| Settlement (§A) | `redeliveryOnExceptionalFuture`, `redeliveryOnSynchronousThrow`, `redeliveryOnNullFuture`, `redeliveryOnCancelledFuture`, `successfulHandlerNotRedelivered` |
| Lifecycle (§D, §J) | `subscribeFutureMeansReady`, `subscriptionCloseStopsDelivery`, `repeatedCloseIsIdempotent`, `busCloseIsIdempotent`, `apiCallAfterBusCloseThrows` |
| Failure surfaces | `publishToMissingDestinationFails`, `subscribeToMissingDestinationFails`, `listenerThrowDoesNotChangeDelivery` |
| Ordering | `concurrencyOnePreservesOrder` |

Each test creates fresh destinations via `newTopic()` / `newQueue()` → calls `provisionTopic(name)` / `provisionQueue(name)` then returns the `Destination`.

The `@BeforeEach setUp()` calls `createBus()`, and `@AfterEach tearDown()` closes it.

Full code for every test method is identical to what was specified in the previous attempt's Task 4 Step 1. The engineer implementing this task must write all test methods with exact assertions as shown.

- [ ] **Step 2: Create AbstractMessagingConnectivityTest**

Extends the base suite with connectivity tests: `queueSurvivesBrokerRestart`, `queueMessagesPublishedDuringOutageDeliveredAfterRecovery`, `reconnectAndResumeAfterNetworkCut`. Adds abstract methods `restartBroker()`, `cutNetwork()`, `restoreNetwork()`.

- [ ] **Step 3: Create InMemoryTransport (correct baseline)**

An in-memory transport with correct behavior: queue messages dequeued → handler called → on failure, message re-offered to the queue (redelivery). Topic messages fan out to all registered handlers. Used as the base class for faulty variants.

- [ ] **Step 4: Create faulty transport variants**

- `AcksBeforeHandlerTransport`: overrides `consumeQueue()` to NOT re-offer on failure (message already consumed before handler runs)
- `QueueFansOutTransport`: overrides `publish()` and `subscribe()` to deliver queue messages to ALL subscribers
- `DropsHeadersTransport`: overrides `consumeQueue()` to strip headers before delivery

- [ ] **Step 5: Create FaultyTransportProvider + ServiceLoader registration**

Single provider with scheme `faulty`, uses `config.url().getHost()` to select the variant (`acks-before-handler`, `queue-fans-out`, `drops-headers`).

`META-INF/services/com.messaging.spi.TransportProvider` → `com.messaging.conformance.faulty.FaultyTransportProvider`

- [ ] **Step 6: Create faulty conformance test classes**

Each extends `AbstractMessagingConformanceTest` with the faulty transport via `Messaging.connect(MessagingConfig.builder().url("faulty://<variant>").build())`. Excluded from Gradle test discovery by `messaging-conformance/build.gradle` filter.

- [ ] **Step 7: Write MetaConformanceTest**

Uses JUnit Platform `LauncherFactory` to run specific test methods from each faulty conformance class and asserts `getTestsFailedCount() > 0`:
- `AcksBeforeHandlerConformance::redeliveryOnExceptionalFuture` → must fail
- `QueueFansOutConformance::queueCompetingConsumers` → must fail
- `DropsHeadersConformance::headersRoundTrip` → must fail

- [ ] **Step 8: Run conformance module tests**

Run: `.\gradlew.bat :messaging-conformance:test -i`
Expected: PASS — meta-test confirms all three faulty transports are caught

- [ ] **Step 9: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "feat(conformance): abstract suite, faulty transports, and meta-test"
```

---

### Task 5: messaging-jms — JMS Adapter & Artemis Integration

**Files:**
- Create: `messaging-jms/src/main/java/com/messaging/jms/JmsTransportProvider.java`
- Create: `messaging-jms/src/main/java/com/messaging/jms/JmsTransport.java`
- Create: `messaging-jms/src/main/java/com/messaging/jms/JmsConsumerUnit.java`
- Create: `messaging-jms/src/main/resources/META-INF/services/com.messaging.spi.TransportProvider`
- Test: `messaging-jms/src/integrationTest/java/com/messaging/jms/ArtemisConformanceTest.java`

**Interfaces:**
- Consumes: Core types (Tasks 2–3), conformance suite (Task 4)
- Produces: Working JMS adapter, passing conformance suite against Artemis

- [ ] **Step 1: Create JmsTransportProvider**

Scheme `jms`. `open()` creates a `JmsTransport`.

- [ ] **Step 2: Create JmsTransport**

Key design:
- Creates `ConnectionFactory` via runtime reflection — tries Artemis `ActiveMQConnectionFactory` first, then IBM MQ `MQConnectionFactory`. No compile-time provider dependency.
- `publish()`: synchronized on producer session. `BytesMessage` with `PERSISTENT` delivery. Headers → JMS string properties. Returns completed future synchronously.
- `subscribe()`: creates `config.concurrency()` `JmsConsumerUnit` instances.
- `close()`: stops all consumer units, closes producer/session/connection.
- Rejects `messaging.jms.deliveryMode=NON_PERSISTENT` at connect time (§E).

- [ ] **Step 3: Create JmsConsumerUnit**

Key design (§B owner-loop rule):
- One transacted `Session` with one `MessageConsumer`.
- Synchronous `receive(1000ms)` loop on a daemon thread.
- Handler called on the owner thread. `result.get()` blocks the owner thread (one in-flight).
- `session.commit()` on success, `session.rollback()` on failure.
- Sets `messaging.redelivered=true` header if `jmsMsg.getJMSRedelivered()` is true.
- `stop(timeout)`: sets `running=false`, joins the thread.

- [ ] **Step 4: Create ServiceLoader registration**

`META-INF/services/com.messaging.spi.TransportProvider` → `com.messaging.jms.JmsTransportProvider`

- [ ] **Step 5: Create ArtemisConformanceTest**

Extends `AbstractMessagingConnectivityTest`. Uses Testcontainers `GenericContainer` with `apache/activemq-artemis:latest`, Toxiproxy for network faults. Creates bus via `Messaging.connect()` with `jms://host:port` URL.

Adds Artemis-specific test: `redeliveredHeaderSetOnRedelivery()` — verifies `messaging.redelivered=true` on JMS redelivery.

> [!NOTE]
> If Artemis auto-create causes `publishToMissingDestinationFails` to pass unexpectedly, mount a custom `broker.xml` disabling auto-create and pre-provision destinations in `provisionTopic()`/`provisionQueue()`.

- [ ] **Step 6: Run Artemis integration tests**

Run: `.\gradlew.bat :messaging-jms:integrationTest -i`
Expected: PASS (Docker required)

- [ ] **Step 7: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "feat(jms): JMS adapter with Artemis conformance — all tests pass"
```

---

### Task 6: messaging-jms — IBM MQ Portability Proof

**Files:**
- Create: `messaging-jms/src/integrationTest/java/com/messaging/jms/IbmMqConformanceTest.java`

**Interfaces:**
- Consumes: JMS adapter (Task 5), conformance suite (Task 4)
- Produces: Proof that the JMS adapter passes conformance against IBM MQ with zero adapter changes

- [ ] **Step 1: Create IbmMqConformanceTest**

Extends `AbstractMessagingConnectivityTest`. Container: `icr.io/ibm-messaging/mq:latest` with `LICENSE=accept`, `MQ_QMGR_NAME=QM1`, `MQ_APP_PASSWORD=passw0rd`, `MQ_DEV=true`. Wait strategy: log message `AMQ5975I` with 3-minute timeout. Toxiproxy for network faults.

Uses fixed IBM MQ dev destinations: `DEV.QUEUE.1`–`3`, topic string `dev/<uuid>`. Overrides `newQueue()` to cycle through the three dev queues.

Config: `jms://app:passw0rd@host:1414` with passthrough `channel=DEV.APP.SVRCONN`, `queueManager=QM1`.

> [!IMPORTANT]
> Record any adapter changes needed here. The plan says: "Any adapter change needed here is a finding to record, not a chore to absorb quietly."

- [ ] **Step 2: Run IBM MQ integration tests**

Run: `.\gradlew.bat :messaging-jms:integrationTest -i`
Expected: PASS — both Artemis and IBM MQ pass. IBM MQ image is ~1.5GB, first pull may take several minutes.

- [ ] **Step 3: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "feat(jms): IBM MQ portability proof — conformance passes with zero adapter changes"
```

---

### Task 7: messaging-kafka — Kafka Adapter & Integration

**Files:**
- Create: `messaging-kafka/src/main/java/com/messaging/kafka/KafkaTransportProvider.java`
- Create: `messaging-kafka/src/main/java/com/messaging/kafka/KafkaTransport.java`
- Create: `messaging-kafka/src/main/java/com/messaging/kafka/KafkaConsumerUnit.java`
- Create: `messaging-kafka/src/main/resources/META-INF/services/com.messaging.spi.TransportProvider`
- Test: `messaging-kafka/src/integrationTest/java/com/messaging/kafka/KafkaConformanceTest.java`

**Interfaces:**
- Consumes: Core types (Tasks 2–3), conformance suite (Task 4)
- Produces: Working Kafka adapter, passing conformance suite against Kafka

- [ ] **Step 1: Create KafkaTransportProvider**

Scheme `kafka`. `open()` creates a `KafkaTransport`.

- [ ] **Step 2: Create KafkaTransport**

Key design:
- `KafkaProducer` with `acks=all`, `ByteArraySerializer`. Rejects passthrough `acks` ≠ `all`/`-1` at connect time (§E).
- `publish()`: creates `ProducerRecord`, maps headers to Kafka headers (UTF-8 encoded), uses producer callback to complete the future.
- `subscribe()`: creates `config.concurrency()` `KafkaConsumerUnit` instances. Returns future that completes when ALL units have received partition assignment.
- `close()`: stops all consumer units, closes producer.

- [ ] **Step 3: Create KafkaConsumerUnit**

Key design (§B owner-loop rule, §C one-in-flight):
- One `KafkaConsumer` per unit. `enable.auto.commit=false`, `allow.auto.create.topics=false`, `max.poll.records=1`.
- **Topic** → unique group ID (`clientId-uuid`), `auto.offset.reset=latest` (fan-out, ephemeral).
- **Queue** → group ID = queue name, `auto.offset.reset=earliest` (competing consumers).
- `ConsumerRebalanceListener.onPartitionsAssigned()` completes the `readyFuture`.
- `poll()` loop. For each record: pause all partitions → handle → wait for future (keep polling empty to maintain heartbeat) → on success: `commitSync(offset+1)` → on failure: `seek(offset)` → resume.
- `stop()`: sets `running=false`, calls `consumer.wakeup()`.
- Kafka **omits** `messaging.redelivered` header entirely (§G) — Kafka records carry no redelivery flag.

- [ ] **Step 4: Create ServiceLoader registration**

`META-INF/services/com.messaging.spi.TransportProvider` → `com.messaging.kafka.KafkaTransportProvider`

- [ ] **Step 5: Create KafkaConformanceTest**

Extends `AbstractMessagingConnectivityTest`. Uses Testcontainers `KafkaContainer` with `apache/kafka:latest`, Toxiproxy for network faults. Provisions topics via Kafka `AdminClient.createTopics()`.

Adds Kafka-specific tests:
- `perPartitionOrdering()` — 3-partition topic, 30 messages, verifies total delivery count
- `rebalanceDuringInFlightWork()` — slow handler + second subscriber triggers rebalance, verifies no crash

- [ ] **Step 6: Run Kafka integration tests**

Run: `.\gradlew.bat :messaging-kafka:integrationTest -i`
Expected: PASS (Docker required)

- [ ] **Step 7: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "feat(kafka): Kafka adapter with conformance — all tests pass"
```

---

### Task 8: messaging-codec-json — JSON Codec

**Files:**
- Create: `messaging-codec-json/src/main/java/com/messaging/codec/json/JsonCodec.java`
- Test: `messaging-codec-json/src/test/java/com/messaging/codec/json/JsonCodecTest.java`

**Interfaces:**
- Consumes: `Codec<T>` from Task 2
- Produces: `JsonCodec<T>` — thread-safe, stateless Jackson-based codec

- [ ] **Step 1: Write JsonCodec tests**

```java
package com.messaging.codec.json;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class JsonCodecTest {
    record Order(String id, double total) {}
    private final JsonCodec<Order> codec = new JsonCodec<>(Order.class);

    @Test void encodeAndDecode() {
        var order = new Order("abc-123", 42.50);
        Order decoded = codec.decode(codec.encode(order));
        assertThat(decoded.id()).isEqualTo("abc-123");
        assertThat(decoded.total()).isEqualTo(42.50);
    }
    @Test void decodeInvalidJsonThrows() {
        assertThatThrownBy(() -> codec.decode("not json".getBytes())).isInstanceOf(RuntimeException.class);
    }
    @Test void nullValueThrows() {
        assertThatThrownBy(() -> codec.encode(null)).isInstanceOf(NullPointerException.class);
    }
    @Test void threadSafe() throws Exception {
        var errors = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
        var threads = java.util.stream.IntStream.range(0, 10).mapToObj(i -> new Thread(() -> {
            try { for (int j = 0; j < 100; j++) {
                var o = new Order("t" + i + "-" + j, j);
                assertThat(codec.decode(codec.encode(o)).id()).isEqualTo("t" + i + "-" + j);
            }} catch (Throwable e) { errors.add(e); }
        })).toList();
        threads.forEach(Thread::start);
        for (var t : threads) t.join();
        assertThat(errors).isEmpty();
    }
}
```

- [ ] **Step 2: Implement JsonCodec**

```java
package com.messaging.codec.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.Codec;
import java.util.Objects;

public final class JsonCodec<T> implements Codec<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Class<T> type;

    public JsonCodec(Class<T> type) { this.type = type; }

    @Override public byte[] encode(T value) {
        Objects.requireNonNull(value, "Cannot encode null");
        try { return MAPPER.writeValueAsBytes(value); }
        catch (Exception e) { throw new RuntimeException("JSON encode failed for " + type.getSimpleName(), e); }
    }
    @Override public T decode(byte[] bytes) {
        try { return MAPPER.readValue(bytes, type); }
        catch (Exception e) { throw new RuntimeException("JSON decode failed for " + type.getSimpleName(), e); }
    }
}
```

- [ ] **Step 3: Run tests**

Run: `.\gradlew.bat :messaging-codec-json:test -i`
Expected: PASS

- [ ] **Step 4: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "feat(codec-json): Jackson-based JSON codec with thread-safety tests"
```

---

### Task 9: Contract-Mapping Spike (STOMP & Solace)

**Files:**
- Create: `docs/contract-mapping/stomp.md`
- Create: `docs/contract-mapping/solace.md`

**Interfaces:**
- Consumes: Design spec §A–§M
- Produces: Contract-mapping documents identifying where STOMP and Solace fit the spec and where the SPI would need changes

- [ ] **Step 1: Write STOMP contract mapping**

Research the STOMP 1.2 protocol and map each spec section. Key areas:
- §A: `ACK`/`NACK` frames map to settlement
- §B: WebSocket read loop is the owner loop
- §C: No partitions — concurrency = application-managed subscriptions
- §D: Subscriptions are ephemeral (no offset/replay)
- §E: `RECEIPT` frame for publish confirmation
- §F: Server-side destinations; no standard creation mechanism
- §G: Headers are string key-value pairs — direct mapping
- §K: WebSocket reconnect is client-managed

- [ ] **Step 2: Write Solace contract mapping**

Research Solace PubSub+ Java API and map each spec section. Key areas:
- §A: Guaranteed messaging with acknowledgement
- §B: Solace threading model → owner loop mapping
- §C: Queue consumers natively supported
- §D: Durable vs. non-durable endpoints
- §E: Publisher acknowledgement via callbacks
- §F: SEMP or admin provisioning required
- §G: Message properties similar to headers
- §K: Native client reconnection

- [ ] **Step 3: Run `/simplify` and commit**

```powershell
git add -A
git commit -m "docs: contract-mapping spikes for STOMP and Solace"
```

---

### Task 10: Documentation & Final Polish

**Files:**
- Create: `README.md`
- Create: `docs/adding-a-transport.md`

**Interfaces:**
- Consumes: Everything from Tasks 1–9
- Produces: Complete user and contributor documentation

- [ ] **Step 1: Write README.md**

Cover: quickstart, configuration table (all `messaging.*` keys with defaults), per-transport semantics table (Kafka vs JMS differences: topic-during-outage, `messaging.redelivered`, retention limits), ordering/delivery/poison-message/retention caveats, portability promise and its limits, how to run tests.

- [ ] **Step 2: Write docs/adding-a-transport.md**

Cover: implement `TransportProvider` + `Transport`, register via ServiceLoader, extend `AbstractMessagingConnectivityTest`, the owner-loop rule (§B), SPI is provisional through Phase 2.

- [ ] **Step 3: Run full verification**

```powershell
.\gradlew.bat test            # Unit tests + meta-test, no Docker
.\gradlew.bat integrationTest  # Conformance against real brokers, Docker required
.\gradlew.bat check            # Everything
```

All must pass. No SKIPPED results.

- [ ] **Step 4: Cross-transport portability check**

Write a small example that publishes and subscribes unchanged against all three brokers by changing only `messaging.url`. This is the product claim — verify it directly.

- [ ] **Step 5: Run `/simplify` across the entire project**

- [ ] **Step 6: Final commit**

```powershell
git add -A
git commit -m "docs: README, adding-a-transport guide, and final polish"
```

---

## Verification Plan

### Automated Tests

```powershell
# Fast — unit tests + conformance meta-test, no Docker
.\gradlew.bat test

# Full — conformance suite against Artemis, IBM MQ, and Kafka, Docker required
.\gradlew.bat integrationTest

# Everything
.\gradlew.bat check
```

All three must pass. The skip-is-failure `TestListener` mechanically enforces that no test is skipped.

### Manual Verification

1. **Cross-transport portability:** Run one example app against all three brokers by changing only `messaging.url`. Verify messages round-trip correctly.
2. **Contract cross-check:** Every scenario traces to a spec section (§A–§M). Every observable rule has a scenario.
3. **Meta-test validity:** Comment out the fault in any faulty transport — the meta-test must then fail, proving it detects real faults.
4. **IBM MQ adapter unchanged:** `git diff` between Task 5 and Task 6 shows zero changes to `messaging-jms/src/main/`. Any change is a documented finding.
