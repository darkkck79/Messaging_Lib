# Manual test: IBM MQ conformance run

**Why this is manual:** the JMS adapter's IBM MQ integration test
(`messaging-jms/src/integrationTest/java/com/messaging/jms/IbmMqConformanceTest.java`) needs
Docker to pull and run `icr.io/ibm-messaging/mq`, which was not available in the environment
this test was authored in. It compiles and follows the same pattern already proven against
Artemis, but **has never actually been run.** Please run it once and report the outcome back.

## Prerequisites

- Docker available and able to pull `icr.io/ibm-messaging/mq:9.4.5.1-r1` (this requires
  accepting IBM's container image terms — the image is public but gated by license acceptance,
  which the test itself handles via `LICENSE=accept`).
- Enough free RAM/CPU for the container (IBM MQ's dev image is heavier than Artemis's).

## Running it

```sh
gradlew.bat :messaging-jms:integrationTest --tests "com.messaging.jms.IbmMqConformanceTest"
```

This starts one `IBM_MQ` container per test class run (`@BeforeAll`/`@AfterAll`), same
lifecycle as `ArtemisConformanceTest`, and runs the full `AbstractMessagingConformanceTest`
scenario suite plus `redeliveredHeaderSetOnRedelivery` (the JMS-only §G amendment scenario)
against it — using **the same `messaging-jms/src/main` adapter code, unmodified**, that
already passed against Artemis.

## What to report back

1. **Full pass, zero skips:** great — the adapter is now proven against both brokers with
   zero code differences, matching the spec's cross-provider requirement. Let me know and I'll
   mark Task 18 done in the plan and fold this into the final verification sweep (Task 22).

2. **Container fails to start** (image pull, license, resource limits): paste the container
   startup error. This is almost certainly an environment/image issue, not an adapter bug —
   we'll adjust the container config (image tag, memory limits, wait strategy) rather than
   touch `src/main`.

3. **One or more scenarios fail once the broker is up:** paste:
   - the failing test name(s) and full stack trace,
   - `docker logs <container>` output for the IBM MQ container around the failure time.

   Per the plan's Phase 6 rule: **do not let me "fix" a failure by changing
   `messaging-jms/src/main`** to special-case IBM MQ — the whole point of this adapter is zero
   code differences between providers. If a real fix would require touching `src/main`, we
   record it as a risk in the spec instead (see
   `docs/superpowers/specs/2026-09-13-jms-adapter-design.md`, Risks section) and decide from
   there together. Likely first suspects if something fails, per the original plan's own risk
   list:
   - `createSharedConsumer` support on the dev image (topic concurrency scenarios,
     `topicConcurrencyDeliversOncePerSubscription`).
   - IBM MQ's REST admin API path/headers (`IbmMqAdmin.runMqsc`) — the dev image's exact
     `/api/v2/admin/action/qmgr/QM1/mqsc` path and `ibm-mq-rest-csrf-token` header requirement
     can vary by patch version; a 400/401 there is a fixture bug, not an adapter bug.

## Known scope limitation (already recorded for Artemis, applies here too)

This test does **not** exercise the §K network-outage/reconnection scenarios
(`AbstractMessagingConnectivityTest`'s `cutNetwork`/`restoreNetwork`). Those require routing
the JMS connection through a Toxiproxy proxy, which reproducibly failed in the environment
this plan was executed in (see `ArtemisConformanceTest`'s javadoc and the spec's Risks
section) — so neither the Artemis nor the IBM MQ integration test currently proves §K against
a real broker. If you have a Linux Docker host (native Docker, not Docker Desktop for
Windows) available, it's worth trying the Toxiproxy-based version there — the npipe/vpnkit
port-forwarding layer is specific to Docker Desktop for Windows and may not reproduce on
Linux.
