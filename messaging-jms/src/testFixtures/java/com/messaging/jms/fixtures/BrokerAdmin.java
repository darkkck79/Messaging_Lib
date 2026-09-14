package com.messaging.jms.fixtures;

import com.messaging.Destination;

/** Provisions destinations outside the library API (§F) — the suite never creates one
 * itself. Every implementation gives each name a per-run unique suffix. */
public interface BrokerAdmin {
    Destination createQueue(String name);
    Destination createTopic(String name);
}
