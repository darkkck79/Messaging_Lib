package com.messaging.conformance.faulty;

import com.messaging.Destination;

/**
 * Faulty variant: delivers every Queue message to ALL subscribers instead of exactly one
 * (fan-out instead of competing-consumers). Violates the Queue semantics in the scope-of
 * -the-guarantee table (§ Scope of the guarantee). Used only by
 * {@code QueueFansOutConformance} to prove the suite catches it.
 */
public final class QueueFansOutTransport extends InMemoryTransport {

    @Override
    protected boolean fansOut(Destination destination) {
        // Bug under test: Queues broadcast to every subscriber, exactly like a Topic.
        return true;
    }
}
