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
