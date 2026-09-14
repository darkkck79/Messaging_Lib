package com.messaging.conformance;

import com.messaging.MessagingListener;
import java.time.Duration;

/** Settings a transport is opened with, mirroring what a real adapter reads from
 * {@code MessagingConfig} via {@code open(config, listener)}. */
public record BusSettings(MessagingListener listener, Duration closeTimeout, int concurrency) {}
