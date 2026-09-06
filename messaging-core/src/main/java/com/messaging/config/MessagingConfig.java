package com.messaging.config;

import com.messaging.MessagingException;
import com.messaging.MessagingListener;
import com.messaging.spi.TransportProvider;
import java.net.URI;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Immutable transport configuration. Built via {@link #builder()}, parsed from
 * a connection URI via {@link #parse(String)}, or seeded from
 * {@link Properties} via {@link #fromProperties(Properties)}.
 */
public final class MessagingConfig {

    private static final String PREFIX = "messaging.";
    private static final String URL = PREFIX + "url";
    private static final String CLIENT_ID = PREFIX + "client-id";
    private static final String CONCURRENCY = PREFIX + "consumer.concurrency";
    private static final String CONNECT_TIMEOUT = PREFIX + "connect-timeout";
    private static final String CLOSE_TIMEOUT = PREFIX + "close-timeout";
    private static final Set<String> CORE_KEYS =
        Set.of(URL, CLIENT_ID, CONCURRENCY, CONNECT_TIMEOUT, CLOSE_TIMEOUT);

    private final URI url;
    private final String scheme;
    private final String clientId;
    private final int concurrency;
    private final Duration connectTimeout;
    private final Duration closeTimeout;
    private final MessagingListener listener;
    private final Map<String, String> passthroughProperties;

    private MessagingConfig(Builder builder) {
        this.url = builder.url;
        this.scheme = builder.url != null ? builder.url.getScheme() : "";
        this.clientId = builder.clientId != null ? builder.clientId : "messaging-" + UUID.randomUUID();
        this.concurrency = builder.concurrency;
        this.connectTimeout = builder.connectTimeout;
        this.closeTimeout = builder.closeTimeout;
        this.listener = builder.listener != null ? builder.listener : MessagingListener.noOp();
        this.passthroughProperties = Map.copyOf(builder.passthroughProperties);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Parses a connection URI into a {@link MessagingConfig}. */
    public static MessagingConfig parse(String uri) {
        Objects.requireNonNull(uri, "uri must not be null");
        return builder().url(uri).build();
    }

    /** Seeds a builder from {@code messaging.*} properties; unknown core keys fail fast. */
    public static MessagingConfig fromProperties(Properties properties) {
        Objects.requireNonNull(properties, "properties must not be null");
        Builder builder = new Builder();
        String url = properties.getProperty(URL);
        if (url != null) {
            builder.url(url);
        }
        String scheme = builder.url != null ? builder.url.getScheme() : null;

        for (String key : properties.stringPropertyNames()) {
            if (!key.startsWith(PREFIX)) {
                continue;
            }
            String remainder = key.substring(PREFIX.length());
            String value = properties.getProperty(key);
            switch (key) {
                case URL -> { /* already applied */ }
                case CLIENT_ID -> builder.clientId(value);
                case CONCURRENCY -> builder.concurrency(parseInt(key, value));
                case CONNECT_TIMEOUT -> builder.connectTimeout(parseDuration(key, value));
                case CLOSE_TIMEOUT -> builder.closeTimeout(parseDuration(key, value));
                default -> {
                    int dot = remainder.indexOf('.');
                    if (dot < 0) {
                        throw new MessagingException("Unknown configuration key: " + key);
                    }
                    if (remainder.substring(0, dot).equals(scheme)) {
                        builder.property(remainder.substring(dot + 1), value);
                    }
                }
            }
        }
        return builder.build();
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new MessagingException("Invalid integer for " + key + ": " + value, e);
        }
    }

    private static Duration parseDuration(String key, String value) {
        try {
            return Duration.parse(value.trim());
        } catch (DateTimeParseException e) {
            throw new MessagingException("Invalid ISO-8601 duration for " + key + ": " + value, e);
        }
    }

    public URI url() {
        return url;
    }

    public String scheme() {
        return scheme;
    }

    public String clientId() {
        return clientId;
    }

    public int concurrency() {
        return concurrency;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration closeTimeout() {
        return closeTimeout;
    }

    public MessagingListener listener() {
        return listener;
    }

    /** Returns transport-specific properties passed through to the transport implementation. */
    public Map<String, String> transportProperties() {
        return passthroughProperties;
    }

    public Map<String, String> passthroughProperties() {
        return passthroughProperties;
    }

    @Override
    public String toString() {
        return "MessagingConfig[url=" + redactUserInfo(url)
            + ", clientId=" + clientId
            + ", concurrency=" + concurrency
            + ", connectTimeout=" + connectTimeout
            + ", closeTimeout=" + closeTimeout
            + ", passthroughProperties={" + redactedProperties() + "}]";
    }

    private static String redactUserInfo(URI uri) {
        if (uri.getUserInfo() == null) {
            return uri.toString();
        }
        return uri.toString().replace(uri.getUserInfo() + "@", "***@");
    }

    private String redactedProperties() {
        return passthroughProperties.entrySet().stream()
            .map(e -> e.getKey() + "=" + (isSensitive(e.getKey()) ? "***" : e.getValue()))
            .collect(java.util.stream.Collectors.joining(", "));
    }

    private static boolean isSensitive(String key) {
        String lower = key.toLowerCase();
        return lower.contains("password") || lower.contains("secret");
    }

    public static final class Builder {
        private URI url;
        private String clientId;
        private int concurrency = 1;
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration closeTimeout = Duration.ofSeconds(30);
        private MessagingListener listener = MessagingListener.noOp();
        private final Map<String, String> passthroughProperties = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder url(String url) {
            Objects.requireNonNull(url, "url must not be null");
            URI uri = URI.create(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                throw new IllegalArgumentException("url must be scheme://host[:port], got: " + url);
            }
            this.url = uri;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = Objects.requireNonNull(clientId, "clientId must not be null");
            return this;
        }

        public Builder concurrency(int concurrency) {
            if (concurrency < 1) {
                throw new IllegalArgumentException("concurrency must be >= 1, got: " + concurrency);
            }
            this.concurrency = concurrency;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout must not be null");
            return this;
        }

        public Builder closeTimeout(Duration closeTimeout) {
            this.closeTimeout = Objects.requireNonNull(closeTimeout, "closeTimeout must not be null");
            return this;
        }

        public Builder listener(MessagingListener listener) {
            this.listener = listener;
            return this;
        }

        public Builder property(String key, String value) {
            passthroughProperties.put(
                Objects.requireNonNull(key, "property key must not be null"),
                Objects.requireNonNull(value, "property value must not be null"));
            return this;
        }

        public MessagingConfig build() {
            if (url == null) {
                throw new MessagingException("messaging.url is required");
            }
            return new MessagingConfig(this);
        }
    }
}
