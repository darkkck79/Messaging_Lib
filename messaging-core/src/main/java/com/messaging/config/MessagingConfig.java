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
        this.listener = b.listener != null ? b.listener : new MessagingListener() {};
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
