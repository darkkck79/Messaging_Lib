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
        String body = "{\"type\":\"runCommand\",\"parameters\":{\"command\":\"" + escape(command) + "\"}}";
        HttpRequest request = HttpRequest.newBuilder(restBase.resolve("/api/v2/admin/action/qmgr/QM1/mqsc"))
            .header("Content-Type", "application/json")
            .header("ibm-mq-rest-csrf-token", "value")
            .header("Authorization", authHeader)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        AdminHttp.send(client, request, status -> status / 100 == 2, "IBM MQ mqsc call for command: " + command);
    }

    private static String escape(String s) { return s.replace("\"", "\\\""); }
}
