package com.messaging.jms.fixtures;

import com.messaging.Destination;
import com.messaging.MessagingException;
import com.messaging.Queue;
import com.messaging.Topic;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
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
        HttpRequest.Builder builder = HttpRequest.newBuilder(jolokiaBase.resolve(jolokiaBase.getPath() + path))
            .header("Origin", jolokiaBase.getScheme() + "://" + jolokiaBase.getHost() + ":" + jolokiaBase.getPort())
            .GET();
        if (authHeader != null) builder.header("Authorization", authHeader);
        return AdminHttp.send(client, builder.build(), status -> status == 200, "Jolokia call to " + path);
    }
}
