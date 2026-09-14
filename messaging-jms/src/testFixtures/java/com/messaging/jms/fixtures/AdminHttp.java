package com.messaging.jms.fixtures;

import com.messaging.MessagingException;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.IntPredicate;

/** Shared HTTP send/validate/wrap-exception logic for the admin fixtures — each provider's
 * request shape differs, but "send, check status, wrap failures in a MessagingException
 * naming the call" does not. */
final class AdminHttp {
    private AdminHttp() {}

    static String send(HttpClient client, HttpRequest request, IntPredicate statusOk, String what) {
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (!statusOk.test(response.statusCode())) {
                throw new MessagingException(what + " failed: HTTP " + response.statusCode() + " " + response.body());
            }
            return response.body();
        } catch (MessagingException e) {
            throw e;
        } catch (Exception e) {
            throw new MessagingException(what + " failed", e);
        }
    }
}
