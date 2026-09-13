package com.messaging;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The listener contract of §H: exactly four callbacks, every one a no-op default so an
 * application overrides only what it observes.
 *
 * <p>Reflection rather than direct calls, so a wrong method name shows up as a failed
 * assertion here instead of a compile error that takes the rest of the module's tests
 * down with it.
 */
class MessagingListenerTest {

    @Test void isInterface() {
        assertThat(MessagingListener.class.isInterface()).isTrue();
    }

    @Test void declaresExactlyTheFourSpecCallbacks() {
        assertThat(MessagingListener.class.getDeclaredMethods())
            .filteredOn(method -> !Modifier.isStatic(method.getModifiers()))
            .extracting(Method::getName)
            .containsExactlyInAnyOrder(
                "onPublished", "onConsumed", "onError", "onConnectionStateChanged");
    }

    @Test void onPublishedTakesDestination() throws Exception {
        Method method = MessagingListener.class.getDeclaredMethod("onPublished", Destination.class);
        assertThat(method.getReturnType()).isEqualTo(void.class);
    }

    @Test void onConsumedTakesDestination() throws Exception {
        Method method = MessagingListener.class.getDeclaredMethod("onConsumed", Destination.class);
        assertThat(method.getReturnType()).isEqualTo(void.class);
    }

    @Test void onErrorTakesDestinationAndThrowable() throws Exception {
        Method method =
            MessagingListener.class.getDeclaredMethod("onError", Destination.class, Throwable.class);
        assertThat(method.getReturnType()).isEqualTo(void.class);
    }

    @Test void onConnectionStateChangedTakesConnectionState() throws Exception {
        Method method = MessagingListener.class
            .getDeclaredMethod("onConnectionStateChanged", ConnectionState.class);
        assertThat(method.getReturnType()).isEqualTo(void.class);
    }

    @Test void everyCallbackIsADefaultMethod() {
        assertThat(MessagingListener.class.getDeclaredMethods())
            .filteredOn(method -> !Modifier.isStatic(method.getModifiers()))
            .allSatisfy(method -> assertThat(method.isDefault())
                .as("%s must be a default method", method.getName())
                .isTrue());
    }

    @Test void noCallbackDeclaresACheckedException() {
        assertThat(MessagingListener.class.getDeclaredMethods())
            .allSatisfy(method -> assertThat(method.getExceptionTypes())
                .as("%s must not declare checked exceptions", method.getName())
                .isEmpty());
    }

    @Test void emptyImplementationInheritsWorkingNoOps() {
        MessagingListener listener = new MessagingListener() {
        };
        Topic topic = Topic.of("orders");

        assertThatCode(() -> {
            invoke(listener, "onPublished", new Class<?>[] {Destination.class}, topic);
            invoke(listener, "onConsumed", new Class<?>[] {Destination.class}, topic);
            invoke(listener, "onError",
                new Class<?>[] {Destination.class, Throwable.class}, topic, new RuntimeException("boom"));
            invoke(listener, "onConnectionStateChanged",
                new Class<?>[] {ConnectionState.class}, ConnectionState.CONNECTED);
        }).doesNotThrowAnyException();
    }

    @Test void isImplementableAsASingleOverride() throws Exception {
        // A dynamic proxy that answers nothing proves no abstract method forces an override.
        Object proxy = Proxy.newProxyInstance(
            MessagingListener.class.getClassLoader(),
            new Class<?>[] {MessagingListener.class},
            (p, method, args) -> null);
        assertThat(proxy).isInstanceOf(MessagingListener.class);
        assertThat(Arrays.stream(MessagingListener.class.getDeclaredMethods())
            .filter(m -> Modifier.isAbstract(m.getModifiers()))
            .toList())
            .as("no abstract callbacks — all four are defaults")
            .isEmpty();
    }

    private static void invoke(MessagingListener listener, String name, Class<?>[] types, Object... args)
        throws Exception {
        MessagingListener.class.getDeclaredMethod(name, types).invoke(listener, args);
    }
}
