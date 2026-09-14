package com.messaging.jms;

import com.messaging.MessagingException;
import com.messaging.config.MessagingConfig;
import jakarta.jms.ConnectionFactory;

import java.lang.reflect.Method;
import java.util.Map;

/** Builds a provider's {@link ConnectionFactory} purely from {@code messaging.jms.*}
 * passthrough config: {@code connection-factory} names the class by FQCN, and every other
 * key is applied as a public single-argument bean setter (§I amendment). No provider
 * class or constant is referenced anywhere in this adapter. */
final class ConnectionFactoryBuilder {

    private static final String CONNECTION_FACTORY_KEY = "connection-factory";

    private ConnectionFactoryBuilder() {}

    static ConnectionFactory build(MessagingConfig config) {
        Map<String, String> props = config.passthroughProperties();
        String className = props.get(CONNECTION_FACTORY_KEY);
        if (className == null) {
            throw new MessagingException("Missing required key 'messaging.jms." + CONNECTION_FACTORY_KEY + "'");
        }
        ConnectionFactory factory = instantiate(className);
        for (var entry : props.entrySet()) {
            if (entry.getKey().equals(CONNECTION_FACTORY_KEY)) continue;
            applySetter(factory, className, entry.getKey(), entry.getValue());
        }
        return factory;
    }

    private static ConnectionFactory instantiate(String className) {
        try {
            Class<?> factoryClass = Class.forName(className);
            Object instance = factoryClass.getDeclaredConstructor().newInstance();
            if (!(instance instanceof ConnectionFactory factory)) {
                throw new MessagingException("Class named by 'messaging.jms." + CONNECTION_FACTORY_KEY
                    + "' (" + className + ") does not implement jakarta.jms.ConnectionFactory");
            }
            return factory;
        } catch (ReflectiveOperationException e) {
            throw new MessagingException("Cannot instantiate connection factory class named by key '"
                + CONNECTION_FACTORY_KEY + "': " + className, e);
        }
    }

    private static void applySetter(ConnectionFactory factory, String className, String key, String value) {
        if (key.equals("deliveryMode") && (value.equals("NON_PERSISTENT") || value.equals("1"))) {
            throw new MessagingException("messaging.jms.deliveryMode=" + value + " is rejected: this "
                + "library enforces PERSISTENT delivery (§E) and will not let a config file weaken it");
        }
        String setterName = "set" + Character.toUpperCase(key.charAt(0)) + key.substring(1);
        Method setter = findSetter(factory.getClass(), setterName);
        if (setter == null) {
            throw new MessagingException("No public setter '" + setterName + "' on " + className
                + " for key 'messaging.jms." + key + "'");
        }
        Object coerced = coerce(key, setter.getParameterTypes()[0], value);
        try {
            setter.invoke(factory, coerced);
        } catch (ReflectiveOperationException e) {
            throw new MessagingException("Failed to apply 'messaging.jms." + key + "' via " + setterName, e);
        }
    }

    private static Method findSetter(Class<?> factoryClass, String setterName) {
        for (Method m : factoryClass.getMethods()) {
            if (m.getName().equals(setterName) && m.getParameterCount() == 1) return m;
        }
        return null;
    }

    private static Object coerce(String key, Class<?> paramType, String value) {
        try {
            if (paramType == String.class) return value;
            if (paramType == int.class || paramType == Integer.class) return Integer.parseInt(value);
            if (paramType == long.class || paramType == Long.class) return Long.parseLong(value);
            if (paramType == boolean.class || paramType == Boolean.class) return Boolean.parseBoolean(value);
        } catch (NumberFormatException e) {
            throw new MessagingException("Cannot coerce 'messaging.jms." + key + "'=" + value
                + " to " + paramType.getSimpleName(), e);
        }
        throw new MessagingException("Unsupported setter parameter type " + paramType.getSimpleName()
            + " for key 'messaging.jms." + key + "'");
    }
}
