package com.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

/** A ConnectionFactory whose class loads fine but whose public no-arg constructor always
 * throws, used to test ConnectionFactoryBuilder's instantiation-failure branch. */
public class StubConnectionFactoryThrowingConstructor implements ConnectionFactory {

    public StubConnectionFactoryThrowingConstructor() {
        throw new IllegalStateException("boom: constructor always fails");
    }

    @Override public Connection createConnection() { return null; }
    @Override public Connection createConnection(String userName, String password) { return null; }
    @Override public JMSContext createContext() { return null; }
    @Override public JMSContext createContext(String userName, String password) { return null; }
    @Override public JMSContext createContext(String userName, String password, int sessionMode) { return null; }
    @Override public JMSContext createContext(int sessionMode) { return null; }
}
