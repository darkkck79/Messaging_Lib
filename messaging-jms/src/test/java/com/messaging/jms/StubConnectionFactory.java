package com.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

/** A minimal ConnectionFactory used only to test ConnectionFactoryBuilder without a real broker. */
public class StubConnectionFactory implements ConnectionFactory {
    String brokerUrl;
    int port;
    long timeout;
    boolean useSsl;

    public void setBrokerURL(String url) { this.brokerUrl = url; }
    public void setPort(int port) { this.port = port; }
    public void setTimeout(long timeout) { this.timeout = timeout; }
    public void setUseSsl(boolean useSsl) { this.useSsl = useSsl; }

    @Override public Connection createConnection() { return null; }
    @Override public Connection createConnection(String userName, String password) { return null; }
    @Override public JMSContext createContext() { return null; }
    @Override public JMSContext createContext(String userName, String password) { return null; }
    @Override public JMSContext createContext(String userName, String password, int sessionMode) { return null; }
    @Override public JMSContext createContext(int sessionMode) { return null; }
}
