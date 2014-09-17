package net.bluemud.tcp.api;

/**
 */
public interface ConnectionProcessor extends ConnectionReader, ConnectionWriter {
    void connectionClosed(Exception ex);
}
