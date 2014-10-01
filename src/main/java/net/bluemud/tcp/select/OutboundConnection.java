package net.bluemud.tcp.select;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 */
public interface OutboundConnection extends Connection {

    InetSocketAddress getRemoteAddress();

    /**
     * Called by the selector upon connection.
     */
    void connected(SocketChannel channel);
}
