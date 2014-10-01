package net.bluemud.tcp.select;

import java.nio.channels.SocketChannel;

/**
 */
public interface Connection {

    SocketChannel getChannel();



    /**
     * Called by the selector when the channel is readable
     * @return {@code true} if the connection will read, {@code false} if not (e.g. no read buffer available).
     */
    boolean readable();

    /**
     * Called by the selector when the channel is writable
     * @return {@code true} if the connection will write, {@code false} if not (e.g. nothing to write).
     */
    boolean writable();

    /**
     * Called by the selector if the connection is closed
     * @param ex the exception, if any, that caused the closure.
     */
    void closed(Exception ex);
}
