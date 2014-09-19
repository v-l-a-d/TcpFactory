package net.bluemud.tcp.api;

import java.nio.ByteBuffer;

/**
 * Implemented by classes writing to a {@link net.bluemud.tcp.ConnectionLeg}.
 */
public interface ConnectionWriter {

    /**
     * Called on completion of a write.
     * @param buffer the buffer that was readComplete (now available for re-use)
     */
    void writeComplete(ByteBuffer buffer);
}
