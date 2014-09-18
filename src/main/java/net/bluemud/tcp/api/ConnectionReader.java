package net.bluemud.tcp.api;

import java.nio.ByteBuffer;

/**
 * Implemented by classes reading from tcp connections
 */
public interface ConnectionReader {

    /**
     * Called by a {@link net.bluemud.tcp.ConnectionLeg} to obtain a buffer for receiving data. The buffer is returned
     * on a subsequent call to {@link #readComplete(java.nio.ByteBuffer)}.
     * <p>
     * This method must not block.
     *
     * @return a {@code ByteBuffer} ready to receive the data, if {@code null} or a buffer with no capacity is returned
     *   there will be no further calls to read data, until the connection is notified by a call to {@link Connection#readBufferAvailable()}.
     */
    ByteBuffer getReadBuffer();

    /**
     *  Called by a {@link net.bluemud.tcp.ConnectionLeg} when it has finished reading data into the specified buffer.
     *  <p>
     *  This method must not block.
     *
     *  @param buffer buffer containing read data
     */
    void readComplete(ByteBuffer buffer);
}
