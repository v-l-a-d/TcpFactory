package net.bluemud.tcp;

import com.google.common.io.Closeables;
import net.bluemud.tcp.api.ConnectionProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * A connection leg, either inbound or outbound. A leg is always one of a pair.
 */
class ConnectionLeg {
    private final static Logger LOG = LoggerFactory.getLogger(ConnectionLeg.class);

    private final SocketChannel channel;
    private final SelectionKey key;
    private final ConnectionProcessor processor;

    /**
     * Constructor for received socket channel
     * @param channel
     */
    ConnectionLeg(SocketChannel channel, SelectionKey key, ConnectionProcessor processor) {
        this.channel = channel;
        this.key = key;
        this.processor = processor;
    }

    SocketChannel getChannel() {
        return this.channel;
    }

    /**
     * Read from the channel
     * @return {@code true} if all the read data is written
     * @throws java.io.IOException
     */
    boolean read() {
        boolean complete = false;

        if (this.channel.isConnected()) {
            // Obtain a read buffer
            ByteBuffer buffer = processor.getReadBuffer();

            if ((buffer == null) || (buffer.remaining() == 0)) {
                // Remove read registration - this can be re-instated by a call to readBufferAvailable.
                this.key.interestOps(this.key.interestOps() & SelectionKey.OP_WRITE);
            } else {
                try {
                    // Read data into the receive buffer.
                    int lread = this.channel.read(buffer);
                    if (lread == -1) {
                        // The socket has been closed by the far-end.
                        close(null);
                    }

                    // Flip the buffer and return it
                    buffer.flip();
                    processor.readComplete(buffer);
                }
                catch (IOException iox) {
                    close(iox);
                }
            }
        }
        return complete;
    }

    /**
     * Write pending data to the socket channel
     * @return {@code true} if all data in the buffer is written, otherwise {@code false}
     */
    boolean write(ByteBuffer buffer) {
        if (!channel.isConnected()) {
            return false;
        }

        // Write the buffer to the socket channel.
        try {
            channel.write(buffer);

            if (buffer.remaining() == 0) {
                // Wrote the entire buffer - set the selection key's interest set to read operations only.
                //System.out.println("wrote whole buffer");
                key.interestOps(SelectionKey.OP_READ);
                return true;
            } else {
                // Did not write the entire buffer - break out of the loop. Update the selection key set in order to
                // be notified by the selector when the socket becomes writable.
                LOG.debug("wrote part buffer");

                this.key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                return false;
            }
        }
        catch (IOException iox) {
            close(iox);
        }
        return true;
    }

    void close(Exception ex) {
        LOG.debug("Closing connection to {} with exception {}", channel.socket().getRemoteSocketAddress(), ex);
        Closeables.closeQuietly(this.channel);

        processor.connectionClosed(ex);
    }
}
