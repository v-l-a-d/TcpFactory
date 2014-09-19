package net.bluemud.tcp;

import com.google.common.io.Closeables;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.ConnectionProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A connection leg, either inbound or outbound. A leg is always one of a pair.
 */
class ConnectionLeg implements Connection {
    private final static Logger LOG = LoggerFactory.getLogger(ConnectionLeg.class);

    private final SocketChannel channel;
    private final SelectionKey key;
	private final SelectorThread selectorThread;
	private final Queue<ByteBuffer> writeQueue;

	private volatile ConnectionProcessor processor;

	/**
     * Constructor for received socket channel
     * @param channel
     */
    ConnectionLeg(SocketChannel channel, SelectionKey key, SelectorThread selectorThread) {
        this.channel = channel;
        this.key = key;
		this.selectorThread = selectorThread;
		this.writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
    }

	@Override
	public void write(ByteBuffer buffer) {
		this.writeQueue.offer(buffer);

		if (this.channel.isConnected()) {
			this.selectorThread.addPendingWrite(this);
		}
	}

	@Override
	public void readBufferAvailable() {
		this.selectorThread.addReadReady(this);
	}

	@Override
	public void setProcessor(ConnectionProcessor processor) {
		this.processor = processor;
	}

	@Override
    public InetSocketAddress getLocalAddress() {
		return (InetSocketAddress)this.channel.socket().getLocalSocketAddress();
	}

	@Override
	public InetSocketAddress getRemoteAddress() {
		return (InetSocketAddress)this.channel.socket().getRemoteSocketAddress();
	}

	SocketChannel getChannel() {
        return this.channel;
    }

	void enableRead() {
		this.key.interestOps(this.key.interestOps() | SelectionKey.OP_READ);
	}

    /**
     * Read from the channel
     */
    void read() {
        if (processor != null && this.channel.isConnected()) {
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

					// Return the buffer.
                    processor.readComplete(buffer);
                }
                catch (IOException iox) {
                    close(iox);
                }
            }
        }
    }

    /**
     * Write pending data to the socket channel
     * @return {@code true} if all data in the buffer is readComplete, otherwise {@code false}
     */
    boolean write() {
        if (!channel.isConnected()) {
            return false;
        }

        // Write the buffer to the socket channel.
        try {
			ByteBuffer buffer = writeQueue.peek();
			while (buffer != null) {
				channel.write(buffer);

				if (buffer.remaining() != 0) {
					// Did not write the entire buffer - break out of the loop. Update the selection key set in order to
					// be notified by the selector when the socket becomes writable.
					LOG.debug("wrote part buffer");
					this.key.interestOps(this.key.interestOps() | SelectionKey.OP_WRITE);
					return false;
				} else {
					// Remove readComplete buffer from queue
					writeQueue.poll();

					if (processor != null) {
						// Notify processor of write completion, returning the buffer.
						this.processor.writeComplete(buffer);
					}
				}

				// Try writing the next buffer.
				buffer = writeQueue.peek();
			}

			// Wrote all buffers - update the selection key's interest set to remove write operations.
			key.interestOps(this.key.interestOps() & SelectionKey.OP_READ);
			return true;
		}
        catch (IOException iox) {
            close(iox);
        }
        return true;
    }

	public void close() {
		Closeables.closeQuietly(this.channel);
	}

    void close(Exception ex) {
        LOG.debug("Closing connection to {} with exception {}", channel.socket().getRemoteSocketAddress(), ex);
        Closeables.closeQuietly(this.channel);

		if (processor != null) {
			processor.connectionClosed(ex);
		}
    }
}
