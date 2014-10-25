package net.bluemud.tcp.internal;

import com.google.common.io.Closeables;
import net.bluemud.tcp.api.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A connection leg, either inbound or outbound. A leg is always one of a pair.
 */
class ConnectionLeg implements Connection, BufferReader, BufferWriter {
    private final static Logger LOG = LoggerFactory.getLogger(ConnectionLeg.class);

    private final SocketChannel channel;
    private final SelectionKey key;
	private final SelectorThread selectorThread;
	private final Queue<ByteBuffer> writeQueue;

	private final RingByteBuffer inputBuffer;
	private final OutputRingByteBuffer outputBuffer;

	private volatile boolean closed;

	/**
     * Constructor
     */
    ConnectionLeg(SocketChannel channel, SelectionKey key, SelectorThread selectorThread) {
        this.channel = channel;
        this.key = key;
		this.selectorThread = selectorThread;
		this.writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();

		this.inputBuffer = new RingByteBuffer(32 * 1024, this);
		this.outputBuffer = new OutputRingByteBuffer(32 * 1024, this);
    }

	@Override
	public InputStream getInputStream() {
		return this.inputBuffer.getInputStream();
	}

	@Override
	public OutputStream getOutputStream() {
		return this.outputBuffer.getOutputStream();
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
    public InetSocketAddress getLocalAddress() {
		return (InetSocketAddress)this.channel.socket().getLocalSocketAddress();
	}

	@Override
	public InetSocketAddress getRemoteAddress() {
		return (InetSocketAddress)this.channel.socket().getRemoteSocketAddress();
	}

	@Override
	public void close() {
		closed = true;
		Closeables.closeQuietly(this.channel);
	}

	@Override
    public boolean isClosed() {
		return closed;
	}

	SocketChannel getChannel() {
        return this.channel;
    }


	///
	/// Methods below are ONLY called by the selector thread.
	///

	void enableRead() {
		this.key.interestOps(this.key.interestOps() | SelectionKey.OP_READ);
	}

    /**
     * Read from the channel
     */
    void read() {
        if (this.channel.isConnected()) {
            // Obtain a read buffer
            ByteBuffer buffer = inputBuffer.getEmptyBuffer();

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
                    inputBuffer.readComplete(buffer);
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


					// Return the buffer.
					this.outputBuffer.writeComplete(buffer);
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

    void close(Exception ex) {
        LOG.debug("Closing connection to {} with exception {}", channel.socket().getRemoteSocketAddress(), ex);
		closed = true;
        Closeables.closeQuietly(this.channel);

		// TODO notify connection closed?
    }
}
