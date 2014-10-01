package net.bluemud.tcp.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Adapter for a {@code SocketChannel}, exposing an {@code InputStream} api to the user and using a ring buffer internally to buffer input.
 */
public class ChannelInputAdapter extends InputStream {

	// The backing byte array.
	private final byte[] array;

	// The wrapped channel.
	private final SocketChannel channel;

	// These pointers will wrap after writing / reading 16k petabytes of data.
	private volatile long virtual_head = 0L;
	private volatile long virtual_data = 0L;
	private volatile long virtual_tail = 0L;

	// Lock and condition used to signal data availability
	private final ReentrantLock readNotificationLock;
	private final Condition dataAvailableForRead;

	ChannelInputAdapter(SocketChannel channel, int bufferSize) {
		this.channel = channel;
		this.array = new byte[bufferSize];

		readNotificationLock = new ReentrantLock();
		dataAvailableForRead = readNotificationLock.newCondition();
	}

	@Override
	public int read() throws IOException {
		while (available() == 0) {
			// Populate the buffer from the channel
			ByteBuffer emptyBuffer = getEmptyBuffer();
			int bytesRead = channel.read(emptyBuffer);
			readComplete(emptyBuffer);

			if (bytesRead == -1) {
				// Channel closed
				channel.close();
				throw new EOFException("Stream closed by remote endpoint");
			}

			if (bytesRead == 0) {
				// No data available - wait for read notification.
				readNotificationLock.lock();
				try {
					// TODO add timeout
					dataAvailableForRead.await();
				} catch (InterruptedException ex) {
					throw new IOException("Interrupt waiting for data", ex);
				} finally {
					readNotificationLock.unlock();
				}
			}
		}

		// Read the tail value (this may be overwritten as soon as tail pointer is updated)
		byte datum = array[(int)(virtual_tail % array.length)];
		virtual_tail++;

		return datum;
	}

	/**
	 * @return amount of buffered data available for immediate read.
	 */
	@Override
	public int available() {
		return (int)(virtual_data - virtual_tail);
	}

	@Override
	public void close() throws IOException {
		channel.close();
	}

	/**
	 * Callback from selector when data is available.
	 */
	public void dataAvailable() {
		readNotificationLock.lock();
		try {
			dataAvailableForRead.signal();
		} finally {
			readNotificationLock.unlock();
		}
	}

	/**
	 * Returns a view of the ring as a {@code ByteBuffer}. The returned buffer have less remaining space than reported by {@link #remaining()} if the
	 * underlying ring buffer has wrapped (in the most degenerate case the returned buffer may have a capacity of just one byte).
	 * <p>
	 * If a buffer has been returned, then another cannot be allocated until the previous buffer is returned - for example, if 2 buffers were
	 * allocated and the first was only partially readComplete, then there would be a gap in the underlying array- which would be a PITA.
	 *
	 * @return a {@code ByteBuffer}.
	 */
	private ByteBuffer getEmptyBuffer() {
		// Calculate the size of the buffer to return
		int buffer_length = remaining();
		int head_pos = (int)(virtual_head % array.length);

		// Set the virtual head - if there are further calls to this method before readComplete() is called, then a zero length buffer will be returned.
		virtual_head += buffer_length;

		if ((head_pos + buffer_length) > array.length) {
			// Cannot wrap over the end of the underlying array - return a truncated buffer.
			buffer_length = array.length - head_pos;
		}

		return ByteBuffer.wrap(array, head_pos, buffer_length);
	}

	/**
	 * Buffer that has had data readComplete to it - and therefore into the underlying array.
	 * <p> Assumes the buffer has NOT been flipped</p>
	 * @param buffer returned buffer
	 */
	private void readComplete(ByteBuffer buffer) {
		// Update data head - data has been read in to the array.
		boolean wasEmpty = virtual_tail == virtual_data;
		int data_pos = (int)(virtual_data % array.length);
		virtual_data += buffer.position() - data_pos;
		virtual_head = virtual_data;
	}

	/**
	 * @return space remaining for writing
	 */
	private int remaining() {
		// Available space in the buffer
		return array.length - (int)(virtual_head - virtual_tail);
	}
}

