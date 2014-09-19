package net.bluemud.tcp.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A circular byte buffer that allows portions of the buffer to be viewed / used as instances {@code ByteBuffer}
 *
 * Single reader and single writer only!
 */
class RingByteBuffer {

	// The backing byte array
	private final byte[] array;
	private final InputStream readStream;

	// This pointers will wrap after writing / reading 16k petabytes of data.
	private volatile long virtual_head = 0L;
	private volatile long virtual_data = 0L;
	private volatile long virtual_tail = 0L;

	// Lock and condition used to signal data availability
	private final ReentrantLock readNotificationLock;
	private final Condition dataAvailableForRead;

	// Lock and condition used to signal space availability
	private final ReentrantLock writeNotificationLock;
	private final Condition spaceAvailableForWrite;

	public RingByteBuffer(int capacity) {
		this.array = new byte[capacity];
		this.readStream = new RingInputStream();

		readNotificationLock = new ReentrantLock();
		dataAvailableForRead = readNotificationLock.newCondition();
		writeNotificationLock = new ReentrantLock();
		spaceAvailableForWrite = writeNotificationLock.newCondition();
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
	public ByteBuffer getEmptyBuffer() {
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
	public void readComplete(ByteBuffer buffer) {
		// Update data head - data has been read in to the array.
		boolean wasEmpty = virtual_tail == virtual_data;
		int data_pos = (int)(virtual_data % array.length);
		virtual_data += buffer.position() - data_pos;
		virtual_head = virtual_data;

		if (wasEmpty) {
			// Notify data availability
			readNotificationLock.lock();
			try {
				dataAvailableForRead.signal();
			} finally {
				readNotificationLock.unlock();
			}
		}
	}

	/**
	 * @return space remaining for writing
	 */
	public int remaining() {
		// Available space in the buffer
		return array.length - (int)(virtual_head - virtual_tail);
	}

	/**
	 * @return amount of data available for read.
	 */
	public int available() {
		return (int)(virtual_data - virtual_tail);
	}

	public InputStream getInputStream() {
		return readStream;
	}

	/**
	 * An input stream that reads the portion of the ring buffer containing data.
	 */
	private class RingInputStream extends InputStream {
		@Override
		public int read() throws IOException {
			while (available() == 0) {
				// nothing to read, wait for input
				readNotificationLock.lock();
				try {
					dataAvailableForRead.await(10, TimeUnit.MILLISECONDS); // TODO timeout
				} catch (InterruptedException ix) {
					throw new IOException("Read interrupted", ix);
				} finally {
					readNotificationLock.unlock();
				}
			}

			// Read the tail value (this may be overwritten as soon as tail pointer is updated)
			byte datum = array[(int)(virtual_tail % array.length)];
			virtual_tail++;
			return datum;
		}

        @Override
        public int available() {
            return RingByteBuffer.this.available();
        }
	}
}
