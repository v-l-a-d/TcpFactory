package net.bluemud.tcp.util;

import java.io.IOException;
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
class OutputRingByteBuffer {

	// The backing byte array
	private final byte[] array;
	private final OutputStream writeStream;
	private final BufferWriter writer;

	// This pointers will wrap after writing / reading 16k petabytes of data.
	private volatile long virtual_head = 0L;
	private volatile long virtual_data = 0L;
	private volatile long virtual_tail = 0L;

	// Lock and condition used to signal space availability
	private final ReentrantLock writeNotificationLock;
	private final Condition spaceAvailableForWrite;

	public OutputRingByteBuffer(int capacity, BufferWriter writer) {
		this.array = new byte[capacity];
		this.writeStream = new RingOutputStream();
		this.writer = writer;

		writeNotificationLock = new ReentrantLock();
		spaceAvailableForWrite = writeNotificationLock.newCondition();
	}

	/**
	 * Returns a ByteBuffer wrapping the portion of the underlying array containing data. If the data portion wraps the end of the array, then the
	 * returned buffer will only represent the part of the data before the end of the array. Another call to {@link #getDataBuffer()} will return the
	 * remaining data.
	 * <p>
	 * The returned buffer 'owns' the section of underlying data in the array.
	 * <p>
	 * This method may be called repeatedly without intervening calls to {@link #writeComplete(java.nio.ByteBuffer)} - but buffers MUST be returned in the
	 * same order that they were dispensed.
	 *
	 * @return {@code ByteBuffer}
	 */
	ByteBuffer getDataBuffer() {
		int buffer_length = available();
		int data_pos = (int)(virtual_data % array.length);

		if ((data_pos + buffer_length) > array.length) {
			// Cannot wrap over the end of the underlying array - return a truncated buffer.
			buffer_length = array.length - data_pos;
		}

		// Set the virtual tail - the next call to getDataBuffer() will return the next data block (if there is one).
		virtual_data += buffer_length;
		return ByteBuffer.wrap(array, data_pos, buffer_length);
	}

	void writeComplete(ByteBuffer buffer) {
		// Update the tail - can free up data that has been written.
		boolean wasFull = remaining() == 0;
		int tail_pos = (int)(virtual_tail % array.length);
		virtual_tail += buffer.position() - tail_pos;

		if (wasFull) {
			// Notify space available
			writeNotificationLock.lock();
			try {
				spaceAvailableForWrite.signal();
			} finally {
				writeNotificationLock.unlock();
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
		return (int)(virtual_head - virtual_data);
	}

	public OutputStream getOutputStream() {
		return writeStream;
	}

	/**
	 * An output stream that writes to the empty portion of the ring.
	 */
	private class RingOutputStream extends OutputStream {
		@Override
		public void write(int b) throws IOException {
			while(remaining() == 0) {
				// Try to flush the buffer
				flush();

				// Wait for space to become available
				writeNotificationLock.lock();
				try {
					spaceAvailableForWrite.await(10, TimeUnit.MILLISECONDS);
				} catch (InterruptedException ix) {
					throw new IOException("Read interrupted", ix);
				} finally {
					writeNotificationLock.unlock();
				}
			}

			array[(int)(virtual_head % array.length)] = (byte)(b & 0xFF);
			virtual_head++;
		}

		@Override
		public void flush() throws IOException {
			writer.write(getDataBuffer());
			if (available() > 0) {
				// When wrapping over the nd of the array, not all available data will be returned in the data buffer.
				// Call again to get the complete data.
				ByteBuffer buffer = getDataBuffer();
				if (buffer.remaining() > 0) {
					writer.write(buffer);
				}
			}
		}
	}
}
