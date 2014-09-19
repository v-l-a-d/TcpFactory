package net.bluemud.tcp.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A circular byte buffer that allows portions of the buffer to be viewed / used as instances {@code ByteBuffer}
 *
 * Single reader and single writer only!
 */
public class RingByteBuffer {

	// The backing byte array
	private final byte[] array;
	private final InputStream readStream;

	private volatile int head = 0; // Write cursor
	private volatile int data_head = 0; // Data head cursor
	private volatile int tail = 0; // Read cursor
    private boolean headToTail = false;

	// Lock and condition used to signal data availability
	private final ReentrantLock notificationLock;
	private final Condition dataAvailableForRead;

	public RingByteBuffer(int capacity) {
		this.array = new byte[capacity];
		this.readStream = new RingInputStream();

		notificationLock = new ReentrantLock();
		dataAvailableForRead = notificationLock.newCondition();
	}

	/**
	 * Allocate a buffer of the specified capacity. The returned buffer may have a smaller capacity if the underlying ring buffer
	 * has wrapped (in the most degenerate case the returned buffer may have a capacity of just one byte).
	 * <p>
	 * If a buffer has been allocated, then another cannot be allocated until the previous buffer is returned - for example, if 2 buffers were
	 * allocated and the first was only partially filled, then there would be a gap in the underlying array- which would be a PITA.
	 *
	 * @param size - desired capacity
	 * @return a {@code ByteBuffer} with capacity up to the specified size.
	 */
	public ByteBuffer allocate(int size) {
		// Move the head ahead
        int pre_tail = tail == 0 ? array.length - 1 : tail - 1;

        int limit = (head >= tail) ? array.length  : pre_tail;

 		int oldHead = head;
		if (head + size >= limit) {
			// Will wrap - just return the remaining portion of the array - caller will have to make another
			// call to allocate another buffer.
			head = limit % array.length;
			return ByteBuffer.wrap(array, oldHead, (limit - oldHead));
		} else {
			// Return the reserved portion of the array as a ByteBuffer
			head = oldHead + size;
            return ByteBuffer.wrap(array, oldHead, size);
		}
	}

	/**
	 * Buffer that has had data written to it - and therefore into the underlying array.
	 * <p> Assumes the buffer has NOT been flipped</p>
	 * @param buffer returned buffer
	 */
	public void filled(ByteBuffer buffer) {
		// Update data head - data has been read in to the array.
		boolean wasEmpty = tail == data_head;
		data_head = buffer.position();

		if (wasEmpty) {
			notificationLock.lock();
			try {
				dataAvailableForRead.signal();
			} finally {
				notificationLock.unlock();
			}
		}
	}

	/**
	 * @return space remaining
	 */
	public int remaining() {
		// Available space in the buffer
		if (head >= tail) {
			return array.length - (head - tail);
		} else {
			return tail - head;
		}
	}

	/**
	 * @return amount of data available for read.
	 */
	public int available() {
		if (data_head >= tail) {
			return data_head - tail;
		} else {
			return array.length - tail + data_head;
		}
	}

	public InputStream getInputStream() {
		return readStream;
	}

	private class RingInputStream extends InputStream {
		@Override
		public int read() throws IOException {
			while (tail == data_head) {
				// nothing to read, wait for input
				notificationLock.lock();
				try {
					dataAvailableForRead.await();
				} catch (InterruptedException ix) {
					throw new IOException("Read interrupted", ix);
				} finally {
					notificationLock.unlock();
				}
			}

			// Read the tail value (this may be overwritten as soon as tail is updated)
			byte datum = array[tail];
			tail = (tail + 1) % array.length;
            if (tail >= head) {
                System.out.println("Tail:" + tail + " head: " + head);
            }
			return datum;
		}

        @Override
        public int available() {
            return RingByteBuffer.this.available();
        }
	}
}
