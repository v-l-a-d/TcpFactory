package net.bluemud.tcp.util;

import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class OutputRingByteBufferTest  {

	@Test
	public void threaded() throws Exception {

		final BlockingQueue<ByteBuffer> returnQueue = new LinkedBlockingDeque<ByteBuffer>();
        final BufferWriter writer = new BufferWriter() {
			@Override
			public void write(ByteBuffer buffer) {
				// Add it to the return queue.
				if (!buffer.hasRemaining()) {
					System.out.println("Zero length buffer!");
				}
				returnQueue.add(buffer);
			}
		};


		final int data_size = 2048 + (int)(Math.random() * (12*1024));
		final OutputRingByteBuffer ring = new OutputRingByteBuffer(512, writer);
		final AtomicInteger written = new AtomicInteger(0);

		final Thread writeThread = new Thread() {
			@Override
			public void run() {
				int bytesWritten = 0;
				try {
					ByteBuffer buffer = returnQueue.take();
				    while (buffer.remaining() != 0) {
						// 'Write' the buffer contents
						bytesWritten += buffer.remaining();
						buffer.get(new byte[buffer.remaining()]);

						// Return it
						ring.writeComplete(buffer);
						buffer = returnQueue.take();
					}
				} catch (InterruptedException e) {
					System.out.println("Error " + e);
				} finally {
					written.set(bytesWritten);
				}
			}
		};

		writeThread.start();

		// Loop writing bytes to the ring buffer
		OutputStream stream = ring.getOutputStream();
		int bytesWritten = 0;
		while (bytesWritten < data_size) {
			int writeAmount = Math.max(1, Math.min((int)(Math.random() * 256), data_size - bytesWritten));
			bytesWritten += writeAmount;

			stream.write(new byte[writeAmount]);

		}
		stream.flush();
		assertThat(bytesWritten, is(data_size));

		// Stop the writer
		writeThread.interrupt();
		writeThread.join(1000);
		assertThat(written.get(), is(data_size));
        System.out.println("Bashed " + data_size + "bytes");
	}

	@Test
	public void repeater() throws Exception {
		for (int ii = 0; ii < 1000; ii++) {
			threaded();
		}
	}
}