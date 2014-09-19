package net.bluemud.tcp.util;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RingByteBufferTest {

	@Test
	public void remaining() throws Exception {
		RingByteBuffer ring = new RingByteBuffer(10);
		assertThat(ring.remaining(), is(10));

		ByteBuffer buffer = ring.allocate(5);
		assertThat(ring.remaining(), is(5));
		assertThat(buffer.position(), is(0));
		assertThat(buffer.limit(), is(5));
		assertThat(buffer.capacity(), is(10));
		assertThat(ring.available(), is(0));

		ByteBuffer buffer2 = ring.allocate(3);
		assertThat(ring.remaining(), is(2));
		assertThat(buffer2.position(), is(5));
		assertThat(buffer2.limit(), is(8));
		assertThat(buffer2.capacity(), is(10));
		assertThat(ring.available(), is(0));

		buffer.put(new byte[5]);
		ring.filled(buffer);
		assertThat(ring.remaining(), is(2));
		assertThat(ring.available(), is(5));

		InputStream instr = ring.getInputStream();
		instr.read();
		assertThat(ring.remaining(), is(3));
		assertThat(ring.available(), is(4));
		instr.read(new byte[4]);
		assertThat(ring.remaining(), is(7));
		assertThat(ring.available(), is(0));

		buffer2.put(new byte[3]);
		ring.filled(buffer2);
		assertThat(ring.remaining(), is(7));
		assertThat(ring.available(), is(3));

		ByteBuffer buffer3 = ring.allocate(7);
		assertThat(buffer3.remaining(), is(2)); // end of the underlying array
		assertThat(ring.remaining(), is(5));
		assertThat(ring.available(), is(3));
	}

    @Test
    public void threaded() throws Exception {
        final RingByteBuffer ring = new RingByteBuffer(512);
        final AtomicInteger received = new AtomicInteger(0);

        final Thread reader = new Thread() {
            @Override
            public void run() {
                InputStream inStr = ring.getInputStream();
                int bytesRead = 0;
                byte[] buff = new byte[128];
                try {
//                    int read = inStr.read(buff);
//                    byte val = buff[read -1];
                    int val = inStr.read();
                    while (val != 23) {
                        bytesRead ++;

//                        if (bytesRead >= 2048) {
//                            break;
//                        }
//                        read = inStr.read(buff);
//                        val = buff[read -1];
                        val = inStr.read();
                    }
                } catch (Exception e) {
                    System.out.println("Error " + e);
                } finally {
                    received.set(bytesRead);
                }
            }
        };

        reader.start();

        // Loop writing bytes to the ring buffer ByteBuffers
        int bytesWritten = 0;
        while (bytesWritten < 2048) {
            int writeAmount = Math.max(1, Math.min((int)(Math.random() * 256), 2048 - bytesWritten));
            ByteBuffer buffer = ring.allocate(writeAmount);

            if (buffer != null) {
                System.out.println("position: " + buffer.position() + " limit: " + buffer.limit() + " remaining: " + buffer.remaining());
                bytesWritten += buffer.remaining();

                // Fill the buffer
                buffer.put(new byte[buffer.remaining()]);

                // Pass it back.
                ring.filled(buffer);
            } else {
                // No capacity - wait
                System.out.println("No write buffer");
                Thread.sleep(250);
            }
        }

        System.out.println("Bytes written " + bytesWritten);

        // Write the poison pill.
        ByteBuffer buffer = ring.allocate(1);
        buffer.put((byte)23);
        ring.filled(buffer);

        reader.join(1000);
        assertThat(received.get(), is(2048));
    }
}