package net.bluemud.tcp.internal;

import net.bluemud.tcp.internal.BufferReader;
import net.bluemud.tcp.internal.RingByteBuffer;
import org.junit.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class RingByteBufferTest {

	@Test
	public void remaining() throws Exception {
		RingByteBuffer ring = new RingByteBuffer(10);
		assertThat(ring.remaining(), is(10));
		assertThat(ring.available(), is(0));


		ByteBuffer buffer = ring.getEmptyBuffer(); // T=0, DT=0, H=0
		assertThat(ring.remaining(), is(0));
		assertThat(buffer.position(), is(0));
		assertThat(buffer.limit(), is(10));
		assertThat(buffer.capacity(), is(10));
		assertThat(ring.available(), is(0));

		ByteBuffer buffer2 = ring.getEmptyBuffer(); // T=0, DT=0, H=0
		assertThat(buffer2.remaining(), is(0));
		assertThat(ring.remaining(), is(0));
		assertThat(buffer2.capacity(), is(10));
		assertThat(ring.available(), is(0));

		buffer.put(new byte[5]);
		ring.readComplete(buffer);                  // T=0, DT=5, H=5
		assertThat(ring.remaining(), is(5));
		assertThat(ring.available(), is(5));

		InputStream instr = ring.getInputStream();
		instr.read();                        // T=1, DT=5, H=5
		assertThat(ring.remaining(), is(6));
		assertThat(ring.available(), is(4));
		instr.read(new byte[4]);             // T=5, DT=5, H=5
		assertThat(ring.remaining(), is(10));
		assertThat(ring.available(), is(0));

		buffer2 = ring.getEmptyBuffer();
		buffer2.put(new byte[3]);
		ring.readComplete(buffer2);               // T=5, DT=8, H=8
		assertThat(ring.remaining(), is(7));
		assertThat(ring.available(), is(3));

		ByteBuffer buffer3 = ring.getEmptyBuffer(); // T=5, DT=8, H=0
		assertThat(buffer3.remaining(), is(2)); // end of the underlying array
		assertThat(ring.remaining(), is(0));
		assertThat(ring.available(), is(3));    // overrun case

		buffer3.put(new byte[2]);
		ring.readComplete(buffer3);              // T=5, DT=0, H=0
		assertThat(ring.available(), is(5));
		assertThat(ring.remaining(), is(5));

		instr.read(new byte[5]);           // T=0, DT=0, H=0
		assertThat(ring.available(), is(0));
		assertThat(ring.remaining(), is(10));
	}

	@Test
	public void lapping() throws Exception {
		RingByteBuffer ring = new RingByteBuffer(10);
		ByteBuffer buffer1 = ring.getEmptyBuffer();
		buffer1.put(new byte[10]);
		ring.readComplete(buffer1);

		assertThat(ring.available(), is(10));
		assertThat(ring.remaining(), is(0));
		assertThat(ring.getEmptyBuffer().remaining(), is(0));
	}

    @Test
    public void threaded() throws Exception {

		final int data_size = 2048 + (int)(Math.random() * (12*1024));
        final RingByteBuffer ring = new RingByteBuffer(512, new BufferReader() {
            @Override
            public void readBufferAvailable() {
            }
        });

        final AtomicInteger received = new AtomicInteger(0);

        final Thread reader = new Thread() {
            @Override
            public void run() {
                InputStream inStr = ring.getInputStream();
                int bytesRead = 0;
                try {
                    int val = inStr.read();
                    while (val != 23) {
                        bytesRead++;
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
        while (bytesWritten < data_size) {
            ByteBuffer buffer = ring.getEmptyBuffer();

            if (buffer.remaining() > 0) {
				int writeAmount = Math.max(1, Math.min((int)(Math.random() * 256), data_size - bytesWritten));
				writeAmount = Math.min(writeAmount, buffer.remaining());
                bytesWritten += writeAmount;

                // Fill the buffer
                buffer.put(new byte[writeAmount]);

                // Pass it back.
                ring.readComplete(buffer);
            } else {
                // No capacity - pause
                Thread.sleep(10);
            }
        }

        assertThat(bytesWritten, is(data_size));

        // Write the poison pill.
        ByteBuffer buffer = ring.getEmptyBuffer();
		while (!buffer.hasRemaining()) {
			Thread.sleep(50);
			buffer = ring.getEmptyBuffer();
		}
        buffer.put((byte)23);
        ring.readComplete(buffer);

        reader.join(1000);
        assertThat(received.get(), is(data_size));
        System.out.println("Bashed " + data_size + " bytes");
    }

    @Test
    public void repeater() throws Exception {
        for (int ii = 0; ii < 500; ii++) {
            threaded2();
        }
    }

    @Test
    public void threaded2() throws Exception {

        final int data_size = 2048 + (int)(Math.random() * (12*1024));
        final LinkedBlockingQueue<Boolean> canWriteQ = new LinkedBlockingQueue<Boolean>();

        final RingByteBuffer ring = new RingByteBuffer(512, new BufferReader() {
            @Override
            public void readBufferAvailable() {
                canWriteQ.add(Boolean.TRUE);
            }
        });

        final AtomicInteger received = new AtomicInteger(0);

        final Thread reader = new Thread() {
            @Override
            public void run() {
                InputStream inStr = ring.getInputStream();
                int bytesRead = 0;
                try {
                    int val = inStr.read();
                    while (val != 23) {
                        bytesRead++;
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
        while (bytesWritten < data_size) {
            ByteBuffer buffer = ring.getEmptyBuffer();

            if (buffer.remaining() > 0) {
                int writeAmount = Math.max(1, Math.min((int)(Math.random() * 256), data_size - bytesWritten));
                writeAmount = Math.min(writeAmount, buffer.remaining());
                bytesWritten += writeAmount;

                // Fill the buffer
                buffer.put(new byte[writeAmount]);

                // Pass it back.
                ring.readComplete(buffer);
            } else if (ring.remaining() == 0) {
                // No capacity - pause
                ring.readComplete(buffer);
                canWriteQ.take();
            }
        }

        assertThat(bytesWritten, is(data_size));

        // Write the poison pill.
        ByteBuffer buffer = ring.getEmptyBuffer();
        while (!buffer.hasRemaining()) {
            Thread.sleep(50);
            buffer = ring.getEmptyBuffer();
        }
        buffer.put((byte)23);
        ring.readComplete(buffer);

        reader.join(1000);
        assertThat(received.get(), is(data_size));
        System.out.println("Bashed " + data_size + " bytes");
    }

}