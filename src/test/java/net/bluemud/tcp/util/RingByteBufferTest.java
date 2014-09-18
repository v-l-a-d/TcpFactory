package net.bluemud.tcp.util;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;

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
}