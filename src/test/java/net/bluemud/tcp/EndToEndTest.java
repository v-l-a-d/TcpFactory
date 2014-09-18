package net.bluemud.tcp;

import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.ConnectionProcessor;
import net.bluemud.tcp.api.InboundConnectionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test sending / receiving TCP end-to-end
 */
public class EndToEndTest {

	private TcpFactory clientFactory;
	private TcpFactory serverFactory;
	private TestConnectionProcessor inboundProcessor;
	private volatile Connection inboundConnection;
	private TestConnectionProcessor clientProcessor;
	private Connection clientConnection;

	@Before
	public void setup() throws Exception {
		inboundProcessor = new TestConnectionProcessor();
		clientFactory = new TcpFactory();
		serverFactory = new TcpFactory(new InboundConnectionHandler() {
			@Override public ConnectionProcessor acceptConnection(Connection connection) {
				inboundConnection = connection;
				return inboundProcessor;
			}
		});

		// Start listening.
		serverFactory.listenOn(new InetSocketAddress("127.0.0.1", 11211));

		// Make a client connection to the listening address.
		clientProcessor = new TestConnectionProcessor();
		clientConnection = clientFactory.connectTo(clientProcessor, new InetSocketAddress("127.0.0.1", 11211));

		Thread.sleep(100);
		assertThat(inboundConnection, not(nullValue()));
	}

	@After
	public void cleanup() throws Exception {
		inboundConnection.close();
		clientFactory.shutdown();
		serverFactory.shutdown();
	}

	@Test
	public void readWriteTest() throws Exception {
		// Write some data on the client connection
		byte[] testData = "Some sort of test data string".getBytes();
		clientConnection.write(ByteBuffer.wrap(testData));

		// Expect data to be received at the server.
		ByteBuffer recv = inboundProcessor.recvQueue.poll(5, TimeUnit.SECONDS);
		byte[] recvData = new byte[recv.remaining()];
		recv.get(recvData);
		assertThat(new String(recvData), is("Some sort of test data string"));

		// Send something back again
		recv.clear();
		recv.put("Some kind of response text".getBytes());
		recv.flip();
		inboundConnection.write(recv);

		recv = clientProcessor.recvQueue.poll(5, TimeUnit.SECONDS);
		recvData = new byte[recv.remaining()];
		recv.get(recvData);
		assertThat(new String(recvData), is("Some kind of response text"));

		// Close the connection at the client end.
		clientConnection.close();
		Thread.sleep(100);
		assertThat(inboundProcessor.isClosed(), is(true));
	}

	@Test
	public void flowControl() throws Exception {
		// Simulate lack of read buffers on the server side.
		inboundProcessor.reading = false;

		// Write some buffers on the client side - until write buffers are no longer returned.
		for (int ii = 0; ii < 10; ii++) {
			byte[] data = new byte[5*1024];
			Arrays.fill(data, (byte)ii);

			ByteBuffer buffer = ByteBuffer.wrap(data);
			buffer.flip();
			clientProcessor.writeBuffers.add(buffer);
		}

		ByteBuffer buffer = clientProcessor.writeBuffers.poll(1, TimeUnit.SECONDS);
		int bytesWritten = 0;

		while (buffer != null) {
			buffer.position(0).limit(5*1024);
			clientConnection.write(buffer);
			bytesWritten += buffer.capacity();
			buffer = clientProcessor.writeBuffers.poll(1, TimeUnit.SECONDS);
		}

		// 50K sent to the server - it has not read any data
		assertThat(inboundProcessor.recvQueue.isEmpty(), is(true));

		// Set server to read again
		Thread.sleep(500);
		System.out.println("Re-enabling server reads");

		inboundProcessor.reading = true;
		inboundConnection.readBufferAvailable();

		int bytesRead = 0;
		ByteBuffer recv;
		do {
			recv = inboundProcessor.recvQueue.poll(5, TimeUnit.SECONDS);
			if (recv != null) {
				bytesRead += recv.remaining();
			}
		} while ((recv != null) && (bytesRead < bytesWritten));
		assertThat(bytesRead, is(bytesWritten));
	}

	class TestConnectionProcessor implements ConnectionProcessor {

		BlockingQueue<ByteBuffer> recvQueue = new LinkedBlockingQueue<ByteBuffer>();
		BlockingQueue<ByteBuffer> writeBuffers = new LinkedBlockingQueue<ByteBuffer>();

		volatile boolean closed = false;
		volatile boolean reading = true;
		int writeBuffersReturned = 0;

		boolean isClosed() {
			return closed;
		}

		@Override public void connectionClosed(Exception ex) {
			closed = true;
		}

		@Override public ByteBuffer getReadBuffer() {
			if (reading) {
				return ByteBuffer.allocate(2 * 1024);
			} else {
				return null;
			}
		}

		@Override public void readComplete(ByteBuffer buffer) {
			recvQueue.add(buffer);
		}

		@Override public void writeComplete(ByteBuffer buffer) {
			writeBuffersReturned++;
			writeBuffers.add(buffer);
		}
	}
}
