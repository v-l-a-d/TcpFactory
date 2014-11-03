package net.bluemud.tcp;

import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.InboundConnectionHandler;
import net.bluemud.tcp.internal.TcpFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
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
	private volatile Connection inboundConnection;
	private Connection clientConnection;

	@Before
	public void setup() throws Exception {
		clientFactory = new TcpFactory();
		serverFactory = new TcpFactory(new InboundConnectionHandler() {
			@Override public boolean acceptConnection(Connection connection) {
				inboundConnection = connection;
				return true;
			}

			@Override public void connectionReadable(Connection connection) {
			}

			@Override
			public void connectionClosed(Connection connection) {
			}
		});

		// Start listening.
		serverFactory.listenOn(new InetSocketAddress("127.0.0.1", 11211));

		// Make a client connection to the listening address.
		clientConnection = clientFactory.connectTo(new InetSocketAddress("127.0.0.1", 11211));

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
		clientConnection.getOutputStream().write(testData);
		clientConnection.getOutputStream().flush();

		// Expect data to be received at the server.
		byte[] b = new byte[testData.length];
		inboundConnection.getInputStream().read(b);
		assertThat(new String(b), is("Some sort of test data string"));

		// Send something back again
		byte[] rsp = "Some kind of response text".getBytes();
		inboundConnection.getOutputStream().write(rsp);
		inboundConnection.getOutputStream().flush();

		byte[] recvData = new byte[rsp.length];
		clientConnection.getInputStream().read(recvData);
		assertThat(new String(recvData), is("Some kind of response text"));

		// Close the connection at the client end.
		clientConnection.close();
		Thread.sleep(100);
		assertThat(inboundConnection.isClosed(), is(true));
	}}
