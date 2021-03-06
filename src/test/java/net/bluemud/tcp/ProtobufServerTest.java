package net.bluemud.tcp;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import net.bluemud.protobuf.v1.Proto;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.internal.TcpFactory;
import net.bluemud.tcp.util.AbstractInboundHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 */
public class ProtobufServerTest {
	private TcpFactory clientFactory;
	private TcpFactory serverFactory;

	private final int dataSize = 128*1024;
	private final int numClients = 15;
	private final int iterations = 23;

	private byte[] data = new byte[dataSize];

	@Before
	public void setup() throws Exception {

		for (int ii = 0; ii < dataSize; ii++) {
			data[ii] = (byte)(Math.random() * 255);
		}

		clientFactory = new TcpFactory();
		serverFactory = new TcpFactory(new AbstractInboundHandler(Executors.newFixedThreadPool(5)) {
			@Override public void handle(Connection processor) {
				try {
					// Read request
					Proto.Request request = Proto.Request.parseDelimitedFrom(processor.getInputStream());
					long id = request.getId();
					assertThat(request.getMethod(), is(Proto.Request.Type.POST));
					assertThat(request.getUrl(), is("http:/bludemud.net?a=b"));
					byte[] bytes = request.getBody().toByteArray();
					assertThat(bytes.length, is(data.length));

					// Write response
					Proto.Response.Builder rspBuilder = Proto.Response.newBuilder();
					rspBuilder.setId(id);
					rspBuilder.setStatusCode(200);
					rspBuilder.setBody(ByteString.copyFrom(bytes));
					rspBuilder.build().writeDelimitedTo(processor.getOutputStream());
					processor.getOutputStream().flush();

				} catch (Exception ex) {
					System.out.println("Error handling protobuf message");
				}
			}
		});

		// Start listening.
		serverFactory.listenOn(new InetSocketAddress("127.0.0.1", 11211));
	}

	@After
	public void cleanup() throws Exception {
		serverFactory.shutdown();
		clientFactory.shutdown();
	}

	@Test
	public void requestResponse() throws Exception {

		List<Thread> clients = Lists.newArrayList();

		for (int ii = 0; ii < numClients; ii++) {
			Thread t = new Thread() {
				@Override
				public void run() {

					try {
						Connection clientConnection = clientFactory.connectTo(new InetSocketAddress("127.0.0.1", 11211));
						Thread.sleep(100);

						OutputStream out = clientConnection.getOutputStream();
						InputStream in = clientConnection.getInputStream();

						for (int jj = 0; jj < iterations; jj++) {
							long start = System.nanoTime();

							// Send request and read response.
							// Build and send request
							long id = 12345L + jj;

							Proto.Request.Builder builder = Proto.Request.newBuilder();
							builder.setId(id);
							builder.setUrl("http:/bludemud.net?a=b");
							builder.setMethod(Proto.Request.Type.POST);
							builder.setBody(ByteString.copyFrom(data));
							builder.build().writeDelimitedTo(out);
							out.flush();

							// Read the response.
							Proto.Response response = Proto.Response.parseDelimitedFrom(clientConnection.getInputStream());
							assertThat(response.getId(), is(id));
							assertThat(response.getStatusCode(), is(200));
							assertThat(response.getBody().toByteArray().length, is(data.length));
							System.out.println("iteration " + jj + " complete in " + ((System.nanoTime() - start) / (1000L*1000)) + "ms");
							assertThat(response.getBody().toByteArray(), is(data));
						}
					} catch (Exception ex) {
						System.out.println("Client error " + ex);
					}
				}
			};
			t.setDaemon(true);
			clients.add(t);
		}

		for (Thread t : clients) {
			t.start();
		}

		for (Thread t : clients) {
			t.join(30000);
		}

		for (Thread t : clients) {
			t.interrupt();
		}
	}
}
