package net.bluemud.tcp;

import com.google.protobuf.ByteString;
import net.bluemud.protobuf.v1.Proto;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.InboundConnectionHandler;
import net.bluemud.tcp.internal.TcpFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test using protobuf read / write
 */
public class ProtobufTest {

	private TcpFactory clientFactory;
	private TcpFactory serverFactory;
	private Connection clientProcessor;
	private Connection serverProcessor;

	@Before
	public void setup() throws Exception {
		clientFactory = new TcpFactory();
		serverFactory = new TcpFactory(new InboundConnectionHandler() {
			@Override public boolean acceptConnection(Connection connection) {
				serverProcessor = connection;
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
		clientProcessor = clientFactory.connectTo(new InetSocketAddress("127.0.0.1", 11211));

		Thread.sleep(100);
		assertThat(serverProcessor, not(nullValue()));
	}

	@After
	public void cleanup() throws Exception {
		serverFactory.shutdown();
		clientFactory.shutdown();
	}

	@Test
	public void requestResponse() throws Exception {
		OutputStream out = clientProcessor.getOutputStream();
		InputStream in = serverProcessor.getInputStream();

		// Build and send request
		Proto.Request.Builder builder = Proto.Request.newBuilder();
		builder.setId(12345L);
		builder.setUrl("http:/bludemud.net?a=b");
		builder.setMethod(Proto.Request.Type.POST);
		builder.setBody(ByteString.copyFrom("request body stuff".getBytes()));
		builder.build().writeDelimitedTo(out);
		out.flush();

		// Receive request
		Proto.Request request = Proto.Request.parseDelimitedFrom(in);
		assertThat(request.getId(), is(12345L));
		assertThat(request.getMethod(), is (Proto.Request.Type.POST));
		assertThat(request.getUrl(), is("http:/bludemud.net?a=b"));
		assertThat(new String(request.getBody().toByteArray()), is("request body stuff"));

		// Send a response
		Proto.Response.Builder rspBuilder = Proto.Response.newBuilder();
		rspBuilder.setId(12345L);
		rspBuilder.setStatusCode(200);
		rspBuilder.setBody(ByteString.copyFrom("Thank you for that request. Smashing.".getBytes()));
		rspBuilder.build().writeDelimitedTo(serverProcessor.getOutputStream());
		serverProcessor.getOutputStream().flush();

		// Receive the response
		Proto.Response response = Proto.Response.parseDelimitedFrom(clientProcessor.getInputStream());
		assertThat(response.getId(), is(12345L));
		assertThat(response.getStatusCode(), is(200));
		assertThat(new String(response.getBody().toByteArray()), is("Thank you for that request. Smashing."));
	}


}
