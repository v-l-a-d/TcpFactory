package net.bluemud.tcp;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.ConnectionProcessor;
import net.bluemud.tcp.api.InboundConnectionHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class TcpFactory {

	private static InboundConnectionHandler REJECT_ALL = new InboundConnectionHandler() {
		@Override public ConnectionProcessor acceptConnection(Connection connection) {
			return null;
		}

		@Override public void connectionReadable(Connection connection) {
		}
	};

	private final SelectorThread selectorThread;
	private final InboundConnectionHandler inboundHandler;

	public TcpFactory() throws IOException {
		this(REJECT_ALL);
	}

	public TcpFactory(InboundConnectionHandler inboundHandler) throws IOException {
		this.inboundHandler = inboundHandler;
		this.selectorThread = new SelectorThread(this);
	}

	public void listenOn(SocketAddress socketAddress) throws IOException {
		selectorThread.startServerSocket(socketAddress);
	}

	public Connection connectTo(ConnectionProcessor processor, InetSocketAddress socketAddress) throws IOException, InterruptedException {
		final SynchronousQueue<ConnectionLeg> legQueue = new SynchronousQueue<ConnectionLeg>();

		OutboundConnectionListener listener = new OutboundConnectionListener() {
			@Override public void connected(InetSocketAddress remoteAddress, ConnectionLeg connection) {
				try {
					legQueue.offer(connection, 2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// TODO log the error.
				}
			}
		};

		selectorThread.startClientConnection(socketAddress, listener);
		ConnectionLeg connection = legQueue.poll(30, TimeUnit.SECONDS);
		connection.setProcessor(processor);
		return connection;
	}

	public void shutdown() {
		selectorThread.close();
	}

	ConnectionLeg createLegForInboundConnection(SocketChannel channel, SelectionKey key) {
		ConnectionLeg connection = new ConnectionLeg(channel, key, selectorThread);
		ConnectionProcessor processor = inboundHandler.acceptConnection(connection);

		if (processor == null) {
			connection.close(null);
			return null;
		}

		connection.setProcessor(processor);
	    return connection;
	}

	/**
	 * Called by selector when there is data to read on a connection leg.
	 */
	void connectionReadyToRead(ConnectionLeg connection) {
		inboundHandler.connectionReadable(connection);
	}
}
