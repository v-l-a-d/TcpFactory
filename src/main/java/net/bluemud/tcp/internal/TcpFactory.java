package net.bluemud.tcp.internal;

import net.bluemud.tcp.OutboundConnectionListener;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.InboundConnectionHandler;
import net.bluemud.tcp.internal.ConnectionLeg;
import net.bluemud.tcp.internal.SelectorThread;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class TcpFactory {

	private static InboundConnectionHandler REJECT_ALL = new InboundConnectionHandler() {
		@Override
		public boolean acceptConnection(Connection connection) {
			return false;
		}

		@Override public void connectionReadable(Connection connection) {
		}

		@Override
		public void connectionClosed(Connection connection) {
		}
	};

	private final SelectorThread selectorThread;

	public TcpFactory() throws IOException {
		this(REJECT_ALL);
	}

	public TcpFactory(InboundConnectionHandler inboundHandler) throws IOException {
		this.selectorThread = new SelectorThread(inboundHandler);
	}

	public void listenOn(SocketAddress socketAddress) throws IOException {
		selectorThread.startServerSocket(socketAddress);
	}

	public Connection connectTo(InetSocketAddress socketAddress) throws IOException, InterruptedException {
		final SynchronousQueue<Connection> legQueue = new SynchronousQueue<Connection>();

		OutboundConnectionListener listener = new OutboundConnectionListener() {
			@Override public void connected(InetSocketAddress remoteAddress, Connection connection) {
				try {
					legQueue.offer(connection, 2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// TODO log the error.
				}
			}
		};

		selectorThread.startClientConnection(socketAddress, listener);
		Connection connection = legQueue.poll(30, TimeUnit.SECONDS);
		return connection;
	}

	public void shutdown() {
		selectorThread.close();
	}
}
