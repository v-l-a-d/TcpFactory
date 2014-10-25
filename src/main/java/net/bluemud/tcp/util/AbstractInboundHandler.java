package net.bluemud.tcp.util;

import com.google.common.collect.Sets;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.InboundConnectionHandler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 */
public abstract class AbstractInboundHandler implements InboundConnectionHandler {

	private final ExecutorService executor;
	private final Set<Connection> activeConnections;


	public AbstractInboundHandler(ExecutorService executor) {
		this.executor = executor;
		this.activeConnections = Sets.newConcurrentHashSet();
	}

	@Override
	public boolean acceptConnection(Connection connection) {
		return true;
	}

	@Override
	public void connectionReadable(final Connection connection) {
		if (activeConnections.add(connection)) {
			// Not an active connection - add to the read executor
			executor.execute(new Runnable() {
				@Override public void run() {
					try {
						handle(connection);
					} finally {
						returnConnection(connection);
					}
				}
			});
		}
	}

	private void returnConnection(Connection connection) {
		activeConnections.remove(connection);
		try {
			if (connection.getInputStream().available() != 0) {
				connectionReadable(connection);
			}
		} catch (IOException e) {
			// Unexpected
		}
	}

	public abstract void handle(Connection connection);
}
