package net.bluemud.tcp.util;

import com.google.common.collect.Sets;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.ConnectionProcessor;
import net.bluemud.tcp.api.InboundConnectionHandler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 */
public abstract class AbstractInboundHandler implements InboundConnectionHandler {

	private final ExecutorService executor;
	private final Set<ConnectionProcessor> activeProcessors;


	public AbstractInboundHandler(ExecutorService executor) {
		this.executor = executor;
		this.activeProcessors = Sets.newConcurrentHashSet();
	}

	@Override
	public ConnectionProcessor acceptConnection(Connection connection) {
		StreamConnectionProcessor serverProcessor = new StreamConnectionProcessor(64 * 1024);
		serverProcessor.setConnection(connection);
		return serverProcessor;
	}

	@Override
	public void connectionReadable(final Connection connection) {
		final StreamConnectionProcessor processor = (StreamConnectionProcessor) connection.getProcessor();
		if (activeProcessors.add(processor)) {
			// Not an active connection - add to the read executor
			executor.execute(new Runnable() {
				@Override public void run() {
					try {
						process(processor);
					} finally {
						returnConnection(processor);
					}
				}
			});
		}
	}

	private void returnConnection(StreamConnectionProcessor processor) {
		activeProcessors.remove(processor);
		try {
			if (processor.getInputStream().available() != 0) {
				connectionReadable(processor.getConnection());
			}
		} catch (IOException e) {
			// Unexpected
		}
	}

	public abstract void process(StreamConnectionProcessor processor);
}
