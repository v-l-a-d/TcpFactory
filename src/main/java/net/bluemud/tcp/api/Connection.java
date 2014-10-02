package net.bluemud.tcp.api;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 */
public interface Connection {
	void write(ByteBuffer buffer);
	void readBufferAvailable();
	void setProcessor(ConnectionProcessor processor);
	ConnectionProcessor getProcessor();
	void close();

	InetSocketAddress getRemoteAddress();
	InetSocketAddress getLocalAddress();
}
