package net.bluemud.tcp.api;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 */
public interface Connection {
	public InputStream getInputStream();
	public OutputStream getOutputStream();

	InetSocketAddress getRemoteAddress();
	InetSocketAddress getLocalAddress();

	void close();
	boolean isClosed();
}
