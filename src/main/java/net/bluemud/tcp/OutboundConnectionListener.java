package net.bluemud.tcp;

import net.bluemud.tcp.api.Connection;

import java.net.InetSocketAddress;

/**
 * Called when a particular outbound connection is connected.
 */
public interface OutboundConnectionListener {
	public void connected(InetSocketAddress remoteAddress, Connection connection);
}
