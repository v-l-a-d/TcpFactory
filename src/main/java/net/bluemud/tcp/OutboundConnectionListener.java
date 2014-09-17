package net.bluemud.tcp;

import java.net.InetSocketAddress;

/**
 * Called when a particular outbound connection is connected.
 */
public interface OutboundConnectionListener {
	public void connected(InetSocketAddress remoteAddress, ConnectionLeg connection);
}
