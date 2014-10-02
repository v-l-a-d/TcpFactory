package net.bluemud.tcp.api;

/**
 * Interface called whenever a new inbound request is received
 */
public interface InboundConnectionHandler {
	ConnectionProcessor acceptConnection(Connection connection);
	void connectionReadable(Connection connection);
}
