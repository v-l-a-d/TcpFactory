package net.bluemud.tcp.api;

/**
 * Interface called whenever a new inbound request is received
 */
public interface InboundConnectionHandler {
	boolean acceptConnection(Connection connection);
	void connectionReadable(Connection connection);
}
