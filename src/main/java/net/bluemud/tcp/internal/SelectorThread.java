package net.bluemud.tcp.internal;

import net.bluemud.tcp.OutboundConnectionListener;
import net.bluemud.tcp.api.InboundConnectionHandler;
import net.bluemud.tcp.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

class SelectorThread extends Thread implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(SelectorThread.class);

    /**
     * The selector.
     */
    private final Selector selector;

	/**
	 * Inbound connection handler
	 */
	private final InboundConnectionHandler inboundHandler;

    /**
     * The run flag.
     */
    private volatile boolean running;

    /**
     * Server socket channels waiting to register with the selector.
     */
    private ConcurrentLinkedQueue<ServerSocketChannel> svrRegistrations =
            new ConcurrentLinkedQueue<ServerSocketChannel>();

    /**
     * Client socket channels waiting to register with the selector.
     */
    private ConcurrentLinkedQueue<Pair<InetSocketAddress, OutboundConnectionListener>> clientRegistrations =
            new ConcurrentLinkedQueue<Pair<InetSocketAddress, OutboundConnectionListener>>();

    /**
     * Connection legs with pending writes.
     */
    private ConcurrentLinkedQueue<ConnectionLeg> pendingWrites =
            new ConcurrentLinkedQueue<ConnectionLeg>();

	/**
	 * Connection legs ready to read.
	 */
	private ConcurrentLinkedQueue<ConnectionLeg> readReady =
			new ConcurrentLinkedQueue<ConnectionLeg>();

    /**
     * Constructor.
     *
     * @throws java.io.IOException if an error occurs opening the selector
     */
    SelectorThread(InboundConnectionHandler inboundHandler) throws IOException {
        super("TCP-Selector");

        // Open selector.
        this.selector = Selector.open();
		this.inboundHandler = inboundHandler;
        start();
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    public void start() {
        // Start the thread.
        setDaemon(true);
        running = true;
        super.start();
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    public void run() {

        while (running) {
            try {
                // Wait for an event
                int lkeys = selector.select();

                if (running && (lkeys > 0)) {
                    // There are channels with pending operations in the key set.
                    Iterator<SelectionKey> liter = selector.selectedKeys().iterator();

                    // Process each key
                    while (liter.hasNext()) {
                        // Get the selection key
                        SelectionKey lkey = liter.next();

                        // Remove it from the list to indicate that it is being processed
                        liter.remove();

                        // Check for an inbound connection request
                        if (lkey.isValid() && lkey.isAcceptable()) {
                            processInboundConnection(lkey);
                        }

                        // Check for outbound connection completion.
                        if (lkey.isValid() && lkey.isConnectable()) {
                            processOutboundConnection(lkey);
                        }

                        // Check for received data.
                        if (lkey.isValid() && lkey.isReadable()) {
                            processRead(lkey);
                        }

                        // Check whether data can be readComplete again.
                        if (lkey.isValid() && lkey.isWritable()) {
                            processWrite(lkey);
                        }

                        // Check for a cancelled key
                        if (!lkey.isValid()) {
                            ConnectionLeg leg  = (ConnectionLeg)lkey.attachment();
                            if (leg != null) {
                                leg.close(null);
                            }
                        }
                    }
                }

                // Register any pending server channels.
                ServerSocketChannel lsvr = svrRegistrations.poll();

                while (lsvr != null) {
                    // Register the channel with the selector.
                    SelectionKey key = lsvr.register(selector, SelectionKey.OP_ACCEPT);
					key.interestOps(SelectionKey.OP_ACCEPT);
                    lsvr = svrRegistrations.poll();
                }

                // Start any pending client connections.
                Pair<InetSocketAddress, OutboundConnectionListener> laddr = clientRegistrations.poll();

                while (laddr != null) {
                    // Open a socket channel to the specified address and initiate the connection.
                    createOutboundLeg(laddr.getKey(), laddr.getValue());

                    // Get the next pending connection.
                    laddr = clientRegistrations.poll();
                }

                // Process any pending writes
                ConnectionLeg pendingWrite = pendingWrites.poll();
                while(pendingWrite != null) {
                    pendingWrite.write();
                    pendingWrite = pendingWrites.poll();
                }

				// Process any read ready legs
				ConnectionLeg reader = readReady.poll();
				while (reader != null) {
					reader.enableRead();
					reader = readReady.poll();
				}
			}
            catch (ConcurrentModificationException cmex) {
                // The selected key set has been modified by a separate thread.
                LOG.error("selector key error", cmex);
            }
            catch (IOException iox) {
                LOG.error("selector error ", iox);
            }
            catch (ClosedSelectorException csx) {
                LOG.error("selector closed error ", csx);
            }
            catch (Throwable t) {
                LOG.error("unexpected selector error", t);
            }
        }

        // Thread exiting, close the selector.
        try {
            for (SelectionKey key : selector.keys()) {
                try {
                    key.channel().close();
                } catch (Exception ex) {
                    LOG.warn("Error closing channel", ex);
                }
            }

            selector.close();
        }
        catch (IOException iox) {
            LOG.warn("error closing selector ", iox);
        }
    }

    /**
     * Shutdown the selector thread.
     */
    public void close() {
        running = false;
        selector.wakeup();
    }

    /**
     * Start a server socket using this selector thread.
     *
     * @param addr the local address to listen on
     * @throws java.io.IOException if an error occurs
     */
    ServerSocketChannel startServerSocket(SocketAddress addr)
            throws IOException {

        // Create a non-blocking server socket channel.
        ServerSocketChannel lchannel = ServerSocketChannel.open();
        lchannel.socket().bind(addr);
        lchannel.configureBlocking(false);
        lchannel.accept();

        System.out.println("New server socket on " + lchannel.socket().getLocalSocketAddress() + " port " + lchannel.socket().getLocalPort());

        // Add to the queue of channels pending registration - we do not register
        // here since this may block waiting to acquire the selector's key set
        // which will be locked by the selector.
        svrRegistrations.add(lchannel);

        // Wake-up the selector.
        selector.wakeup();

        return(lchannel);
    }

	void startClientConnection(InetSocketAddress socketAddress, OutboundConnectionListener listener) {
		clientRegistrations.add(Pair.of(socketAddress, listener));
		selector.wakeup();
	}

	void addPendingWrite(ConnectionLeg leg) {
		pendingWrites.offer(leg);
		selector.wakeup();
	}

	void addReadReady(ConnectionLeg leg) {
		readReady.offer(leg);
		selector.wakeup();
	}

    private void processInboundConnection(SelectionKey key) {

        // Get channel with connection request
        ServerSocketChannel lsrvChannel = (ServerSocketChannel)key.channel();

        try {
            // Get the incoming connection.
            SocketChannel lchannel = lsrvChannel.accept();
            assert(lchannel != null);

            LOG.debug("Inbound connection from {}", lchannel.socket().getRemoteSocketAddress());

            // Set channel to non-blocking mode.
            lchannel.configureBlocking(false);

            // Register the received channel with the selector.
            SelectionKey lkey = lchannel.register(selector, SelectionKey.OP_READ);

            // Set up a new connection leg instance object.
			ConnectionLeg inbound = new ConnectionLeg(lchannel, lkey, this);
            if (inboundHandler.acceptConnection(inbound)) {
				lkey.attach(inbound);
			} else {
				// Rejected
				inbound.close();
			}
        }
        catch (IOException iox) {
            LOG.error("Error processing inbound connection ", iox);
        }
    }

    ConnectionLeg createOutboundLeg(InetSocketAddress addr, OutboundConnectionListener listener) throws IOException {
        // Open a socket channel to the specified address and initiate the connection.
        SocketChannel lchannel = SocketChannel.open();
        lchannel.configureBlocking(false);

        // Register with the selector.
        SelectionKey key = lchannel.register(selector, SelectionKey.OP_CONNECT);

        // Create the new connection leg
        ConnectionLeg outbound = new ConnectionLeg(lchannel, key, this);

		// Attach the leg to the key
		key.attach(outbound);

		// Initiate the connection.
		lchannel.connect(addr);

		// Notify.
		listener.connected(addr, outbound);

        return outbound;
    }

    private void processOutboundConnection(SelectionKey key) {

        // Get channel with connection request
        SocketChannel lchannel = (SocketChannel)key.channel();

        try {
            // Complete the connection.
            boolean success = lchannel.finishConnect();

            if (!success) {
                throw new IOException("Connection completion failed");
            }

            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);

			// Process any writes pending on the connection
			((ConnectionLeg)key.attachment()).write();
        }
        catch (Exception iox) {
            LOG.error("Outbound connection completion error ", iox);

            // Close the pending connection object
            ((ConnectionLeg)key.attachment()).close(iox);

            // Unregister the channel from the selector
            key.cancel();
        }
    }

    private void processRead(SelectionKey key) throws InterruptedException {
        // Get the connection object associated with the key.
        ConnectionLeg lconn = (ConnectionLeg)key.attachment();
		lconn.read();

		// Notify the connection handler.
		inboundHandler.connectionReadable(lconn);
    }

    private void processWrite(SelectionKey key) throws InterruptedException {
        // Get the connection object associated with the key.
        ConnectionLeg lconn = (ConnectionLeg)key.attachment();
		lconn.write();
    }
}
