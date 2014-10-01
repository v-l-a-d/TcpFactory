package net.bluemud.tcp.select;

import net.bluemud.tcp.select.impl.InboundConnectionImpl;
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

class SocketSelector extends Thread implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(SocketSelector.class);

    /**
     * The selector.
     */
    private final Selector selector;

    /**
     * The run flag.
     */
    private volatile boolean running;

    /**
     * Server socket channels waiting to register with the selector.
     */
    private ConcurrentLinkedQueue<Pair<ServerSocketChannel, InboundConnectionHandler>> svrRegistrations =
            new ConcurrentLinkedQueue<Pair<ServerSocketChannel, InboundConnectionHandler>>();

    /**
     * Client socket channels waiting to register with the selector.
     */
    private ConcurrentLinkedQueue<OutboundConnection> clientRegistrations =
            new ConcurrentLinkedQueue<OutboundConnection>();

    /**
     * Constructor.
     *
     * @throws java.io.IOException if an error occurs opening the selector
     */
    SocketSelector() throws IOException {
        super("TCP-Selector");

        // Open selector.
        this.selector = Selector.open();
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
                            if (!((Connection)lkey.attachment()).readable()) {
                                // Not readable, remove read interest
                                lkey.interestOps(lkey.interestOps() & SelectionKey.OP_WRITE);
                            }
                        }

                        // Check whether data can be readComplete again.
                        if (lkey.isValid() && lkey.isWritable()) {
                            if (!((Connection)lkey.attachment()).writable()) {
                                // Nothing to write, remove write interest
                                lkey.interestOps(lkey.interestOps() & SelectionKey.OP_READ);
                            }
                        }

                        // Check for a cancelled key
                        if (!lkey.isValid()) {
                            Connection connection  = (Connection)lkey.attachment();
                            if (connection != null) {
                                connection.closed(null);
                            }
                        }
                    }
                }

                // Register any pending server channels.
                Pair<ServerSocketChannel, InboundConnectionHandler> lsvr = svrRegistrations.poll();

                while (lsvr != null) {
                    // Register the channel with the selector.
                    SelectionKey selectionKey = lsvr.getKey().register(selector, SelectionKey.OP_ACCEPT);
                    selectionKey.attach(lsvr.getValue());
                    lsvr = svrRegistrations.poll();
                }

                // Start any pending client connections.
                OutboundConnection outboundConnection = clientRegistrations.poll();

                while (outboundConnection != null) {
                    // Open a socket channel to the specified address and initiate the connection.
                    startOutboundConnection(outboundConnection);

                    // Get the next pending connection.
                    outboundConnection = clientRegistrations.poll();
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
    ServerSocketChannel startServerSocket(SocketAddress addr, InboundConnectionHandler handler)
            throws IOException {

        // Create a non-blocking server socket channel.
        ServerSocketChannel lchannel = ServerSocketChannel.open();
        lchannel.socket().bind(addr);
        lchannel.configureBlocking(false);

        System.out.println("New server socket on " + lchannel.socket().getLocalSocketAddress() + " port " + lchannel.socket().getLocalPort());

        // Add to the queue of channels pending registration - we do not register
        // here since this may block waiting to acquire the selector's key set
        // which will be locked by the selector.
        svrRegistrations.add(Pair.of(lchannel, handler));

        // Wake-up the selector.
        selector.wakeup();

        return(lchannel);
    }

    void startClientConnection(OutboundConnection connection) {
        clientRegistrations.add(connection);
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

            // Set up a new connection instance.
            Connection connection = new InboundConnectionImpl(lchannel);
            lkey.attach(connection);

            // Add to inbound connection queue.
            ((InboundConnectionHandler)key.attachment()).inboundConnection(connection);
        }
        catch (IOException iox) {
            LOG.error("Error processing inbound connection ", iox);
        }
    }

    void startOutboundConnection(OutboundConnection connection) throws IOException {
        // Open a socket channel to the specified address and initiate the connection.
        SocketChannel lchannel = SocketChannel.open();
        lchannel.configureBlocking(false);

        // Register with the selector.
        SelectionKey key = lchannel.register(selector, SelectionKey.OP_CONNECT);

        // Attach the leg to the key
        key.attach(connection);

        // Initiate the connection.
        lchannel.connect(connection.getRemoteAddress());
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
            OutboundConnection connection = (OutboundConnection)key.attachment();
            connection.connected(lchannel);
        }
        catch (Exception iox) {
            LOG.error("Outbound connection completion error ", iox);

            // Close the pending connection object
            ((Connection)key.attachment()).closed(iox);

            // Unregister the channel from the selector
            key.cancel();
        }
    }

}
