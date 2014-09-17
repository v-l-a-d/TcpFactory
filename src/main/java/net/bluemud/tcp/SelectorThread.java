package net.bluemud.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

class SelectorThread extends Thread implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(SelectorThread.class);

    /**
     * The selector.
     */
    private Selector selector;

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
    private ConcurrentLinkedQueue<InetSocketAddress> clientRegistrations =
            new ConcurrentLinkedQueue<InetSocketAddress>();

    /**
     * Connection legs with pending writes.
     */
    private ConcurrentLinkedQueue<ConnectionLeg> pendingWrites =
            new ConcurrentLinkedQueue<ConnectionLeg>();


    /**
     * Constructor.
     *
     * @throws java.io.IOException if an error occurs opening the selector
     */
    SelectorThread() throws IOException {
        super("TCP-Selector");

        // Open selector.
        selector = Selector.open();
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

                        // Check whether data can be written again.
                        if (lkey.isValid() && lkey.isWritable()) {
                            processWrite(lkey);
                        }

                        // Check for a cancelled key
                        if (!lkey.isValid()) {
                            ConnectionLeg leg  = (ConnectionLeg)lkey.attachment();
                            if (leg != null) {
                                leg.close();
                            }
                        }
                    }
                }

                // Register any pending server channels.
                ServerSocketChannel lsvr = svrRegistrations.poll();

                while (lsvr != null) {
                    // Register the channel with the selector.
                    lsvr.register(selector, SelectionKey.OP_ACCEPT);
                    lsvr = svrRegistrations.poll();
                }

                // Start any pending client connections.
                InetSocketAddress laddr = clientRegistrations.poll();

                while (laddr != null) {
                    // Open a socket channel to the specified address and initiate the connection.
                    createOutboundLeg(laddr);

                    // Get the next pending connection.
                    laddr = clientRegistrations.poll();
                }

                // Process any pending writes
                ConnectionLeg pendingWrite = pendingWrites.poll();
                while(pendingWrite != null) {
                    pendingWrite.write();
                    pendingWrite = pendingWrites.poll();
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
        lchannel.configureBlocking(false);
        lchannel.socket().bind(addr);

        // Add to the queue of channels pending registration - we do not register
        // here since this may block waiting to acquire the selector's key set
        // which will be locked by the selector.
        svrRegistrations.add(lchannel);

        // Wake-up the selector.
        selector.wakeup();

        return(lchannel);
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

            // TODO call out to create new leg or close.

            // Set up a new connection leg instance object.
//            ConnectionLeg inbound = new ConnectionLeg(lchannel, lkey);
//            lkey.attach(inbound);

            // TODO notify new inbound leg
        }
        catch (IOException iox) {
            LOG.error("Error processing inbound connection ", iox);
        }
    }

    private ConnectionLeg createOutboundLeg(InetSocketAddress addr) throws IOException {
        // Open a socket channel to the specified address and initiate the connection.
        SocketChannel lchannel = SocketChannel.open();
        lchannel.configureBlocking(false);

        // Register with the selector.
        SelectionKey key = lchannel.register(selector, SelectionKey.OP_CONNECT);

        // Create the new connection leg
        ConnectionLeg outbound = new ConnectionLeg(lchannel, key);

        // Attach the leg to the key
        key.attach(outbound);

        // Initiate the connection.
        lchannel.connect(addr);

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
        }
        catch (Exception iox) {
            LOG.error("Outbound connection completion error ", iox);

            // Close the pending connection object
            ((ConnectionLeg)key.attachment()).close();

            // Unregister the channel from the selector
            key.cancel();
        }
    }

    private void processRead(SelectionKey key) throws InterruptedException {
        // Get the connection object associated with the key.
        ConnectionLeg lconn = (ConnectionLeg)key.attachment();

        try {
            // Call into the connection to read data.
            lconn.read();
        }
        catch (Exception iox) {
            LOG.error("read error ", iox);
        }
    }

    private void processWrite(SelectionKey key) throws InterruptedException {
        // Get the connection object associated with the key.
        ConnectionLeg lconn = (ConnectionLeg)key.attachment();

        // TODO write pending data.
    }
}