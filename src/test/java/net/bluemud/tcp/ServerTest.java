package net.bluemud.tcp;

import com.google.common.base.Throwables;
import com.google.common.collect.Queues;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.ConnectionProcessor;
import net.bluemud.tcp.api.InboundConnectionHandler;
import net.bluemud.tcp.util.StreamConnectionProcessor;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: vlad
 * Date: 26/09/2014
 * Time: 22:29
 * To change this template use File | Settings | File Templates.
 */
public class ServerTest {

    private TcpFactory clientFactory;
    private TcpFactory serverFactory;
    private Queue<StreamConnectionProcessor> connections;

    @Before
    public void setup() throws Exception {
        connections = Queues.newConcurrentLinkedQueue();
        clientFactory = new TcpFactory();
        serverFactory = new TcpFactory(new InboundConnectionHandler() {
            @Override public ConnectionProcessor acceptConnection(Connection connection) {
                System.out.println("new connection");
                StreamConnectionProcessor serverProcessor = new StreamConnectionProcessor(1024);
                serverProcessor.setConnection(connection);
                connections.add(serverProcessor);
                return serverProcessor;
            }
        });

        // Start listening.
        serverFactory.listenOn(new InetSocketAddress("127.0.0.1", 11211));
    }

    @org.junit.Test
    public void serverTest() throws Exception {
        final AtomicInteger received = new AtomicInteger(0);
        final int data_size = 1024*1024;
        final int clients = 1;

        final byte[] data = new byte[data_size];
        for (int ii = 0; ii < data_size; ii++) {
            data[ii] = (byte)(ii % 255);
        }

        final byte[] read = new byte[clients * data_size];

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }

                int bytesRead = 0;
                while (bytesRead < (clients * data_size)) {
                    try {
                        StreamConnectionProcessor processor = connections.poll();
                        if (processor == null) {
                            break;
                        }

                        if (!processor.isClosed()) {
                            InputStream serverIn = processor.getInputStream();

                        //    if (serverIn.available() > 0) {
                               bytesRead += serverIn.read(read, bytesRead, Math.min(serverIn.available(), read.length - bytesRead));
                               System.out.println("Read: " + bytesRead + " of " + (clients * data_size));
                        //    }
                            connections.add(processor);
                        }
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
                received.set(bytesRead);
            }
        };
        t.setDaemon(true);
        t.start();

        for (int ii = 0; ii < clients; ii++) {
            Thread cl = new Thread() {
                @Override
                public void run() {
                    try {

                        StreamConnectionProcessor clProcessor = new StreamConnectionProcessor(1024);
                        Connection clientConnection = clientFactory.connectTo(clProcessor, new InetSocketAddress("127.0.0.1", 11211));
                        clProcessor.setConnection(clientConnection);
                        OutputStream clOut = clProcessor.getOutputStream();

                        int written = 0;
                        while (written < data_size) {
                            clOut.write(data, written, 1024);
                            clOut.flush();
                            written += 1024;
                            System.out.println("Written " + written + " of " + data_size);
                        }

                        clProcessor.close();
                        System.out.println("writer done");
                    } catch (Exception ex) {
                        System.out.println("Client thread exiting with error " + ex);
                        ex.printStackTrace(System.out);
                    }
                }
            };
            cl.setDaemon(true);
            cl.start();
        }

        t.join(30000);
        assertThat(received.get(), is(clients * data_size));
        //assertThat(read, is(data));
    }
}
