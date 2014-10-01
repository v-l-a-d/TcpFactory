package net.bluemud.tcp;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
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

    final AtomicInteger received = new AtomicInteger(0);
    final int data_size = 1024*1024;
    final int clients = 11;
    final byte[] data = new byte[data_size];


    final List<Thread> threads = Lists.newCopyOnWriteArrayList();

    @Before
    public void setup() throws Exception {
        for (int ii = 0; ii < data_size; ii++) {
            data[ii] = (byte)(ii % 255);
        }

        clientFactory = new TcpFactory();
        serverFactory = new TcpFactory(new InboundConnectionHandler() {
            @Override public ConnectionProcessor acceptConnection(Connection connection) {
                System.out.println("new connection");
                final StreamConnectionProcessor serverProcessor = new StreamConnectionProcessor(32*1024);
                serverProcessor.setConnection(connection);

                // Launch thread to read the connection
                Thread t = new Thread() {
                    @Override
                    public void run() {
                        byte[] read = new byte[data_size];
                        long start = System.currentTimeMillis();
                        InputStream serverIn = serverProcessor.getInputStream();

                        int bytesRead = 0;
                        while (bytesRead < (data_size)) {
                            try {
                                bytesRead += serverIn.read(read, bytesRead, Math.min(Math.max(256, serverIn.available()),read.length - bytesRead));
                                System.out.println("Read: " + bytesRead + " of " + (data_size));
                            } catch (Exception e) {
                                throw Throwables.propagate(e);
                            }
                        }
                        System.out.println("Read took " + (System.currentTimeMillis() - start) + "ms");
                        assertThat(read, is(data));
                        received.addAndGet(bytesRead);
                    }
                };
                t.setDaemon(true);
                t.start();
                threads.add(t);

                return serverProcessor;
            }
        });

        // Start listening.
        serverFactory.listenOn(new InetSocketAddress(11211));
    }



    @org.junit.Test
    public void serverTest() throws Exception {

        for (int ii = 0; ii < clients; ii++) {
            Thread cl = new Thread() {
                @Override
                public void run() {
                    try {

                        StreamConnectionProcessor clProcessor = new StreamConnectionProcessor(8*1024);
                        Connection clientConnection = clientFactory.connectTo(clProcessor, new InetSocketAddress("127.0.0.1", 11211));
                        clProcessor.setConnection(clientConnection);
                        OutputStream clOut = clProcessor.getOutputStream();

                        Thread.sleep(100);
                        long start = System.currentTimeMillis();
                        int written = 0;
                        while (written < data_size) {
                            clOut.write(data, written, 8*1024);
                            clOut.flush();
                            written += 8*1024;
                            System.out.println("Written " + written + " of " + data_size);
                        }
                        System.out.println("writer done " + (System.currentTimeMillis() - start) + "ms");

                        Thread.sleep(200);
                        clProcessor.close();
                    } catch (Exception ex) {
                        System.out.println("Client thread exiting with error " + ex);
                        ex.printStackTrace(System.out);
                    }
                }
            };
            cl.setDaemon(true);
            cl.start();
        }

        Thread.sleep(200);
        for (Thread t : threads) {
            t.join(30000);
        }
        assertThat(received.get(), is(clients * data_size));
    }
}
