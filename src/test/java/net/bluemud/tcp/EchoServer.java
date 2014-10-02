package net.bluemud.tcp;

import com.google.common.base.Throwables;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.ConnectionProcessor;
import net.bluemud.tcp.api.InboundConnectionHandler;
import net.bluemud.tcp.util.StreamConnectionProcessor;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 */
public class EchoServer {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        TcpFactory serverFactory = null;
        try {
            serverFactory = new TcpFactory(new InboundConnectionHandler() {
                @Override public ConnectionProcessor acceptConnection(Connection connection) {
                    System.out.println("new connection");
                    final StreamConnectionProcessor serverProcessor = new StreamConnectionProcessor(32*1024);
                    serverProcessor.setConnection(connection);

                    // Launch thread to read the connection
                    Runnable r = new Runnable() {
                        @Override
                        public void run() {
                            byte[] read = new byte[1024];
                            InputStream serverIn = serverProcessor.getInputStream();
                            OutputStream serverOut = serverProcessor.getOutputStream();

                            while (!serverProcessor.isClosed()) {
                                try {
                                    int bytesRead = serverIn.read(read, 0, Math.min(serverIn.available(),read.length));
                                    System.out.println("Read: " + bytesRead);
                                    serverOut.write(read, 0, bytesRead);
                                } catch (Exception e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        }
                    };

                    return serverProcessor;
                }

				@Override public void connectionReadable(Connection connection) {
				}
			});

            // Start listening
            serverFactory.listenOn(new InetSocketAddress(9999));

            // Start socket server
//            ServerSocket svrSock = new ServerSocket(9998);
//            svrSock.accept();

        } catch (Exception ex) {
            System.out.println("ERROR: " + ex);
            ex.printStackTrace(System.out);
        } finally {
            if (serverFactory != null) {
                serverFactory.shutdown();
            }
            executorService.shutdown();
            try {
                executorService.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
        }

        synchronized (EchoServer.class) {
            try {
                EchoServer.class.wait();
            } catch (InterruptedException e) {
            }
        }
    }
}
