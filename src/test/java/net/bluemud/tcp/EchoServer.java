package net.bluemud.tcp;

import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.internal.TcpFactory;
import net.bluemud.tcp.util.AbstractInboundHandler;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 */
public class EchoServer {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        TcpFactory serverFactory = null;
        try {
            serverFactory = new TcpFactory(new AbstractInboundHandler(Executors.newSingleThreadExecutor()) {
				@Override public void handle(Connection connection) {
					byte[] read = new byte[1024];
					InputStream serverIn = connection.getInputStream();
					OutputStream serverOut = connection.getOutputStream();
					try {

						int bytesRead = serverIn.read(read, 0, Math.min(read.length, serverIn.available()));
						System.out.println("Read: " + new String(read, 0, bytesRead));
						serverOut.write(read, 0, bytesRead);
						serverOut.flush();
					} catch (Exception ex ) {
						System.out.println("read error:" + ex);
					}
				}
			});

            // Start listening
            serverFactory.listenOn(new InetSocketAddress(9999));

			synchronized (EchoServer.class) {
				try {
					EchoServer.class.wait();
				} catch (InterruptedException e) {
				}
			}

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
    }
}
