package net.bluemud.tcp;

import com.google.common.base.Throwables;
import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.InboundConnectionHandler;
import net.bluemud.tcp.internal.TcpFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 */
public class StreamEndToEndTest {
    private TcpFactory clientFactory;
    private TcpFactory serverFactory;
    private Connection clientProcessor;
    private Connection serverProcessor;

    @Before
    public void setup() throws Exception {
        clientFactory = new TcpFactory();
        serverFactory = new TcpFactory(new InboundConnectionHandler() {
            @Override public boolean acceptConnection(Connection connection) {
				serverProcessor = connection;
                return true;
            }

			@Override public void connectionReadable(Connection connection) {
			}

			@Override
			public void connectionClosed(Connection connection) {
			}
		});

        // Start listening.
        serverFactory.listenOn(new InetSocketAddress("127.0.0.1", 11212));

        // Make a client connection to the listening address.
        clientProcessor = clientFactory.connectTo(new InetSocketAddress("127.0.0.1", 11212));

        Thread.sleep(100);
        assertThat(serverProcessor, not(nullValue()));
    }

    @After
    public void cleanup() throws Exception {
		serverFactory.shutdown();
		clientFactory.shutdown();
    }

    @Test
    public void readAndWrite() throws Exception {
        OutputStream clientOut = clientProcessor.getOutputStream();
        InputStream serverIn = serverProcessor.getInputStream();

        byte[] packet = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        clientOut.write(packet);
        clientOut.flush();

        byte[] recv = new byte[9];
        int bytesRead = 0;
        while (bytesRead < 9) {
            bytesRead += serverIn.read(recv, bytesRead, 9 - bytesRead);
        }
        assertThat(serverIn.available(), is(0));
        assertThat(recv, is(packet));

        OutputStream serverOut = serverProcessor.getOutputStream();
        InputStream clientIn = clientProcessor.getInputStream();
        serverOut.write(recv);
        serverOut.flush();

        byte[] cRecv = new byte[9];
        bytesRead = 0;
        while (bytesRead < 9) {
            bytesRead += clientIn.read(cRecv, bytesRead, 9 - bytesRead);
        }
        assertThat(clientIn.available(), is(0));
        assertThat(cRecv, is(recv));
    }

    @Test
    public void bigReadAndWrite() throws Exception {
        final OutputStream clientOut = clientProcessor.getOutputStream();
        final InputStream serverIn = serverProcessor.getInputStream();
        final AtomicInteger received = new AtomicInteger(0);
        final int data_size = 1024*1024;

        final byte[] data = new byte[data_size];
        for (int ii = 0; ii < data_size; ii++) {
            data[ii] = (byte)(ii % 255);
        }

        final byte[] read = new byte[data_size];

        Thread t = new Thread() {
            @Override
            public void run() {
                int bytesRead = 0;
                while (bytesRead < data_size) {
                    try {
                        bytesRead += serverIn.read(read, bytesRead, Math.min(1024, read.length - bytesRead));
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
                received.set(bytesRead);
            }
        };
        t.setDaemon(true);
        t.start();

        int written = 0;
        while (written < data_size) {
            clientOut.write(data, written, 1024);
            clientOut.flush();
            written += 1024;
        }

        t.join(1000);
        assertThat(received.get(), is(data_size));
        assertThat(read, is(data));
    }
}
