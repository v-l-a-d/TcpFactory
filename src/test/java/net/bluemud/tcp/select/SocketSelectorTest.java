package net.bluemud.tcp.select;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SocketSelectorTest {

    private final static InetSocketAddress SERVER = new InetSocketAddress("localhost", 9999);

    private SocketSelector selector;
    private BlockingQueue<Connection> inbound;

    @Before
    public void setup() throws Exception {
        selector = new SocketSelector();
        inbound = new LinkedBlockingQueue<Connection>();

        selector.startServerSocket(SERVER, new InboundConnectionHandler() {
            @Override
            public void inboundConnection(Connection connection) {
                System.out.println("New inbound connection from " +
                        connection.getChannel().socket().getRemoteSocketAddress());
                inbound.add(connection);
            }
        });
        Thread.sleep(100);
    }

    @After
    public void cleanup() {
        selector.close();
    }

    @Test
    public void connect() throws Exception {
        Outbound outbound = new Outbound();
        selector.startClientConnection(outbound);
        Connection connection = inbound.poll(1, TimeUnit.SECONDS);

        assertThat(connection.getChannel().socket().getRemoteSocketAddress(),
                is(outbound.getChannel().socket().getLocalSocketAddress()));
    }

    @Test
    public void readWrite() throws Exception {
        Outbound outbound = new Outbound();
        selector.startClientConnection(outbound);
        Connection connection = inbound.poll(1, TimeUnit.SECONDS);

        Thread.sleep(100); // wait for outbound connect callback

        ByteBuffer data = ByteBuffer.wrap(new byte[] { 23, 23, 23});
        outbound.getChannel().write(data);

        ByteBuffer recv = ByteBuffer.allocate(5);
        connection.getChannel().read(recv);
        recv.flip();
        connection.getChannel().write(recv);

        data.clear();
        outbound.getChannel().read(data);
        assertThat(data.array(), is(new byte[] { 23, 23, 23}));
    }


    private final static class Outbound implements OutboundConnection {

        private volatile SocketChannel channel;

        @Override
        public InetSocketAddress getRemoteAddress() {
            return SERVER;
        }

        @Override
        public void connected(SocketChannel channel) {
            System.out.println("outbound connected");
            this.channel = channel;
        }

        @Override
        public SocketChannel getChannel() {
            return channel;
        }

        @Override
        public boolean readable() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean writable() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void closed(Exception ex) {
            System.out.println("Outbound closed: " + ex);
        }
    }
}
