package net.bluemud.tcp.select.impl;

import net.bluemud.tcp.select.Connection;

import java.nio.channels.SocketChannel;

/**
 */
public class InboundConnectionImpl implements Connection {

    private final SocketChannel channel;

    public InboundConnectionImpl(SocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public SocketChannel getChannel() {
        return channel;
    }

    @Override
    public boolean readable() {
        return false;
    }

    @Override
    public boolean writable() {
        return false;
    }

    @Override
    public void closed(Exception ex) {
    }
}
