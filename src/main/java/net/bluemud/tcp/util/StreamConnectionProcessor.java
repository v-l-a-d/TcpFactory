package net.bluemud.tcp.util;

import net.bluemud.tcp.api.Connection;
import net.bluemud.tcp.api.ConnectionProcessor;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Connection processor that presents an InputStream / OutputStream API northbound.
 */
public class StreamConnectionProcessor implements ConnectionProcessor, BufferWriter, BufferReader {

	private final RingByteBuffer inputBuffer;
    private final OutputRingByteBuffer outputRingByteBuffer;

    private Connection connection;

	public StreamConnectionProcessor(int bufferSize) {
		this.inputBuffer = new RingByteBuffer(bufferSize, this);
        this.outputRingByteBuffer = new OutputRingByteBuffer(bufferSize, this);
	}

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void close() {
        this.connection.close();
    }

    public InputStream getInputStream() {
        return this.inputBuffer.getInputStream();
    }

    public OutputStream getOutputStream() {
        return this.outputRingByteBuffer.getOutputStream();
    }

	@Override
	public void connectionClosed(Exception ex) {
        // Notify
        System.out.println("Connection closed: " + ex);
	}

	@Override
	public ByteBuffer getReadBuffer() {
		return this.inputBuffer.getEmptyBuffer();
	}

	@Override
	public void readComplete(ByteBuffer buffer) {
		this.inputBuffer.readComplete(buffer);
	}

	@Override
	public void writeComplete(ByteBuffer buffer) {
        this.outputRingByteBuffer.writeComplete(buffer);
	}

    @Override
    public void write(ByteBuffer buffer) {
        if (connection == null) {
            throw new IllegalStateException("Connection not set");
        }
        connection.write(buffer);
    }

    @Override
    public void readBufferAvailable() {
        if (connection == null) {
            throw new IllegalStateException("Connection not set");
        }
        connection.readBufferAvailable();
    }
}
