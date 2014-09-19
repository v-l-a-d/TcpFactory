package net.bluemud.tcp.util;

import net.bluemud.tcp.api.ConnectionProcessor;

import java.nio.ByteBuffer;

/**
 * Connection processor that presents an InputStream / OutputStream API northbound.
 */
public class StreamConnectionProcessor implements ConnectionProcessor {

	private final RingByteBuffer inputBuffer;

	StreamConnectionProcessor(int bufferSize) {
		this.inputBuffer = new RingByteBuffer(bufferSize);
	}

	@Override
	public void connectionClosed(Exception ex) {

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

	}
}
