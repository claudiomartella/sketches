package org.acaro.sketches.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.util.Util;

/**
 * 
 * @author Claudio Martella
 * 
 * This implements a smart buffered file writer. The smart comes from the i/o efficiency 
 * through buffering and from the fact it can tell you the file position where it is 
 * writing. You can pass a FileChannel to the constructor, so you can still seek to EOF 
 * before writing or, if you like, during usage (by flushing before seeking). 
 * 
 * Modified from krati.io.FastDataWriter
 *
 */
public class SmartWriter {
	private static final int DEFAULT_BUFFERSIZE = 65536;
	private static final int MAX_WRITE_RETRIES = 3;
	private FileChannel channel;
	private ByteBuffer buffer;

	public SmartWriter(FileChannel channel, int bufferSize) {
		this.channel = channel;
		this.buffer  = ByteBuffer.allocateDirect(bufferSize);
	}

	public SmartWriter(FileChannel channel) {
		this(channel, DEFAULT_BUFFERSIZE);
	}

	public void write(int b) throws IOException {
		checkAvailability(Util.SIZEOF_BYTE);
		buffer.put((byte) b);
	}

	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		int remaining = buffer.remaining();

		// if enough space remaining => we can simply copy it to the buffer
		if (remaining >= len) {
			buffer.put(b, off, len);
		} else {
			// not enough space left.. we squeeze as much as we can
			if (remaining > 0) {
				buffer.put(b, off, remaining);
				off += remaining;
				len -= remaining;
			}

			// we then flush the buffer completely
			flushBuffer();

			// if what we need to write fits in the buffer then we add it to the buffer
			if (len < buffer.capacity()) {
				buffer.put(b, off, len);
			} else {
				// no need to buffer => we stream it directly
				flushBuffer(ByteBuffer.wrap(b, off, len));
			}
		}
	}

	public void writeByte(int v) throws IOException {
		checkAvailability(Util.SIZEOF_BYTE);
		buffer.put((byte) v);
	}

	public void writeInt(int v) throws IOException {
		checkAvailability(Util.SIZEOF_INT);
		buffer.putInt(v);
	}

	public void writeShort(short v) throws IOException {
		checkAvailability(Util.SIZEOF_SHORT);
		buffer.putShort(v);
	}

	public void writeLong(long v) throws IOException {
		checkAvailability(Util.SIZEOF_LONG);
		buffer.putLong(v);
	}

	public void writeFloat(float v) throws IOException {
		checkAvailability(Util.SIZEOF_FLOAT);
		buffer.putFloat(v);
	}

	public void writeDouble(double v) throws IOException {
		checkAvailability(Util.SIZEOF_DOUBLE);
		buffer.putDouble(v);
	}

	public void flush() throws IOException {
		flushBuffer();
	}

	public void sync() throws IOException {
		flush();
		channel.force(true);
	}

	public void close() throws IOException {
		sync();
		channel.close();
	}

	public long getFilePointer() throws IOException {
		return channel.position() + buffer.position();
	}

	public FileChannel getChannel() {
		return this.channel;
	}

	private void flushBuffer() throws IOException {
		buffer.flip();
		flushBuffer(buffer);
		buffer.clear();
	}

	private void flushBuffer(ByteBuffer buffer) throws IOException {
		int retry = MAX_WRITE_RETRIES;
		int ret;

		while (buffer.remaining() > 0 && retry > 0) {
			ret = channel.write(buffer);
			if(ret == 0)
				retry--;
			else
				retry = MAX_WRITE_RETRIES;
		}

		if (buffer.remaining() > 0)
			throw new IOException("couldn't write the buffer after " + MAX_WRITE_RETRIES + " tries");
	}

	private void checkAvailability(int size) throws IOException {
		if (buffer.remaining() < size)
			flushBuffer();
	}
}
