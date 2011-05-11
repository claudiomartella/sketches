package org.acaro.sketches.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.util.Util;

import com.google.common.base.Preconditions;

/**
 * This class implements a smart file reader. Smart comes from the i/o efficiency
 * through buffering, from the seekability and from the fact it can return its 
 * current position within the file.
 * 
 * TODO: do check EOFException for read(byte[], int, int)
 * 
 * @author Claudio Martella
 *
 */
public class SmartReader {
	private static final int DEFAULT_BUFFERSIZE = 64*1024;
	private static final int MAX_READ_RETRIES = 3;
	private FileChannel channel;
	private ByteBuffer buffer;
	private boolean hitEOF = false;
	private long size, left;
	
	public SmartReader(FileChannel channel, int bufferSize) throws IOException {
		this.buffer  = ByteBuffer.allocateDirect(bufferSize);
		this.channel = channel;
		this.size    = channel.size();
		this.left    = channel.position();
		fillBuffer(buffer);
	}
	
	public SmartReader(FileChannel channel) throws IOException {
		this(channel, DEFAULT_BUFFERSIZE);
	}
	
	public byte read() throws IOException {
		checkAvailability(Util.SIZEOF_BYTE);
		return buffer.get();
	}
	
	public void read(byte [] dst) throws IOException {
		read(dst, 0, dst.length);
	}
	
	public void read(byte [] dst, int off, int len) throws IOException {
		int remaining = buffer.remaining();

		// if all data is there => we can simply copy it from the buffer
		if (remaining >= len) {
			buffer.get(dst, off, len);
		} else {
			// not enough space left.. we squeeze as much as we can
			if (remaining > 0) {
				buffer.get(dst, off, remaining);
				off += remaining;
				len -= remaining;
			}

			// we then fill the buffer completely
			fillBuffer();

			// if what we need to read fits in then we take it to the buffer
			if (len < buffer.capacity()) 
				buffer.get(dst, off, len);
			else 
				// no need to buffer => we stream it directly
				fillBuffer(ByteBuffer.wrap(dst, off, len));
		}
	}
	
	public int readInt() throws IOException {
		checkAvailability(Util.SIZEOF_INT);
		return buffer.getInt();
	}
	
	public short readShort() throws IOException {
		checkAvailability(Util.SIZEOF_SHORT);
		return buffer.getShort();
	}
	
	public long readLong() throws IOException {
		checkAvailability(Util.SIZEOF_LONG);
		return buffer.getLong();
	}
	
	public float readFloat() throws IOException {
		checkAvailability(Util.SIZEOF_FLOAT);
		return buffer.getFloat();
	}
	
	public double readDouble() throws IOException {
		checkAvailability(Util.SIZEOF_DOUBLE);
		return buffer.getDouble();
	}
		
	public byte readByte() throws IOException {
		checkAvailability(Util.SIZEOF_BYTE);
		return buffer.get();
	}
	
	public boolean hasRemaining() {
		return buffer.hasRemaining() || !hitEOF;
	}
	
	public long getFilePointer() throws IOException {
		return left + buffer.position();
	}
	
	public FileChannel getChannel() {
		return this.channel;
	}
	
	public long length() throws IOException {
		return this.size;
	}
	
	public void seek(long destination) throws IOException {
		Preconditions.checkArgument(destination < size, "cannot seek past EOF");
		
		if (destination >= left && destination < left + buffer.limit())
			buffer.position((int) (destination - left)); // seek inside the buffer
		else  
			doSeek(destination);
	}
	
	public void close() throws IOException {
		channel.close();
	}
	
	private void checkAvailability(int size) throws IOException {
		if (buffer.remaining() >= size) 
			return;
		if (buffer.remaining() < size && !hitEOF)
			fillBuffer();
		if (buffer.remaining() < size && hitEOF)
        	throw new EOFException();
	}
	
	private void doSeek(long destination) throws IOException {
		channel.position(destination);
		left = destination;
		buffer.clear();
		fillBuffer(buffer);
	}

	private void fillBuffer() throws IOException {
		left += buffer.position();
		buffer.compact(); // don't loose data that might still be available
		fillBuffer(buffer);
	}
	
	private void fillBuffer(ByteBuffer buffer) throws IOException {
		int ret   = 0;
		int retry = MAX_READ_RETRIES;
		
		while (buffer.hasRemaining() && retry > 0) {
			ret = channel.read(buffer);
			if (ret > 0)
				retry = MAX_READ_RETRIES;
			else if (ret == 0) 
				retry--; // let's try again
			else // if (ret < 0) hit EOF! 
				break;
		}
		
		if (retry == 0)
			throw new IOException("couldn't read the buffer after " + MAX_READ_RETRIES + " tries");

		hitEOF = (ret < 0);

		buffer.flip();
	}
}
