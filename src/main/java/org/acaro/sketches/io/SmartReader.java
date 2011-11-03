/* Copyright 2011 Claudio Martella

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package org.acaro.sketches.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.utils.Sizes;

import com.google.common.base.Preconditions;

/**
 * This class implements a smart file reader. Smart comes from the i/o efficiency
 * through buffering, from the seekability and from the fact it can return its 
 * current position within the file.
 * 
 * TODO: check EOFException for read(byte[], int, int)
 * 
 * @author Claudio Martella
 *
 */
public class SmartReader 
implements DataInput {

	private static final int DEFAULT_BUFFERSIZE = 64*1024;
	private static final int MAX_READ_RETRIES = 3;
	private FileChannel channel;
	private ByteBuffer buffer;
	private boolean hitEOF = false;
	private long size, left;

	public SmartReader(FileChannel channel, int bufferSize) 
	throws IOException {
	
		this.buffer  = ByteBuffer.allocateDirect(bufferSize);
		this.channel = channel;
		this.size    = channel.size();
		this.left    = channel.position();
		fillBuffer(buffer);
	}

	public SmartReader(FileChannel channel) 
	throws IOException {
	
		this(channel, DEFAULT_BUFFERSIZE);
	}

	public byte read() 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_BYTE);
		return buffer.get();
	}

	public void read(byte [] dst) 
	throws IOException {
	
		read(dst, 0, dst.length);
	}

	public void read(byte [] dst, int off, int len) 
	throws IOException {
	
		int remaining = buffer.remaining();

		// if all data is there => we can simply copy it from the buffer
		if (remaining >= len) {
			buffer.get(dst, off, len);
		} else {
			// not enough space left... we squeeze as much as we can
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

	public int readInt() 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_INT);
		return buffer.getInt();
	}

	public short readShort() 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_SHORT);
		return buffer.getShort();
	}

	public long readLong() 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_LONG);
		return buffer.getLong();
	}

	public float readFloat() 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_FLOAT);
		return buffer.getFloat();
	}

	public double readDouble() 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_DOUBLE);
		return buffer.getDouble();
	}

	public byte readByte() 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_BYTE);
		return buffer.get();
	}
	
	public boolean readBoolean() 
	throws IOException {

		return read() != 0;
	}

	public char readChar() 
	throws IOException {

		checkAvailability(Sizes.SIZEOF_CHAR);
		return buffer.getChar();
	}

	public void readFully(byte[] b) 
	throws IOException {
		
		read(b);
	}

	public void readFully(byte[] b, int length, int offset) 
	throws IOException {
		
		read(b, length, offset);
	}

	public String readLine() 
	throws IOException {

		throw new UnsupportedOperationException("readLine not supported");
	}

	public String readUTF() 
	throws IOException {

		short l  = readShort();
		byte[] b = new byte[l];
		readFully(b);
		
		return new String(b, "UTF-8");
	}

	public int readUnsignedByte() 
	throws IOException {

		int ch = read();
		
		return ch;
	}

	public int readUnsignedShort() 
	throws IOException {

		int ch1 = read();
		int ch2 = read();

		return (ch1 << 8) + (ch2 << 0);
	}

	public int skipBytes(int n) 
	throws IOException {

		long current = getFilePointer();
		long size    = length();
		
		// skipBytes() should never throw EOFException
		int actual = current + n > size ? (int) (size - current) : n;
		
		seek(getFilePointer() + actual);
		
		return actual;
	}

	public boolean hasRemaining() {
		return buffer.hasRemaining() || !hitEOF;
	}

	public long getFilePointer() 
	throws IOException {
	
		return left + buffer.position();
	}

	public FileChannel getChannel() {
		return this.channel;
	}

	public long length() 
	throws IOException {
	
		return this.size;
	}

	public SmartReader seek(long destination) 
	throws IOException {
	
		Preconditions.checkArgument(destination <= length(), "cannot seek past EOF");

		if (destination >= left && destination < left + buffer.limit())
			buffer.position((int) (destination - left)); // seek inside the buffer
		else  
			doSeek(destination);

		return this;
	}

	public SmartReader close() 
	throws IOException {
	
		channel.close();

		return this;
	}

	private void checkAvailability(int amount) 
	throws IOException {
	
		if (buffer.remaining() >= amount) 
			return;
		if (buffer.remaining() < amount && !hitEOF)
			fillBuffer();
		if (buffer.remaining() < amount && hitEOF)
			throw new EOFException();
	}

	private void doSeek(long destination) 
	throws IOException {
	
		channel.position(destination);
		left = destination;
		buffer.clear();
		fillBuffer(buffer);
	}

	private void fillBuffer() 
	throws IOException {
	
		left += buffer.position();
		buffer.compact(); // don't loose data that might still be available
		fillBuffer(buffer);
	}

	private void fillBuffer(ByteBuffer buffer) 
	throws IOException {

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
