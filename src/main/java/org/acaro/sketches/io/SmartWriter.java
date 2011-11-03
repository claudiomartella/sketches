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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.utils.Sizes;

import com.google.common.base.Preconditions;

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
public class SmartWriter 
implements DataOutput {
	
	private static final int DEFAULT_BUFFERSIZE = 64*1024;
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

	public void write(int b) 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_BYTE);
		buffer.put((byte) b);
	}

	public void write(byte[] b) 
	throws IOException {
	
		write(b, 0, b.length);
	}

	public void write(byte[] b, int off, int len) 
	throws IOException {
	
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

	public void writeByte(int v) 
	throws IOException {
	
		write(v);
	}

	public void writeInt(int v) 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_INT);
		buffer.putInt(v);
	}

	public void writeShort(short v) 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_SHORT);
		buffer.putShort(v);
	}

	public void writeLong(long v) 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_LONG);
		buffer.putLong(v);
	}

	public void writeFloat(float v) 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_FLOAT);
		buffer.putFloat(v);
	}

	public void writeDouble(double v) 
	throws IOException {
	
		checkAvailability(Sizes.SIZEOF_DOUBLE);
		buffer.putDouble(v);
	}
	
	public void writeBoolean(boolean v) 
	throws IOException {
	
		write(v == true ? 1 : 0);
	}

	public void writeBytes(String s) 
	throws IOException {
		
		write(s.getBytes());
	}

	public void writeChar(int v) 
	throws IOException {

		checkAvailability(Sizes.SIZEOF_CHAR);
		buffer.putChar((char) v);
		
	}

	public void writeChars(String s) 
	throws IOException {
		
        int len = s.length();
        for (int i = 0 ; i < len ; i++)
            writeChar(s.charAt(i));
	}

	public void writeShort(int v) 
	throws IOException {

		checkAvailability(Sizes.SIZEOF_SHORT);
		buffer.putShort((short) v);
	}

	public void writeUTF(String s) 
	throws IOException {

		byte[] b = s.getBytes("UTF-8");
		writeShort((short) b.length);
		write(b);
	}

	public SmartWriter flush() 
	throws IOException {

		flushBuffer();
		
		return this;
	}

	public SmartWriter sync() 
	throws IOException {
	
		flush();
		channel.force(true);
		
		return this;
	}

	public SmartWriter close() 
	throws IOException {
	
		sync();
		channel.close();
		
		return this;
	}

	public long getFilePointer() 
	throws IOException {
	
		return channel.position() + buffer.position();
	}

	public long length()
	throws IOException {
		
		long fileSize = channel.size();
		long currSize = channel.position() + buffer.position();

		return fileSize < currSize ? currSize : fileSize;
	}
	
	/*
	 * Will not try to seek inside of buffer or make holes inside of the file. Just flush and move.
	 */
	public SmartWriter seek(long destination)
	throws IOException {
		
		Preconditions.checkArgument(destination <= length(), "cannot seek past EOF");
		
		flushBuffer();
		channel.position(destination);
		
		return this;
	}
	
	public FileChannel getChannel() {
		return this.channel;
	}

	private void flushBuffer() 
	throws IOException {
	
		buffer.flip();
		flushBuffer(buffer);
		buffer.clear();
	}

	private void flushBuffer(ByteBuffer buffer) 
	throws IOException {
	
		int retry = MAX_WRITE_RETRIES;
		int ret;

		while (buffer.remaining() > 0 && retry > 0) {
			ret = channel.write(buffer);
			if (ret == 0)
				retry--;
			else
				retry = MAX_WRITE_RETRIES;
		}

		if (buffer.remaining() > 0)
			throw new IOException("couldn't write the buffer after " + MAX_WRITE_RETRIES + " tries");
	}

	private void checkAvailability(int size) 
	throws IOException {
	
		if (buffer.remaining() < size)
			flushBuffer();
	}
}
