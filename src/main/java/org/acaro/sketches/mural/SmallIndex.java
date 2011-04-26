package org.acaro.sketches.mural;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.acaro.sketches.util.Util;

public class SmallIndex implements Index {
	private MappedByteBuffer buffer;
	private int length;

	public SmallIndex(FileChannel channel, MapMode mode, long start, long length) throws IOException {
		if (length >= Integer.MAX_VALUE) 
			throw new IllegalArgumentException("length should be smaller than Integer.MAX_VALUE.");
		this.length = (int) length;
		this.buffer = channel.map(mode, start, (int) length);
	}
	
	public Index force() {
		buffer.force();
		return this;
	}

	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	public long position() {
		return this.position();
	}

	public Index position(long position) {
		checkPosition(position);
		buffer.position((int) position);
		
		return this;
	}

	public long getOffset() {
		return buffer.getLong();
	}

	public long getOffset(long position) {
		checkPosition(position);
		return buffer.getLong((int) position);
	}

	public Index putOffset(long offset) {
		buffer.putLong(offset);
		
		return this;
	}
	
	public Index putOffset(long position, long offset) {
		checkPosition(position);
		buffer.putLong((int) position, offset);
		
		return this;
	}
	
	private void checkPosition(long position) {
		if (position % Util.SIZEOF_LONG != 0) 
			throw new IllegalArgumentException("illegal position: " + position + ". It should be dividable by the size of long.");
		if (position >= length || position < 0) 
			throw new IndexOutOfBoundsException("position: " + position + " length: " + length);
	}
}
