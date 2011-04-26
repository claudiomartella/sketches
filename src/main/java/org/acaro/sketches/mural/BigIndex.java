package org.acaro.sketches.mural;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.acaro.sketches.util.Util;

/**
 * 
 * @author Claudio Martella
 * 
 */

public class BigIndex implements Index {
	private static final long PAGE_SIZE = Integer.MAX_VALUE - 7; // biggest int dividable by 8
	private MappedByteBuffer buffers[];
	private long length;
	private long position;
	
	public BigIndex(FileChannel channel, MapMode mode, long start, long length) throws IOException {
		this.length   = length;
		this.position = 0;
		
		int n = (int) ((length % PAGE_SIZE == 0) ? length / PAGE_SIZE : length / PAGE_SIZE + 1);
		buffers = new MappedByteBuffer[n];

		long s = 0;
		long l = 0;
		for (long i = 0; s + l < length; i++) {
			if (i == n - 1) // last chunk
				l = (length - i * PAGE_SIZE);
			else // whole page
				l = PAGE_SIZE;
			
			s = i * PAGE_SIZE;
			buffers[(int) i] = channel.map(mode, start + s, l);
		}
	}
	
	public Index force() {
		for (MappedByteBuffer b: buffers)
			b.force();
		
		return this;
	}

	public boolean hasRemaining() {
		return this.position < this.length;
	}
	
	public long position() {
		return this.position;
	}
	
	public BigIndex position(long position) {
		checkPosition(position);
		this.position = position;
		
		return this;
	}
	
	public long getOffset() {
		long value = getOffset(position);
		increasePosition();
		
		return value;
	}
	
	public long getOffset(long position) {
		checkPosition(position);
        return buffers[getPage(position)].getLong(getIndex(position));
	}
	
	public Index putOffset(long offset) {
		putOffset(position, offset);
		increasePosition();
		
		return this;
	}
	
	public Index putOffset(long position, long offset) {
		checkPosition(position);
        buffers[getPage(position)].putLong(getIndex(position), offset);
        
        return this;
	}
	
	private int getPage(long position) {
		return (int) (position / PAGE_SIZE);
	}

	private int getIndex(long position) {
		return (int) (position % PAGE_SIZE);
	}
	
	private void increasePosition() {
		position += Util.SIZEOF_LONG;
	}
	
	private void checkPosition(long position) {
		if (position % Util.SIZEOF_LONG != 0) 
			throw new IllegalArgumentException("illegal position: " + position + ". It should be dividable by the size of long.");
		if (position >= length || position < 0) 
			throw new IndexOutOfBoundsException("position: " + position + " length: " + length);
	}
}
