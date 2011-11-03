package org.acaro.sketches.sfile.index;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.acaro.sketches.utils.Sizes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SmallIndex implements Index {
	private static final Logger logger = LoggerFactory.getLogger(SmallIndex.class);
	private MappedByteBuffer buffer;
	private int length;

	public SmallIndex(FileChannel channel, MapMode mode, long start, long length) throws IOException {
		Preconditions.checkArgument(length < IndexFactory.PAGE_SIZE, "length should be smaller than %s", IndexFactory.PAGE_SIZE);
		
		this.length = (int) length;
		this.buffer = channel.map(mode, start, (int) length);
	}
	
	public Index force() {
		buffer.force();
		return this;
	}

	public void load() {
		buffer.load();
	}
	
	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	public long position() {
		return buffer.position();
	}

	public Index position(long position) {
		assert position % Sizes.SIZEOF_LONG == 0: "illegal position: " + position +". It should be dividable by the size of long.";
		assert position < length && position >= 0: "position: " + position + " length: " + this.length;
		
		buffer.position((int) position);
		
		return this;
	}

	public long getOffset() {
		return buffer.getLong();
	}

	public long getOffset(long position) {
		assert position % Sizes.SIZEOF_LONG == 0: "illegal position: " + position +". It should be dividable by the size of long.";
		assert position < length && position >= 0: "position: " + position + " length: " + this.length;
		
		return buffer.getLong((int) position);
	}

	public Index putOffset(long offset) {
		buffer.putLong(offset);
		
		return this;
	}
	
	public Index putOffset(long position, long offset) {
		assert position % Sizes.SIZEOF_LONG == 0: "illegal position: " + position +". It should be dividable by the size of long.";
		assert position < length && position >= 0: "position: " + position + " length: " + this.length;
		
		buffer.putLong((int) position, offset);
		
		return this;
	}
}
