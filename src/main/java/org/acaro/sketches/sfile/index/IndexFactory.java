package org.acaro.sketches.sfile.index;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class IndexFactory {
	protected static final long PAGE_SIZE = Integer.MAX_VALUE - 7;
	
	public static Index createIndex(FileChannel channel, MapMode mode, long start, long length) throws IOException {
		if (length >= PAGE_SIZE)
			return new BigIndex(channel, mode, start, length);
		else
			return new SmallIndex(channel, mode, start, length);
	}
}
