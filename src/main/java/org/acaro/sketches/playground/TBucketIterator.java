package org.acaro.sketches.playground;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.acaro.sketches.io.BufferedRandomAccessFile;

class TBucketIterator {
	private long nextItem;
	private BufferedRandomAccessFile tfile;
	
	public TBucketIterator(long item, BufferedRandomAccessFile file) {
		this.nextItem = item;
		this.tfile    = file;
	}

	public boolean hasNext() {
		return nextItem != 0;
	}
	
	public long next() throws IOException {
		if (!hasNext()) throw new NoSuchElementException();
		
		return readItem();
	}
	
	private long readItem() throws IOException {
		tfile.seek(nextItem);
		nextItem = tfile.readLong();
		long offset = tfile.readLong();

		return offset;
	}
}
