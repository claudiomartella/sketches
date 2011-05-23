package org.acaro.sketches.sfile;

import java.io.IOException;

import org.acaro.sketches.memstore.Memstore;
import org.acaro.sketches.operation.Operation;

public class RAMSFile implements SFile, Comparable<SFile> {
	private Memstore memory;
	private long timestamp;
	
	public RAMSFile(Memstore sketchReader) {
		this.memory    = sketchReader;
		this.timestamp = sketchReader.getTimestamp();
	}
	
	public Operation get(byte[] key) throws IOException {
		return memory.get(key);
	}

	public void close() throws IOException { 
		throw new UnsupportedOperationException("Can't close an RAMSFile");
	}

	public int compareTo(SFile other) {
		long otherTs = other.getTimestamp();
		
		if (this.timestamp > otherTs)
			return 1;
		else if (this.timestamp < otherTs)
			return -1;
		else
			return 0;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public boolean scribable() {
		return false;
	}

	public long getSize() {
		throw new UnsupportedOperationException("Can't return size of RAMSFile");
	}
}
