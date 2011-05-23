package org.acaro.sketches.sfile;

import java.io.IOException;

import org.acaro.sketches.io.OperationReader;
import org.acaro.sketches.operation.Operation;

public interface SFile extends OperationReader { 
	public Operation get(byte[] key) throws IOException;
	public void close() throws IOException;
	public boolean scribable();
	public int compareTo(SFile other);
	public long getTimestamp();
	public long getSize();
}
