package org.acaro.sketches.io;

import java.io.IOException;

import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.sfile.SFile;

public interface OperationReader 
extends Comparable<OperationReader> {
	
	public Operation get(byte[] key) throws IOException;
	public long getSize();
	public boolean isCompactable();
	public void close() throws IOException;
	public long getTimestamp();
}
