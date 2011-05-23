package org.acaro.sketches.io;

import java.io.IOException;

import org.acaro.sketches.operation.Operation;

public interface OperationReader {
	public Operation get(byte[] key) throws IOException;
	public long getSize();
}
