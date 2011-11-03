package org.acaro.sketches.io;

import java.io.IOException;

import org.acaro.sketches.operation.Operation;

public interface OperationMutator {
	public void put(byte[] key, Operation sketch) throws IOException;
	public void delete(byte[] key) throws IOException;
	public void flush() throws IOException;
	public long getSize();
}
