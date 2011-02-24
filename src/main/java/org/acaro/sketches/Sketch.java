package org.acaro.sketches;

import java.nio.ByteBuffer;

public interface Sketch {
	final static byte THROWUP = 1;
	final static byte BUFF = 2;
	// sizeof(byte)+sizeof(long)+sizeof(short)+sizeof(int)
	final static int HEADER_SIZE = 15;
	
	public byte[] getKey();
	
	public long getTimestamp();
	
	public abstract ByteBuffer[] getBytes();
	
	public abstract int getSize();
}
