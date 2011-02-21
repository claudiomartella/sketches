package org.acaro.sketches;

import java.nio.ByteBuffer;

public abstract class Sketch {
	final protected static byte THROWUP = 1;
	final protected static byte BUFF = 2;
	final protected static int HEADER_SIZE = Byte.SIZE+Long.SIZE+Short.SIZE+Integer.SIZE;
	
	final private long ts;
	final private ByteBuffer key;
	
	public Sketch(byte[] key) {
		this.ts  = System.currentTimeMillis();
		this.key = ByteBuffer.wrap(key);
	}
	
	public Sketch(ByteBuffer key, long ts) {
		this.ts  = ts;
		this.key = key;
	}

	public ByteBuffer getKey() {
		return this.key;
	}
	
	public long getTimestamp() {
		return this.ts;
	}
	
	public abstract ByteBuffer[] getBytes();
	
	public abstract int getSize();
}
