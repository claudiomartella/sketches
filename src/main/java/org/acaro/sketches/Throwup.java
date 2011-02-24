package org.acaro.sketches;

import java.nio.ByteBuffer;

/**
 * 
 * @author Claudio Martella
 * 
 * This represents the new data written. A new write or an update of an existing entry.
 *
 */

public class Throwup implements Sketch {
	final private byte[] key;
	final private byte[] value;
	final private long ts;
	private byte[] header;
	
	public Throwup(byte[] key, byte[] value) {
		this.key    = key;
		this.value  = value;
		this.ts = System.currentTimeMillis();
		initHeader();
	}

	public Throwup(byte[] key, byte[] value, long ts) {
		this.key    = key;
		this.value  = value;
		this.ts		= ts;
		initHeader();	
	}

	public byte[] getKey() {
		return this.key;
	}
	
	public byte[] getValue() {
		return this.value;
	}

	public ByteBuffer[] getBytes() {
		ByteBuffer[] tokens = { ByteBuffer.wrap(header), ByteBuffer.wrap(key), ByteBuffer.wrap(value) };
		
		return tokens;
	}

	public int getSize() {
		return key.length + value.length;
	}

	public long getTimestamp() {
		return this.ts;
	}
	
	private void initHeader() {
		this.header = ByteBuffer.allocate(HEADER_SIZE).put(THROWUP)
		.putLong(ts)
		.putShort((short) key.length)
		.putInt(value.length)
		.array();
	}
}
