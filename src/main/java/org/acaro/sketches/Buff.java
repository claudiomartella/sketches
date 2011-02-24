package org.acaro.sketches;

import java.nio.ByteBuffer;

/**
 * 
 * @author Claudio Martella
 * 
 * This represents a deleted value. Deleted values are purged during compaction.
 * This is what is normally known as a Tombstone in some NoSQL KV stores.
 *
 */
public class Buff implements Sketch {
	final private byte[] key;
	final private long ts;
	private byte[] header;
	
	public Buff(byte[] key){
		this.key    = key;
		this.ts = System.currentTimeMillis();
		initHeader();
	}
	
	public Buff(byte[] key, long ts) {
		this.key    = key;
		this.ts		= ts;
		initHeader();	
	}

	public ByteBuffer[] getBytes() {
		ByteBuffer[] tokens = { ByteBuffer.wrap(header), ByteBuffer.wrap(key) };
		
		return tokens;
	}
	
	public byte[] getKey() {
		return this.key;
	}
	
	public int getSize() {
		return key.length;
	}

	public long getTimestamp() {
		return this.ts;
	}
	
	private void initHeader() {
		this.header = ByteBuffer.allocate(HEADER_SIZE).put(THROWUP)
		.putLong(ts)
		.putShort((short) key.length)
		.putInt(0)
		.array();
	}
}
