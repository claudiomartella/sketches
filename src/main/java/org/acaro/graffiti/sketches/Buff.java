package org.acaro.graffiti.sketches;

import java.nio.ByteBuffer;

/**
 * 
 * @author Claudio Martella
 * 
 * This represents a deleted value. Deleted values are purged during compaction.
 * This is what is normally known as a Tombstone in NoSQL KV stores.
 *
 */
public class Buff extends Sketch {

	public Buff(byte[] key){
		super(key);
	}
	
	// FIXME: the header is created multiple times while the object is immutable.
	@Override
	public ByteBuffer[] getBytes() {
		ByteBuffer header = ByteBuffer.allocate(ENTRY_HEADER_LENGTH);
		header.put(BUFF);
		header.putLong(getTimestamp());
		header.putShort((short) getKey().array().length);
		header.putInt(0);
		header.rewind();
		
		ByteBuffer[] tokens = { header, getKey() };
		
		return tokens;
	}

	@Override
	public int getSize() {
		return 0;
	}
}
