package org.acaro.graffiti.sketches;

import java.nio.ByteBuffer;

/**
 * 
 * @author Claudio Martella
 * 
 * This represents the new data written. A new write or an update of an existing entry.
 *
 */

public class Throwup extends Sketch {
	final private ByteBuffer value;

	public Throwup(byte[] key, byte[] value) {
		super(key);
		this.value = ByteBuffer.wrap(value);
	}

	public ByteBuffer getValue() {
		return this.value;
	}

	// FIXME: the header is created multiple times while the object is immutable.
	@Override
	public ByteBuffer[] getBytes() {
		ByteBuffer header = ByteBuffer.allocate(ENTRY_HEADER_LENGTH);
		header.put(THROWUP);
		header.putLong(getTimestamp());
		header.putShort((short) getKey().array().length);
		header.putInt(value.array().length);
		header.rewind();
		
		ByteBuffer[] tokens = { header, getKey(), value };
		
		return tokens;
	}

	@Override
	public int getSize() {
		return value.array().length;
	}
}
