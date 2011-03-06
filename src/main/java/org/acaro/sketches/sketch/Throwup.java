/*Copyright 2011 Claudio Martella

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


package org.acaro.sketches.sketch;

import java.nio.ByteBuffer;

/**
 * 
 * @author Claudio Martella
 * 
 * this is an Update. This means both a write, or an overwrite. 
 * 
 * "A throw-up or "throwie" sits between a tag and a piece in terms of complexity 
 * and time investment. It generally consists of a one-color outline and one layer 
 * of fill-color. Throw-ups are often utilized by writers who wish to achieve a 
 * large number of tags while competing with rival artists."
 */

public class Throwup implements Sketch {
	private final byte[] key;
	private final byte[] value;
	private final long ts;
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
		return header.length + key.length + value.length;
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
