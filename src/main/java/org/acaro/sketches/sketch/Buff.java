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
 * this is a Delete. Some NoSQLs call it Tombstone. "To remove painted 
 * graffiti with chemicals and other instruments, or to paint over it with 
 * a flat color."
 */

public class Buff implements Sketch {
	private final byte[] key;
	private final long ts;
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
		return header.length + key.length;
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
