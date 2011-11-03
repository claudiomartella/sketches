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

package org.acaro.sketches.operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.acaro.sketches.utils.Sizes;

/**
 * 
 * @author Claudio Martella
 * 
 * This is an Update. This means either an add, an overwrite or a delete. 
 *  
 * +----+---------+--------+----------+---+-----+
 * |  1 |    8    |   2    |    4     | N |  N  |
 * |Type|Timestamp|Key size|Value size|Key|Value|
 * +----+---------+--------+----------+---+-----+
 */

public class Update 
implements Operation {

	private byte[] key;
	private byte[] value;
	private long ts;

	private Update() { }

	public Update(byte[] key, byte[] value) {
		this.key    = key;
		this.value  = value;
		this.ts = System.currentTimeMillis();
	}

	public Update(byte[] key, byte[] value, long ts) {
		this.key    = key;
		this.value  = value;
		this.ts		= ts;
	}

	public byte[] getKey() {
		return this.key;
	}

	public byte[] getValue() {
		return this.value;
	}

	public int getSize() {
		return Sizes.SIZEOF_LONG + key.length + value.length;
	}

	public long getTimestamp() {
		return this.ts;
	}

	public String toString() {
		return "Throwup key: " + this.key + " value: " + this.value + " ts: " + this.ts;
	}

	public void readFrom(DataInput in) 
	throws IOException {

		this.ts  = in.readLong();
		short kl = in.readShort();
		int   vl = in.readInt();
		byte[] kbuffer = new byte[kl];
		byte[] vbuffer = new byte[vl];
		in.readFully(kbuffer);
		in.readFully(vbuffer);
		this.key   = kbuffer;
		this.value = vbuffer;
	}

	public void writeTo(DataOutput out) 
	throws IOException {

		out.writeByte(UPDATE);
		out.writeLong(ts);
		out.writeShort((short) key.length);
		out.writeInt(value.length);
		out.write(key);
		out.write(value);
	}
	
	public static Update read(DataInput in) 
	throws IOException {
		
		Update u = new Update();
		u.readFrom(in);
		
		return u;
	}
}
