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
 * This is a Delete. Some NoSQLs call it Tombstone.
 * 
 * +----+---------+--------+---+
 * |  1 |    8    |    2   | N |
 * |Type|Timestamp|Key size|Key|
 * +----+---------+--------+---+
 */

public class Delete 
implements Operation {

	private byte[] key;
	private long ts;

	private Delete() { }

	public Delete(byte[] key){
		this.key = key;
		this.ts  = System.currentTimeMillis();
	}

	public Delete(byte[] key, long ts) {
		this.key = key;
		this.ts	 = ts;
	}

	@Override
	public byte[] getKey() {
		return this.key;
	}

	@Override
	public byte[] getValue() {
		return null;
	}

	@Override
	public int getSize() {
		return Sizes.SIZEOF_LONG + key.length;
	}

	@Override
	public long getTimestamp() {
		return this.ts;
	}

	public String toString() {
		return "Buff key: " + this.key + " ts: " + this.ts;
	}

	@Override
	public void readFrom(DataInput in) 
	throws IOException {

		this.ts  = in.readLong();
		short kl = in.readShort();
		byte[] buffer = new byte[kl];
		in.readFully(buffer);
		this.key = buffer;
	}

	@Override
	public void writeTo(DataOutput out) 
	throws IOException {

		out.writeByte(DELETE);
		out.writeLong(ts);
		out.writeShort((short) key.length);
		out.write(key);
	}

	public static Delete read(DataInput in) 
	throws IOException {

		Delete d = new Delete();
		d.readFrom(in);

		return d;
	}
}
