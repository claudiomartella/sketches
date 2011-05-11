/* Copyright 2011 Claudio Martella

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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.io.SmartReader;


public class SketchHelper {

	public static Sketch readItem(MappedByteBuffer buffer) throws IOException {
		Sketch s;
		
		byte type = buffer.get();
		long ts = buffer.getLong();
		short keySize = buffer.getShort();
		int valueSize = buffer.getInt();
		
		byte[] key, value;
		
		switch (type) {
		
		case Sketch.THROWUP: 

			key = new byte[keySize];
			value = new byte[valueSize];
			buffer.get(key); 
			buffer.get(value);
			s = new Throwup(key, value, ts);
			break;

		case Sketch.BUFF:

			key = new byte[keySize];
			buffer.get(key);
			s = new Buff(key, ts); 
			break;

		default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
		}
		
		return s;
	}
	
	public static Sketch readItem(DataInputStream input) throws IOException {
		Sketch s;
		
		byte type = input.readByte();
		long ts = input.readLong();
		short keySize = input.readShort();
		int valueSize = input.readInt();
		
		byte[] key, value;
		
		switch (type) {
		case Sketch.THROWUP: 
			
			key = new byte[keySize];
			value = new byte[valueSize];
			input.readFully(key);
			input.readFully(value);
			s = new Throwup(key, value, ts);
			break;

		case Sketch.BUFF:

			key = new byte[keySize];
			input.readFully(key);
			s = new Buff(key, ts); 
			break;

		default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
		}
		
		return s;
	}
	
	public static Sketch readItem(RandomAccessFile input) throws IOException {
		Sketch s;
		
		byte type = input.readByte();
		long ts = input.readLong();
		short keySize = input.readShort();
		int valueSize = input.readInt();
		
		byte[] key, value;
		
		switch (type) {
		case Sketch.THROWUP: 
			
			key = new byte[keySize];
			value = new byte[valueSize];
			input.readFully(key);
			input.readFully(value);
			s = new Throwup(key, value, ts);
			break;

		case Sketch.BUFF:

			key = new byte[keySize];
			input.readFully(key);
			s = new Buff(key, ts); 
			break;

		default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
		}
		
		return s;
	}
	
	public static Sketch readItem(FileChannel channel) throws IOException {
		Sketch s;
		ByteBuffer buffer = ByteBuffer.allocate(Sketch.HEADER_SIZE);
		
		byte type = buffer.get();
		long ts = buffer.getLong();
		short keySize = buffer.getShort();
		int valueSize = buffer.getInt();
		
		ByteBuffer key, value;
		
		switch (type) {
		
		case Sketch.THROWUP: 

			key = ByteBuffer.allocate(keySize);
			value = ByteBuffer.allocate(valueSize);
			ByteBuffer[] payload = { key , value };
			while (channel.read(payload) > 0);
			s = new Throwup(key.array(), value.array(), ts);
			break;

		case Sketch.BUFF:

			key = ByteBuffer.allocate(keySize);
			while (channel.read(key) > 0);
			s = new Buff(key.array(), ts); 
			break;

		default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
		}
	
		return s;
	}
	
	public static Sketch readItem(SmartReader input) throws IOException {
		Sketch s;
		
		byte type = input.readByte();
		long ts = input.readLong();
		short keySize = input.readShort();
		int valueSize = input.readInt();
		
		byte[] key, value;
		
		switch (type) {
		case Sketch.THROWUP: 
			
			key = new byte[keySize];
			value = new byte[valueSize];
			input.read(key);
			input.read(value);
			s = new Throwup(key, value, ts);
			break;

		case Sketch.BUFF:

			key = new byte[keySize];
			input.read(key);
			s = new Buff(key, ts); 
			break;

		default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
		}
		
		return s;
	}
}
