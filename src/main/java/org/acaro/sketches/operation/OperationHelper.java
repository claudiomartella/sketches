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

package org.acaro.sketches.operation;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.io.SmartReader;


public class OperationHelper {

	public static Operation readItem(MappedByteBuffer buffer) throws IOException {
		Operation o;
		
		byte type = buffer.get();
		long ts = buffer.getLong();
		short keySize = buffer.getShort();
		int valueSize = buffer.getInt();
		
		byte[] key, value;
		
		switch (type) {
		
		case Operation.UPDATE: 

			key = new byte[keySize];
			value = new byte[valueSize];
			buffer.get(key); 
			buffer.get(value);
			o = new Update(key, value, ts);
			break;

		case Operation.DELETE:

			key = new byte[keySize];
			buffer.get(key);
			o = new Delete(key, ts); 
			break;

		default: throw new IOException("Corrupted Logfile: read unknown type: " + type); 
		}
		
		return o;
	}
	
	public static Operation readItem(DataInputStream input) throws IOException {
		Operation o;
		
		byte type = input.readByte();
		long ts = input.readLong();
		short keySize = input.readShort();
		int valueSize = input.readInt();
		
		byte[] key, value;
		
		switch (type) {
		case Operation.UPDATE: 
			
			key = new byte[keySize];
			value = new byte[valueSize];
			input.readFully(key);
			input.readFully(value);
			o = new Update(key, value, ts);
			break;

		case Operation.DELETE:

			key = new byte[keySize];
			input.readFully(key);
			o = new Delete(key, ts); 
			break;

		default: throw new IOException("Corrupted Logfile: read unknown type: " + type); 
		}
		
		return o;
	}
	
	public static Operation readItem(RandomAccessFile input) throws IOException {
		Operation o;
		
		byte type = input.readByte();
		long ts = input.readLong();
		short keySize = input.readShort();
		int valueSize = input.readInt();
		
		byte[] key, value;
		
		switch (type) {
		case Operation.UPDATE: 
			
			key = new byte[keySize];
			value = new byte[valueSize];
			input.readFully(key);
			input.readFully(value);
			o = new Update(key, value, ts);
			break;

		case Operation.DELETE:

			key = new byte[keySize];
			input.readFully(key);
			o = new Delete(key, ts); 
			break;

		default: throw new IOException("Corrupted Logfile: read unknown type: " + type); 
		}
		
		return o;
	}
	
	public static Operation readItem(FileChannel channel) throws IOException {
		Operation o;
		ByteBuffer buffer = ByteBuffer.allocate(Operation.HEADER_SIZE);
		
		byte type = buffer.get();
		long ts = buffer.getLong();
		short keySize = buffer.getShort();
		int valueSize = buffer.getInt();
		
		ByteBuffer key, value;
		
		switch (type) {
		
		case Operation.UPDATE: 

			key = ByteBuffer.allocate(keySize);
			value = ByteBuffer.allocate(valueSize);
			ByteBuffer[] payload = { key , value };
			while (channel.read(payload) > 0);
			o = new Update(key.array(), value.array(), ts);
			break;

		case Operation.DELETE:

			key = ByteBuffer.allocate(keySize);
			while (channel.read(key) > 0);
			o = new Delete(key.array(), ts); 
			break;

		default: throw new IOException("Corrupted Logfile: read unknown type: " + type); 
		}
	
		return o;
	}
	
	public static Operation readItem(SmartReader input) throws IOException {
		Operation o;
		
		byte type = input.readByte();
		long ts = input.readLong();
		short keySize = input.readShort();
		int valueSize = input.readInt();
		
		byte[] key, value;
		
		switch (type) {
		case Operation.UPDATE: 
			
			key = new byte[keySize];
			value = new byte[valueSize];
			input.read(key);
			input.read(value);
			o = new Update(key, value, ts);
			break;

		case Operation.DELETE:

			key = new byte[keySize];
			input.read(key);
			o = new Delete(key, ts); 
			break;

		default: throw new IOException("Corrupted Logfile: read unknown type: " + type); 
		}
		
		return o;
	}
}
