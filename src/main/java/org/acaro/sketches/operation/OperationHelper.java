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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.io.SmartReader;


public class OperationHelper {
	
	public static Operation readOperation(DataInput in) 
	throws IOException {
	
		Operation o;
		
		byte type = in.readByte();
		switch (type) {
		
		case Operation.UPDATE:

			o = Update.read(in);
			break;
			
		case Operation.DELETE:
			
			o = Delete.read(in);
			break;
			
		default:
			throw new IllegalArgumentException("unknown type " + type);
		}
		
		return o;
	}
}
