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

package org.acaro.sketches.sfile;

import java.io.IOException;

import org.acaro.sketches.io.OperationReader;
import org.acaro.sketches.memstore.Memstore;
import org.acaro.sketches.operation.Operation;

public class RAMSFile 
implements SFile {

	private Memstore memory;
	private long timestamp;
	
	public RAMSFile(Memstore memory) {
		
		this.memory    = memory;
		this.timestamp = memory.getTimestamp();
	}
	
	public Operation get(byte[] key) 
	throws IOException {
		return memory.get(key);
	}

	public void close() 
	throws IOException { 
		throw new UnsupportedOperationException("Can't close a RAMSFile");
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public boolean isCompactable() {
		return false;
	}

	public long getSize() {
		return this.memory.getSize();
	}
	
	public String getName() {
		throw new UnsupportedOperationException("RAMSFile doesn't have a filename");
	}

	public int compareTo(OperationReader other) {
		long otherTS = other.getTimestamp();
		
		if (timestamp < otherTS)
			return -1;
		else if (timestamp > otherTS)
			return 1;
		else
			return 0;
	}
}
