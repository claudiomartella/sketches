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

package org.acaro.sketches.logfiles;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.acaro.sketches.io.SmartWriter;
import org.acaro.sketches.io.Writable;

public class BufferedLogfile 
implements Logfile {

	private SmartWriter writer;
	private String filename;
	private boolean sync;

	public BufferedLogfile(String filename)
	throws IOException {
		
		this(filename, false);
	}
	
	public BufferedLogfile(String filename, boolean sync) 
	throws IOException {
	
		this(filename, sync, false);
	}
	
	public BufferedLogfile(String filename, boolean sync, boolean append) 
	throws IOException {

		this.writer = new SmartWriter(new RandomAccessFile(filename, "rw").getChannel());
		this.sync   = sync;
		if (append) 
			writer.seek(writer.length());
	}

	public synchronized Logfile write(Writable o) 
	throws IOException {
	
		o.writeTo(writer);
		if (sync)
			sync();
		
		return this;
	}

	public synchronized void close() 
	throws IOException {
	
		sync();
		writer.close();
	}

	public synchronized Logfile flush() 
	throws IOException {
	
		writer.flush();
		
		return this;
	}
	
	public synchronized Logfile sync() 
	throws IOException {
	
		writer.flush();
		writer.sync();
		
		return this;
	}
	
	public String getName() {
		return this.filename;
	}
}
