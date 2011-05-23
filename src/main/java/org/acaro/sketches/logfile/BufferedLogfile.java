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


package org.acaro.sketches.logfile;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.operation.Operation;

public class BufferedLogfile implements Logfile {
	private FileOutputStream file;
	private BufferedOutputStream bos;
	private final int bufferSize = 64*1024;

	public BufferedLogfile(String filename) throws IOException {
		this.file = new FileOutputStream(filename, true);
		this.bos  = new BufferedOutputStream(file, bufferSize);
		init();
	}
	
	public synchronized void write(Operation o) throws IOException {
		for (ByteBuffer buffer: o.getBytes())
			bos.write(buffer.array());
	}

	public synchronized void close() throws IOException {
		ByteBuffer header = ByteBuffer.allocate(Logfile.HEADER_SIZE);
		header.put(Logfile.CLEAN);
		writeHeader(header);
		sync();
		bos.close();
	}

	public synchronized void flush() throws IOException {
		bos.flush();
	}
	
	public synchronized void sync() throws IOException {
		bos.flush();
		file.getFD().sync();
	}
	
	private void init() throws IOException {
		ByteBuffer header = ByteBuffer.allocate(Logfile.HEADER_SIZE);
		header.put(Logfile.DIRTY);
		writeHeader(header);
		sync();
	}
	
	private void writeHeader(ByteBuffer header) throws IOException {
		FileChannel channel = file.getChannel();
		channel.position(0);
		while (channel.write(header) > 0);
	}
}
