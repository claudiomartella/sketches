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


package org.acaro.sketches.sketchbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.sketch.Sketch;

/**
 * 
 * @author Claudio Martella
 * 
 * Append-only log file.
 *
 */

public class SyncSketchBook implements SketchBook {
	private FileOutputStream file;
	private FileChannel ch;

	public SyncSketchBook(String filename) throws IOException {
		this.file = new FileOutputStream(filename, true);
		this.ch  = file.getChannel();
		init();
	}
	
	public synchronized void write(Sketch b) throws IOException {
		ByteBuffer[] bb = b.getBytes();
		while (ch.write(bb) > 0);
		sync();
	}
	
	public synchronized void close() throws IOException {
		ByteBuffer header = ByteBuffer.allocate(SketchBook.HEADER_SIZE);
		header.put(SketchBook.CLEAN);
		writeHeader(header);
		sync();
		file.close();
	}
	
	private void sync() throws IOException {
		ch.force(false);
	}
	
	private void init() throws IOException {
		ByteBuffer header = ByteBuffer.allocate(SketchBook.HEADER_SIZE);
		header.put(SketchBook.DIRTY);
		writeHeader(header);
		sync();
	}
	
	private void writeHeader(ByteBuffer header) throws IOException {
		ch.position(0);
		while (ch.write(header) > 0);
	}
}
