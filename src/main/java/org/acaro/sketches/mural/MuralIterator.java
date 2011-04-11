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

package org.acaro.sketches.mural;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.acaro.sketches.sketch.Buff;
import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.sketch.SketchHelper;
import org.acaro.sketches.sketch.Throwup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows sequential reading of a Mural. It offers an Iterator-like interface,
 * without actually implementing it (to avoid checked Exceptions).
 * 
 * @author Claudio Martella
 *
 */

public class MuralIterator implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(MuralIterator.class);
	private FileInputStream file;
	private DataInputStream data;
	private String filename;
	private long timestamp;
	private int totalItems, readItems = 0;
	private byte dirty;
	private long position = Mural.HEADER_SIZE;
	private int lastElementSize = 0;
		
	public MuralIterator(String filename) throws IOException {
		this.filename = filename;
		this.file = new FileInputStream(filename);
		this.data = new DataInputStream(new BufferedInputStream(file));
		init();
	}
	
	public boolean hasNext() {
		return readItems < totalItems;
	}

	public Sketch next() throws IOException {
		if (!hasNext()) throw new NoSuchElementException();
		
		Sketch sketch = SketchHelper.readItem(data);
		updateOffset(sketch.getSize());
		if (++readItems == totalItems) {
			file.close();
		}
		
		return sketch;
	}

	public void close() throws IOException {
		this.file.close();
	}
	
	public long getLastOffset() {
		return position-lastElementSize;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public int getNumberOfItems() {
		return this.totalItems;
	}
	
	public byte getDirtyByte() {
		return this.dirty;
	}
	
	private void init() throws IOException {
		this.dirty = data.readByte();
		if (dirty == Mural.DIRTY)
			logger.info("Mural is dirty: " + filename);
		this.timestamp = data.readLong();
		this.totalItems = data.readInt();
	}
	
	private void updateOffset(int size) {
		lastElementSize = size;
		position += size;
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("usage: MuralIterator <filename>");
			System.exit(-1);
		}
		
		MuralIterator iterator = new MuralIterator(args[1]);
		while (iterator.hasNext()) {
			Sketch s = iterator.next();
			if (s instanceof Buff)
				System.out.println("(Buff) key: " + new String(s.getKey(), "UTF-8") +
						"ts: " + s.getTimestamp());
			else if (s instanceof Throwup)
				System.out.println("(Throwup) key: " + new String(s.getKey(), "UTF-8") +
						"value: " + new String(((Throwup) s).getValue(), "UTF-8") +
						"ts: " + s.getTimestamp());
		}
		iterator.close();
	}
}
