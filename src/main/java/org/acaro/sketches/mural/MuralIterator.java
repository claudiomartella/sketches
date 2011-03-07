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

import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.sketch.SketchHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuralIterator implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(MuralIterator.class);
	private FileInputStream file;
	private DataInputStream data;
	private String filename;
	private long timestamp;
	private int totalItems, readItems = 0;
	private byte dirty;
		
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
		if (++readItems == totalItems) {
			file.close();
		}
		
		return sketch;
	}

	public void close() throws IOException {
		this.file.close();
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
}
