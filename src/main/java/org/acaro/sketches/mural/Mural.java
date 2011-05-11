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

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

import org.acaro.sketches.playground.T5MInserter;
import org.acaro.sketches.sketch.Buff;
import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.sketch.SketchHelper;
import org.acaro.sketches.sketch.Throwup;
import org.acaro.sketches.io.BufferedRandomAccessFile;
import org.acaro.sketches.util.MurmurHash3;
import org.acaro.sketches.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It's an immutable sorted list. This is where immutable Sketches go. 
 * Data in these files can't be overwritten. Think of it as SequenceFile. 
 * 
 * "A large and labor-intensive graffiti painting."
 * 
 * @author Claudio Martella
 * 
 */

public class Mural implements Closeable, Comparable<Mural> {
	private static final Logger logger = LoggerFactory.getLogger(Mural.class);
	//									  dirty byte      - timestamp      - #items        - index offset
	public static final int HEADER_SIZE = Util.SIZEOF_BYTE+Util.SIZEOF_LONG+Util.SIZEOF_INT+Util.SIZEOF_LONG;
	public static final int BUCKET_ITEM_SIZE = Util.SIZEOF_LONG;
	public static final byte CLEAN = 0;
	public static final byte DIRTY = 1;
	private Index index;
	private BufferedRandomAccessFile file;
	private long timestamp;
	private long indexOffset;
	private byte dirtyByte;
	private int numberOfItems;
	
	public Mural(String muralFilename) throws IOException {
		this.file = new BufferedRandomAccessFile(muralFilename, "r");
		readHeader();
		this.index = new SmallIndex(this.file.getChannel(), MapMode.READ_ONLY, indexOffset, this.file.length()-indexOffset);
	}
	
	public Sketch get(byte[] key) throws IOException {
		Sketch s;

		long offset = getBucket(key);
		if (offset == 0) {// empty bucket
			//logger.debug("empty bucket");
			s = null;
		} else if (offset < indexOffset) {// direct link to data
			//logger.debug("got it directly");
			s = getSketch(offset);
		} else { // search in bucket
			//logger.debug("searching in the list");
			s = searchSketch(offset - indexOffset, key); 
		}
		return s;
	}
	
	public void close() throws IOException {
		file.close();
	}
	
	public byte getDirtyByte() {
		return this.dirtyByte;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public int getNumberOfItems() {
		return this.numberOfItems;
	}
	
	public long getIndexOffset() {
		return this.indexOffset;
	}
	
	private long getBucket(byte[] key) {
		return index.getOffset(calculateBucket(key) * Util.SIZEOF_LONG);
	}
	
	public int compareTo(Mural other) {
		long otherTs = other.getTimestamp();
		
		if (this.timestamp > otherTs)
			return 1;
		else if (this.timestamp < otherTs)
			return -1;
		else
			return 0;
	}
	
	private int calculateBucket(byte[] key) {
		return Math.abs(MurmurHash3.hash(key)) % numberOfItems;
	}
	
	private Sketch getSketch(long offset) throws IOException {
		file.seek(offset);
		
		return SketchHelper.readItem(file);
	}
	
	private Sketch searchSketch(long offset, byte[] key) throws IOException {
		index.position(offset);
		
		long dataOffset;
		while ((dataOffset = index.getOffset()) != 0) {
			Sketch t = getSketch(dataOffset);
			
			if (Arrays.equals(t.getKey(), key))
				return t;
		}
		
		return null;
	}
	
	private void readHeader() throws IOException {
		dirtyByte     = file.readByte(); // should handle DIRTY file
		timestamp     = file.readLong();
		numberOfItems = file.readInt();
		indexOffset   = file.readLong();		
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: Mural <mural filename> <key>");
			System.exit(0);
		}
		
		Mural mural = new Mural(args[0]);
		Sketch s = mural.get(args[1].getBytes());
		if (s != null) {
			System.out.println("item found:");
			if (s instanceof Throwup) {
				Throwup t = (Throwup) s;
				System.out.print("key: " + t.getKey());
				System.out.print("ts: " + t.getTimestamp());
				System.out.println("value: " + t.getValue());
			} else {
				Buff b = (Buff) s;
				System.out.print("key: " + b.getKey());
				System.out.print("ts: " + b.getTimestamp());
				System.out.println("deleted!");
			}
		} else {
			System.out.println("item doesn't exist!");
		}
		
		mural.close();
	}
}
