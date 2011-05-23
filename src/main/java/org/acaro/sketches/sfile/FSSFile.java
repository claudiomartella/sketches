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

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

import org.acaro.sketches.operation.Delete;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.OperationHelper;
import org.acaro.sketches.operation.Update;
import org.acaro.sketches.playground.T5Miterator;
import org.acaro.sketches.io.SmartReader;
import org.acaro.sketches.sfile.index.Index;
import org.acaro.sketches.sfile.index.IndexFactory;
import org.acaro.sketches.util.BloomFilter;
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

public class FSSFile implements SFile, Closeable, Comparable<SFile> {
	private static final Logger logger = LoggerFactory.getLogger(FSSFile.class);
	//									  dirty byte      - timestamp      - #items         - load factor     - index offset   - bloomfilter off
	public static final int HEADER_SIZE = Util.SIZEOF_BYTE+Util.SIZEOF_LONG+Util.SIZEOF_LONG+Util.SIZEOF_FLOAT+Util.SIZEOF_LONG+Util.SIZEOF_LONG;
	public static final byte CLEAN = 0;
	public static final byte DIRTY = 1;
	private Index index;
	private BloomFilter bloom;
	private SmartReader reader;
	private byte dirtyByte;
	private long timestamp;
	private long indexOffset;
	private long bloomOffset;
	private long numberOfItems;
	private long directorySize;
	private float loadFactor;

	public FSSFile(String filename) throws IOException {
		this.reader = new SmartReader(new RandomAccessFile(filename, "r").getChannel());
		readHeader();
		this.directorySize = (long) Math.floor((double) loadFactor * numberOfItems);
		this.index = IndexFactory.createIndex(reader.getChannel(), MapMode.READ_ONLY, indexOffset, bloomOffset-indexOffset);
		this.bloom = BloomFilter.deserialize(reader.seek(bloomOffset));
	}
	
	public Operation get(byte[] key) throws IOException {
		Operation o;
		
		if (!bloom.isPresent(key))
			return null;
		
		long offset = getBucket(key);
		if (offset == 0)
			o = null;
		else if (offset < indexOffset) // direct link to data
			o = getOperation(offset);
		else // search in the bucket
			o = searchOperation(offset - indexOffset, key);
		
		return o;
	}
	
	public void close() throws IOException {
		reader.close();
	}
	
	public byte getDirtyByte() {
		return this.dirtyByte;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public long getNumberOfItems() {
		return this.numberOfItems;
	}
	
	public float getLoadFactor() {
		return this.loadFactor;
	}
	
	public long getIndexOffset() {
		return this.indexOffset;
	}
	
	public long getBloomFilterOffset() {
		return this.bloomOffset;
	}
	
	public int compareTo(SFile other) {
		long otherTs = other.getTimestamp();
		
		if (this.timestamp > otherTs)
			return 1;
		else if (this.timestamp < otherTs)
			return -1;
		else
			return 0;
	}
	
	public boolean scribable() {
		return true;
	}
	
	public long getSize() {
		return indexOffset - HEADER_SIZE;
	}
	
	private long getBucket(byte[] key) {
		return index.getOffset(calculateBucket(key) << 3);
	}
	
	private long calculateBucket(byte[] key) {
		return (MurmurHash3.hash(key) & 0x7fffffffffffffffL) % directorySize;
	}
	
	private Operation getOperation(long offset) throws IOException {
		reader.seek(offset);
		
		return OperationHelper.readItem(reader);
	}
	
	private Operation searchOperation(long offset, byte[] key) throws IOException {
		long next = offset;
		long data;
		
		do {
			index.position(next);
			
			next = index.getOffset();
			data = index.getOffset();
			
			Operation o = getOperation(data);
			
			if (Arrays.equals(o.getKey(), key))
				return o;
			
		} while (next != 0);
		
		return null;
	}
	
	private void readHeader() throws IOException {
		dirtyByte     = reader.readByte(); // should handle DIRTY file
		timestamp     = reader.readLong();
		numberOfItems = reader.readLong();
		loadFactor    = reader.readFloat();
		indexOffset   = reader.readLong();	
		bloomOffset   = reader.readLong();
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: Mural <mural filename> <key>");
			System.exit(0);
		}
		
		FSSFile sfile = new FSSFile(args[0]);
		Operation s = sfile.get(args[1].getBytes());
		if (s != null) {
			System.out.println("item found:");
			if (s instanceof Update) {
				Update t = (Update) s;
				System.out.print("key: " + t.getKey());
				System.out.print("ts: " + t.getTimestamp());
				System.out.println("value: " + t.getValue());
			} else {
				Delete b = (Delete) s;
				System.out.print("key: " + b.getKey());
				System.out.print("ts: " + b.getTimestamp());
				System.out.println("deleted!");
			}
		} else {
			System.out.println("item doesn't exist!");
		}
		
		sfile.close();
	}
}
