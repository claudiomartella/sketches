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
import org.acaro.sketches.io.OperationReader;
import org.acaro.sketches.io.SmartReader;
import org.acaro.sketches.sfile.index.Index;
import org.acaro.sketches.sfile.index.IndexFactory;
import org.acaro.sketches.utils.BloomFilter;
import org.acaro.sketches.utils.Configuration;
import org.acaro.sketches.utils.MurmurHash3;
import org.acaro.sketches.utils.Sizes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It's an immutable sorted list. This is where immutable Operations go. 
 * Data in these files can't be overwritten. Think of it as SequenceFile. 
 * 
 * @author Claudio Martella
 * 
 */

public class FSSFile 
implements SFile, Closeable {

	//									  dirty byte      - timestamp      - #items         - load factor     - index offset   - bloomfilter off
	public static final int HEADER_SIZE = Sizes.SIZEOF_BYTE+Sizes.SIZEOF_LONG+Sizes.SIZEOF_LONG+Sizes.SIZEOF_FLOAT+Sizes.SIZEOF_LONG+Sizes.SIZEOF_LONG;
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

	public FSSFile(String filename, int blockSize) 
	throws IOException {
	
		this.reader = new SmartReader(new RandomAccessFile(filename, "r").getChannel(), blockSize);
		readHeader();
		this.directorySize = (long) Math.floor((double) loadFactor * numberOfItems);
		this.index = IndexFactory.createIndex(reader.getChannel(), MapMode.READ_ONLY, indexOffset, bloomOffset-indexOffset);
		this.bloom = BloomFilter.deserialize(reader.seek(bloomOffset));
		this.index.load();
	}
	
	public FSSFile(String filename) 
	throws IOException {
	
		this(filename, Configuration.getConf().getInt("sketches.sfile.blocksize", 4096));
	}
	
	public Operation get(byte[] key) 
	throws IOException {
		
		Operation o;
		
		if (!bloom.isPresent(key))
			return null;
		
		long offset = getBucket(key);
		if (offset == 0)
			o = null;
		else if (offset < indexOffset) // direct link to data
			o = getItem(offset);
		else // search in the bucket
			o = searchItem(offset - indexOffset, key);
		
		return o;
	}
	
	public void close() 
	throws IOException {
	
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
	
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public int compareTo(OperationReader other) {
		long otherTs = other.getTimestamp();
		
		if (this.timestamp > otherTs)
			return 1;
		else if (this.timestamp < otherTs)
			return -1;
		else
			return 0;
	}
	
	public boolean isCompactable() {
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
	
	private Operation getItem(long offset) 
	throws IOException {
	
		reader.seek(offset);
		
		return OperationHelper.readOperation(reader);
	}
	
	private Operation searchItem(long offset, byte[] key) 
	throws IOException {
	
		long next = offset;
		long data;
		
		do {
			
			index.position(next);
			
			next = index.getOffset();
			data = index.getOffset();
			
			Operation o = getItem(data);
			
			if (Arrays.equals(o.getKey(), key))
				return o;
			
		} while (next != 0);
		
		return null;
	}
	
	private void readHeader() 
	throws IOException {
	
		dirtyByte     = reader.readByte(); // should handle DIRTY file
		timestamp     = reader.readLong();
		numberOfItems = reader.readLong();
		loadFactor    = reader.readFloat();
		indexOffset   = reader.readLong();	
		bloomOffset   = reader.readLong();
	}
	
	public static void main(String[] args) 
	throws IOException {
	
		if (args.length != 2) {
			System.out.println("usage: FSSFile <fssfile filename> <key>");
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
