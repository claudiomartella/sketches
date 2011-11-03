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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;

import org.acaro.sketches.io.SmartReader;
import org.acaro.sketches.operation.Delete;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.OperationHelper;
import org.acaro.sketches.operation.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows sequential reading of an FSSFile. It offers an Iterator-like interface,
 * without actually implementing it (to avoid checked Exceptions).
 * 
 * @author Claudio Martella
 *
 */

public class FSSFileIterator 
implements Closeable, Comparable<FSSFileIterator> {

	private static final Logger logger = LoggerFactory.getLogger(FSSFileIterator.class);
	private SmartReader reader;
	private String filename;
	private long timestamp;
	private long numberOfItems;
	private long readItems = 0;
	private float loadFactor;
	private byte dirtyByte;
	private long indexOffset;
	private long position = FSSFile.HEADER_SIZE;
	private int lastElementSize = 0;
	private long bloomOffset;
		
	public FSSFileIterator(String filename) 
	throws IOException {

		this.filename = filename;
		this.reader   = new SmartReader(new RandomAccessFile(filename, "r").getChannel());
		init();
	}
	
	public boolean hasNext() {
		return readItems < numberOfItems;
	}

	public Operation next() 
	throws IOException {
	
		if (!hasNext()) throw new NoSuchElementException();
		
		Operation o = OperationHelper.readOperation(reader);
		updateOffset(o.getSize());
		if (++readItems == numberOfItems)
			close();
		
		return o;
	}

	public void close() 
	throws IOException {
		this.reader.close();
	}
	
	public long getLastOffset() {
		return position - lastElementSize;
	}
	
	public long getIndexOffset() {
		return this.indexOffset;
	}
	
	public long getBloomFilterOffeset() {
		return this.bloomOffset;
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
	
	public byte getDirtyByte() {
		return this.dirtyByte;
	}
	
	public int compareTo(FSSFileIterator other) {
		long otherTs = other.getTimestamp();
		
		if (this.timestamp > otherTs)
			return 1;
		else if (this.timestamp < otherTs)
			return -1;
		else
			return 0;
	}
	
	private void init() 
	throws IOException {
		
		this.dirtyByte     = reader.readByte();
		this.timestamp     = reader.readLong();
		this.numberOfItems = reader.readLong();
		this.loadFactor    = reader.readFloat();
		this.indexOffset   = reader.readLong();
		this.bloomOffset   = reader.readLong();
	}
	
	private void updateOffset(int size) {
		lastElementSize = size;
		position += size;
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("usage: FSSFileIterator <filename>");
			System.exit(-1);
		}
		
		FSSFileIterator iterator = new FSSFileIterator(args[0]);
		while (iterator.hasNext()) {
			Operation s = iterator.next();
			if (s instanceof Delete)
				System.out.println("(Delete) key: " + new String(s.getKey(), "UTF-8") +
						"ts: " + s.getTimestamp());
			else if (s instanceof Update)
				System.out.println("(Update) key: " + new String(s.getKey(), "UTF-8") +
						"value: " + new String(((Update) s).getValue(), "UTF-8") +
						"ts: " + s.getTimestamp());
		}
		iterator.close();
	}
}
