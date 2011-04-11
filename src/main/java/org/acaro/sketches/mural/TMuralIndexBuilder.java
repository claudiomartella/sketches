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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Callable;

import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.util.MurmurHash3;
import org.acaro.sketches.util.Util;

/**
 * @author Claudio Martella
 * 
 * This class implements the writing of the MuralIndex by iterating through the Mural and
 * recording each element's position. Collisions are handled through lists. 
 * The index is first built in a temporary file and later appended at the end of the Mural.
 * For write performance, each new list element is written at the end of the temoporary file
 * and spreads the list across the whole file.
 * When the index is appended to the Mural, the content of the bucket, is compacted too. 
 * This meanins that the list content is written sequentially in a whole block so that it 
 * can be read quickly by the MuraIndex.  
 *
 */
public class TMuralIndexBuilder implements Callable<String> {
	public static final int HEADER_SIZE = Util.SIZEOF_BYTE + Util.SIZEOF_INT;
	public static final byte CLEAN = 1;
	public static final byte DIRTY = 2;
	private MuralIterator muralIterator;
	private RandomAccessFile file;
	private MappedByteBuffer buckets;
	private String indexFilename;
	private int totalItems;
	
	public TMuralIndexBuilder(String muralFilename) throws IOException {
		this.muralIterator = new MuralIterator(muralFilename);
		this.totalItems = muralIterator.getNumberOfItems();
		this.indexFilename = muralFilename + ".tmp.idx";
		this.file = new RandomAccessFile(indexFilename, "rw");
		init();
	}
	
	public String call() throws IOException {
		while (muralIterator.hasNext()) {
			Sketch item = muralIterator.next(); // what is it?
			long offset = muralIterator.getLastOffset(); // where is it?
			writeToBucket(calculateBucket(item.getKey()), item, offset);
		}
		close();
		
		return indexFilename;
	}
	
	private void close() throws IOException {
		muralIterator.close();
		buckets.force();
		file.seek(0);
		file.writeByte(CLEAN);
		file.getFD().sync();
		file.close();
	}
	
	private void createBuckets() throws IOException {
		// XXX: this can get big. should be split in a smaller chunk + a for loop
		byte[] b = new byte[MuralIndex.BUCKET_ITEM_SIZE*totalItems];
		file.write(b);
		buckets = file.getChannel().map(MapMode.READ_WRITE, HEADER_SIZE, MuralIndex.BUCKET_ITEM_SIZE*totalItems);
	}
	
	private int calculateBucket(byte[] key) {
		return MurmurHash3.hash(key) % totalItems;
	}
	
	private void writeToBucket(int bucket, Sketch item, long offset) throws IOException {
		buckets.position(MuralIndex.BUCKET_ITEM_SIZE*bucket);
		buckets.mark();
		long last = buckets.getLong();
		long position = writeItem(item, last, offset);
		buckets.reset();
		buckets.putLong(position);
	}

	private long writeItem(Sketch item, long prev, long offset) throws IOException {
		long position = file.getFilePointer();
		
		file.writeLong(prev);
		file.writeShort((short) item.getKey().length);
		file.write(item.getKey());
		file.writeLong(offset);
		
		return position;
	}
	
	private void init() throws IOException {
		writeHeader();
		createBuckets();
	}
	
	private void writeHeader() throws IOException {
		file.seek(0);
		file.writeByte(DIRTY);
		file.writeFloat(totalItems);
		file.getFD().sync();
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("usage: TMuralIndexBuilder <mural filename>");
			System.exit(0);
		}
		
		TMuralIndexBuilder builder = new TMuralIndexBuilder(args[0]);
		builder.call();
	}
}
