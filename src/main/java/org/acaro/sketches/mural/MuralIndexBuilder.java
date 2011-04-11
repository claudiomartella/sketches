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
import java.io.SyncFailedException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Callable;

import org.acaro.sketches.mural.TMuralIndexReader.BucketIterator;
import org.acaro.sketches.mural.TMuralIndexReader.BucketIterator.BucketItem;
import org.acaro.sketches.util.Util;

public class MuralIndexBuilder implements Callable<String> {
	private RandomAccessFile mural;
	private TMuralIndexReader index;
	private MappedByteBuffer buckets;
	private String muralFilename;
	private int numberOfBuckets;
	
	public MuralIndexBuilder(String muralFilename, String tIndexFilename) throws IOException {
		this.muralFilename = muralFilename;
		this.mural = new RandomAccessFile(muralFilename, "rw");
		this.index = new TMuralIndexReader(tIndexFilename);
		this.numberOfBuckets = index.getNumberOfBuckets();
		init();
	}

	public String call() throws IOException {

		for (int i = 0; i < numberOfBuckets; i++) {
			writeBucket(index.getBucket(i), i);
		}
		close();
		
		return muralFilename;
	}
	
	private void close() throws IOException {
		buckets.force();
		mural.seek(0);
		mural.writeByte(Mural.CLEAN);
		mural.getFD().sync();
		mural.close();
		index.close();
	}
	
	private void writeBucket(BucketIterator bucket, int bucketNo) throws IOException {
		int length = 0;
		long start = mural.getFilePointer();
		buckets.position(MuralIndex.BUCKET_ITEM_SIZE*bucketNo);
		buckets.putLong(start);
		
		while (bucket.hasNext()) {
			length += writeItem(bucket.next());
		}
		
		buckets.putInt(length);
	}
	
	private int writeItem(BucketItem item) throws IOException {
		mural.writeLong(item.getOffset());
		mural.writeShort((short) item.getKey().length);
		mural.write(item.getKey());
		
		return Util.SIZEOF_LONG+Util.SIZEOF_SHORT+item.getKey().length;
	}
	
	private void updateMuralHeader() throws IOException {
		mural.writeByte(Mural.DIRTY);
		mural.seek(Util.SIZEOF_BYTE+Util.SIZEOF_LONG+Util.SIZEOF_INT);
		mural.writeLong(mural.length());
		mural.getFD().sync();
	}
	
	private void createBuckets() throws IOException {
		byte[] b = new byte[MuralIndex.BUCKET_ITEM_SIZE*numberOfBuckets];
		long position = mural.length();
		
		mural.seek(position);
		mural.write(b);
		buckets = mural.getChannel().map(MapMode.READ_WRITE, position, MuralIndex.BUCKET_ITEM_SIZE*numberOfBuckets);
	}
	
	private void init() throws IOException {
		updateMuralHeader();
		createBuckets();
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: MuralIndexBuilder <mural filename> <temporary index filename>");
			System.exit(0);
		}
		
		System.out.println("Inserting index in Mural");
		MuralIndexBuilder builder = new MuralIndexBuilder(args[0], args[1]);
		builder.call();
		System.out.println("Done!");
	}
}
