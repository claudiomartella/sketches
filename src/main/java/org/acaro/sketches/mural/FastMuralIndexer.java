package org.acaro.sketches.mural;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Callable;

import org.acaro.sketches.io.SmartWriter;
import org.acaro.sketches.util.MurmurHash3;
import org.acaro.sketches.util.Util;

public class FastMuralIndexer implements Callable<String> {
	private static final double loadFactor = 2;
	private MuralIterator muralIterator;
	private RandomAccessFile file;
	private SmartWriter writer;
	private Index buckets;
	private int numberOfItems;
	private long indexOffset;

	public FastMuralIndexer(String muralFilename) throws IOException {
		this.muralIterator = new MuralIterator(muralFilename);
		this.numberOfItems = (int) Math.floor(loadFactor * muralIterator.getNumberOfItems());
		this.file   = new RandomAccessFile(muralFilename, "rw");
		this.writer = new SmartWriter(file.getChannel());
		this.indexOffset = file.length(); 
		init();
	}

	public String call() throws IOException {
		while (muralIterator.hasNext())
			writeToBucket(muralIterator.next().getKey(), 
					      muralIterator.getLastOffset());		

		close();
		
		return null;
	}
	
	public void close() throws IOException {
		buckets.force();
		muralIterator.close();
		writer.flush();
		file.seek(0);
		file.writeByte(Mural.CLEAN);
		file.getFD().sync();
		file.close();
	}

	private int calculateBucket(byte[] key) {
		return Math.abs(MurmurHash3.hash(key)) % numberOfItems;
	}
	
	private long calculateBucketIndex(int bucketNo) {
		return Mural.BUCKET_ITEM_SIZE * bucketNo;
	}
	
	/*
	 * As we want to distinguish whether a bucket is not in use, points directly to a Sketch
	 * or points to the head of the list in the temporary index file, we use this convention:
	 * (a) offset = 0  -> not used
	 * (b) offset < indexOffset -> points directly to Sketch
	 * (c) offset >= indexOffset -> points to the head of the list 
	 */
	private void writeToBucket(byte[] key, long offset) throws IOException {
		long index = calculateBucketIndex(calculateBucket(key));
		long last  = buckets.getOffset(index);
		long position = 0;

		if (last == 0) // first insert (a)
			position = offset;
		else {
			// direct link to data, convert to list head (b)
			if (last < indexOffset) 
				last = writeItem(0, last);
			// insert the new element (c)
			position = writeItem(last, offset);
		}
		
		buckets.putOffset(index, position);
	}

	private long writeItem(long prev, long offset) throws IOException {
		long position = writer.getFilePointer();
		
		writer.writeLong(prev);
		writer.writeLong(offset);
		
		return position;
	}
	
	private void init() throws IOException {
		writeHeader();
		createBuckets();
	}
	
	private void writeHeader() throws IOException {
		file.seek(0);
		file.writeByte(Mural.DIRTY);
		file.skipBytes(Util.SIZEOF_LONG+Util.SIZEOF_INT);
		file.writeLong(indexOffset);
		file.getFD().sync();
	}
	
	private void createBuckets() throws IOException {
		file.seek(indexOffset);
		createIndex(Mural.BUCKET_ITEM_SIZE * numberOfItems);
		buckets = new SmallIndex(file.getChannel(), MapMode.READ_WRITE, indexOffset, Mural.BUCKET_ITEM_SIZE * numberOfItems);
	}
	
	private void createIndex(long indexSize) throws IOException {
		int CHUNK_SIZE = 65535;
		if (indexSize <= CHUNK_SIZE) {
			byte[] b = new byte[(int) indexSize];
			writer.write(b);
		} else {
			int n = (int) ((indexSize % CHUNK_SIZE == 0) ? indexSize / CHUNK_SIZE : indexSize / CHUNK_SIZE + 1);
			byte[] b = new byte[CHUNK_SIZE];
			
			int l = CHUNK_SIZE;
			for (int i = 0; i < n; i++) {
				if (i == n - 1) { // last chunk
					l = (int) (indexSize - i * CHUNK_SIZE);
					b = new byte[l];
				} 
				
				writer.write(b);
			}
		}
		writer.flush();
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("usage: MuralIndexer <filename>");
			System.exit(-1);
		}
		
		FastMuralIndexer indexer = new FastMuralIndexer(args[0]);
		indexer.call();
	}
}
