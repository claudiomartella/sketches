package org.acaro.sketches.mural;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import org.acaro.sketches.playground.T5MInserter;
import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.io.BufferedRandomAccessFile;
import org.acaro.sketches.io.SmartReader;
import org.acaro.sketches.io.SmartWriter;
import org.acaro.sketches.util.FSUtils;
import org.acaro.sketches.util.MurmurHash3;
import org.acaro.sketches.util.MyBufferedRandomAccessFile;
import org.acaro.sketches.util.NewBufferedRandomAccessFile;
import org.acaro.sketches.util.Util;

public class MuralIndexer implements Callable<String> {
	private MuralIterator muralIterator;
	private BufferedRandomAccessFile file;
	private BufferedRandomAccessFile tfile;
	private SmartWriter writer;
	private Index buckets;
	private int numberOfItems;
	private long indexOffset;
	private SmartWriter twriter;
	private SmartReader treader;

	public MuralIndexer(String muralFilename) throws IOException {
		this.muralIterator = new MuralIterator(muralFilename);
		this.numberOfItems = muralIterator.getNumberOfItems();
		this.file    = new BufferedRandomAccessFile(muralFilename, "rw");
		this.tfile   = new BufferedRandomAccessFile(muralFilename + ".tmp", "rw");
		this.writer  = new SmartWriter(this.file.getChannel());
		this.twriter = new SmartWriter(this.tfile.getChannel());
		this.indexOffset = file.length(); 
		init();
	}

	public String call() throws IOException {
		fillBuckets();
		compactBuckets();

		close();
		
		return null;
	}
	
	public void close() throws IOException {
		buckets.force();
		muralIterator.close();
		writer.flush();
		file.seek(0);
		file.writeByte(Mural.CLEAN);
		file.flush();
		file.getFD().sync();
		file.close();
		tfile.close();
		//FSUtils.delete(tfile.getFile());
	}

	private void fillBuckets() throws IOException {
		while (muralIterator.hasNext()) {
			Sketch item = muralIterator.next(); // what is it?
			long offset = muralIterator.getLastOffset(); // where is it?
			writeToTBucket(calculateBucket(item.getKey()), offset);
		}
		twriter.flush();
	}
	
	private void compactBuckets() throws IOException {
		buckets.position(0);
		treader = new SmartReader(this.tfile.getChannel());
		while (buckets.hasRemaining()) {
			long position = buckets.position();
			long offset   = buckets.getOffset();
			if (offset >= indexOffset) { // get it from tempfile
				TBucketIterator tbucket = new TBucketIterator(offset - indexOffset);
				buckets.putOffset(position, writer.getFilePointer());
				while (tbucket.hasNext())
					writer.writeLong(tbucket.next());
				writer.writeLong(0);
			}
		}
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
	 * (c) offset >= indexOffset -> points to the head of the list in temporary index file. In this case, to distinguish from case (b)
	 * 
	 * for case (c) we add indexOffset to the actual offset (as the indices within the temporary index file start from 0).
	 */
	private void writeToTBucket(int bucketNo, long offset) throws IOException {
		//logger.debug("writing to bucket: " + bucketNo);
		long index = calculateBucketIndex(bucketNo);
		long last  = buckets.getOffset(index);
		long position = 0;
		// first insert (a)
		if (last == 0)
			position = offset;
		else {
			// direct link to data, convert to list head (b)
			if (last < indexOffset) 
				last = indexOffset + writeTItem(0, last);
			// insert the new element (c)
			position = indexOffset + writeTItem(last - indexOffset, offset);
		}
		buckets.putOffset(index, position);
	}

	private long writeTItem(long prev, long offset) throws IOException {
		long position = tfile.getFilePointer();
		
		twriter.writeLong(prev);
		twriter.writeLong(offset);
		
		return position;
	}
	
	private void init() throws IOException {
		writeHeader();
		createBuckets();
	}
	
	private void writeHeader() throws IOException {
		tfile.writeByte(Mural.DIRTY);
		tfile.flush();
		tfile.getFD().sync();
		file.seek(0);
		file.writeByte(Mural.DIRTY);
		file.skipBytes(Util.SIZEOF_LONG+Util.SIZEOF_INT);
		file.writeLong(indexOffset);
		file.flush();
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
	
	class TBucketIterator {
		private long nextItem;
		
		public TBucketIterator(long item) {
			this.nextItem = item;
		}

		public boolean hasNext() {
			return nextItem != 0;
		}
		
		public long next() throws IOException {
			if (!hasNext()) throw new NoSuchElementException();
			
			return readItem();
		}
		
		private long readItem() throws IOException {
			treader.seek(nextItem);
			nextItem = treader.readLong();
			long offset = treader.readLong();

			return offset;
		}
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("usage: MuralIndexer <filename>");
			System.exit(-1);
		}
		
		MuralIndexer indexer = new MuralIndexer(args[0]);
		indexer.call();
	}
}
