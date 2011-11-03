package org.acaro.sketches.sfile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;

import org.acaro.sketches.io.SmartWriter;
import org.acaro.sketches.sfile.index.Index;
import org.acaro.sketches.sfile.index.IndexFactory;
import org.acaro.sketches.utils.BloomFilter;
import org.acaro.sketches.utils.MurmurHash3;
import org.acaro.sketches.utils.Sizes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSSFileIndexer {

	private static final Logger logger = LoggerFactory.getLogger(FSSFileIndexer.class);
	private FSSFileIterator muralIterator;
	private RandomAccessFile file;
	private SmartWriter writer;
	private Index buckets;
	private BloomFilter bloom;
	private long numberOfItems;
	private long indexOffset;
	private long bloomOffset;
	private long directorySize;
	private float loadFactor;

	public FSSFileIndexer(String fssfileFilename) 
	throws IOException {
	
		this.muralIterator = new FSSFileIterator(fssfileFilename);
		this.numberOfItems = muralIterator.getNumberOfItems();
		this.loadFactor    = muralIterator.getLoadFactor();
		this.file          = new RandomAccessFile(fssfileFilename, "rw");
		this.writer        = new SmartWriter(file.getChannel());
		this.bloom         = BloomFilter.getFilter(numberOfItems, 0.01);
		this.indexOffset   = file.length();
		this.directorySize = (long) Math.floor((double) loadFactor * numberOfItems);
		init();
	}

	public void index() 
	throws IOException {
		
		while (muralIterator.hasNext()) {
			byte[] key  = muralIterator.next().getKey();
			long offset = muralIterator.getLastOffset();
			
			writeToBucket(key, offset);
			bloom.add(key);
		}
		
		bloomOffset = writer.getFilePointer();
		BloomFilter.serialize(bloom, writer);
		
		close();
	}
	
	public void close() 
	throws IOException {
	
		buckets.force();
		muralIterator.close();
		writer.flush();
		file.seek(0);
		file.writeByte(FSSFile.CLEAN);
		file.skipBytes(Sizes.SIZEOF_LONG+Sizes.SIZEOF_LONG+Sizes.SIZEOF_FLOAT+Sizes.SIZEOF_LONG);
		file.writeLong(bloomOffset);
		file.getFD().sync();
		file.close();
	}

	private long calculateBucket(byte[] key) {
		return (MurmurHash3.hash(key) & 0x7fffffffffffffffL) % directorySize;	
	}

	private long calculateBucketIndex(long bucketNo) {
		return bucketNo << 3; // bucketNo * SIZEOF_LONG
	}

	/*
	 * As we want to distinguish whether a bucket is not in use, points directly to a Sketch
	 * or points to the head of the list in the temporary index file, we use this convention:
	 * (a) offset = 0  -> not used
	 * (b) offset < indexOffset -> points directly to Sketch
	 * (c) offset >= indexOffset -> points to the head of the list 
	 */
	private void writeToBucket(byte[] key, long offset) 
	throws IOException {
	
		long index = calculateBucketIndex(calculateBucket(key));
		long last  = buckets.getOffset(index);
		long position = 0;

		if (last == 0) // (a)
			position = offset;
		else {
			if (last < indexOffset) // (b) 
				last = writeItem(0, last);
			position = writeItem(last - indexOffset, offset); // (c)
		}

		buckets.putOffset(index, position);
	}

	private long writeItem(long prev, long offset) 
	throws IOException {
	
		long position = writer.getFilePointer();

		writer.writeLong(prev);
		writer.writeLong(offset);

		return position;
	}

	private void init() 
	throws IOException {
	
		writeHeader();
		createBuckets();
	}

	private void writeHeader() 
	throws IOException {
	
		file.seek(0);
		file.writeByte(FSSFile.DIRTY);
		file.skipBytes(Sizes.SIZEOF_LONG+Sizes.SIZEOF_FLOAT+Sizes.SIZEOF_LONG);
		file.writeLong(indexOffset);
		file.getFD().sync();
	}
	
	private void createBuckets() 
	throws IOException {
	
		file.seek(indexOffset);
		createIndex(directorySize << 3);
		buckets = IndexFactory.createIndex(file.getChannel(), MapMode.READ_WRITE, indexOffset, directorySize << 3);
	}

	private void createIndex(long indexSize) 
	throws IOException {
	
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

	public static void main(String[] args) 
	throws IOException {
		if (args.length != 1) {
			System.out.println("usage: MuralIndexer <filename>");
			System.exit(-1);
		}

		FSSFileIndexer indexer = new FSSFileIndexer(args[0]);
		indexer.index();
	}
}
