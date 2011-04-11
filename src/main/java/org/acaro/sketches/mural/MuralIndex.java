package org.acaro.sketches.mural;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.acaro.sketches.util.MurmurHash3;
import org.acaro.sketches.util.Util;

/**
 * 
 * @author Claudio Martella
 *
 *
 * XXX: the chunk is ordered, we could take advante of it and stop at a certain point
 */

public class MuralIndex {
	public static final int BUCKET_ITEM_SIZE = Util.SIZEOF_LONG + Util.SIZEOF_INT;
	private MappedByteBuffer buckets;
	private RandomAccessFile mural;
	private int numberOfBuckets;
	private long indexStart;

	public MuralIndex(RandomAccessFile file, int numberOfBuckets, long indexStart) throws IOException {
		this.mural = file;
		this.numberOfBuckets = numberOfBuckets;
		this.indexStart = indexStart;
		readBuckets();
	}
	
	public MuralIndex(RandomAccessFile file) throws IOException {
		this.mural = file;
		readHeader();
		readBuckets();
	}
	
	public MuralIndex(String muralFilename) throws IOException {
		this(new RandomAccessFile(muralFilename, "r"));
	}

	public long get(byte[] key) throws IOException {
		buckets.position(BUCKET_ITEM_SIZE*calculateBucket(key));
		long position = buckets.getLong();
		int size = buckets.getInt();
		
		return readOffset(position, size, key);
	}

	private long readOffset(long position, int size, byte[] key) throws IOException {
		mural.seek(position);
		byte[] chunk = new byte[size];
		mural.readFully(chunk);
		
		return searchOffset(chunk, key);
	}
	
	private long searchOffset(byte[] chunk, byte[] key) throws IOException {
		ByteBuffer k = ByteBuffer.wrap(key);
		ByteBuffer b = ByteBuffer.wrap(chunk);
		
		short keylength = (short) key.length;
		long offset = -1;
		while (b.hasRemaining()) {
			long off = b.getLong();
			short kl = b.getShort();
			b.limit(b.position()+kl);
			if (kl == keylength && b.equals(k)) { // found!
				offset = off;
				break;
			} else {
				b.position(b.position()+kl);
				b.limit(b.capacity());
			}
		}
		
		return offset;
	}
	
	private int calculateBucket(byte[] key) {
		return MurmurHash3.hash(key) % numberOfBuckets;
	}
	
	private void readBuckets() throws IOException {
		buckets = mural.getChannel().map(MapMode.READ_ONLY, indexStart, BUCKET_ITEM_SIZE*numberOfBuckets);
	}

	private void readHeader() throws IOException {
		mural.skipBytes(Util.SIZEOF_BYTE+Util.SIZEOF_LONG); // skip dirty byte and ts
		numberOfBuckets = mural.readInt();
		indexStart = mural.readLong();		
	}
}
