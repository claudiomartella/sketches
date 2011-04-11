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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;

public class TMuralIndexReader implements Closeable {
	private RandomAccessFile index;
	private MappedByteBuffer buckets;
	private int numberOfBuckets;
	
	public TMuralIndexReader(String indexFilename) throws IOException {
		this.index = new RandomAccessFile(indexFilename, "r");
		init();
	}

	public int getNumberOfBuckets() {
		return numberOfBuckets;
	}
	
	public BucketIterator getBucket(int bucket) {
		if (bucket < 0 || bucket >= numberOfBuckets) 
			throw new IllegalArgumentException(
					"illegal bucket index "+ bucket + "we have "+ numberOfBuckets + " buckets");
		
		buckets.position(MuralIndex.BUCKET_ITEM_SIZE*bucket);
		long item = buckets.getLong();
		
		return new BucketIterator(item);
	}

	public void close() throws IOException {
		this.index.close();
	}
	
	private void readHeader() throws IOException {
		// TODO: check wether the index is CLEAN and report back
		byte dirty = index.readByte();
		numberOfBuckets = index.readInt();
	}
	
	private void readBuckets() throws IOException {
		buckets = index.getChannel().map(
				MapMode.READ_ONLY, TMuralIndexBuilder.HEADER_SIZE, MuralIndex.BUCKET_ITEM_SIZE*numberOfBuckets);
	}
	
	private void init() throws IOException {
		readHeader();
		readBuckets();
	}
	
	class BucketIterator {
		private long nextItem;
		
		public BucketIterator(long item) {
			this.nextItem = item;
		}

		public boolean hasNext() {
			return nextItem != 0;
		}
		
		public BucketItem next() throws IOException {
			if (!hasNext()) throw new NoSuchElementException();
			
			return readItem();
		}
		
		private BucketItem readItem() throws IOException {
			index.seek(nextItem);
			nextItem = index.readLong();
			short kl = index.readShort();
			byte[] k = new byte[kl];
			index.readFully(k);
			long off = index.readLong();

			return new BucketItem(k, off);
		}
		
		class BucketItem {
			final byte[] key;
			final long offset;
			
			public BucketItem(final byte[] key, final long offset) {
				this.key = key;
				this.offset = offset;
			}

			public long getOffset() {
				return this.offset;
			}

			public byte[] getKey() {
				return this.key;
			}
		}
	}
}
