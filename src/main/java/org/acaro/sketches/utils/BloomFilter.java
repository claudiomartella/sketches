/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.acaro.sketches.utils;

import java.io.IOException;

import org.acaro.sketches.io.SmartReader;
import org.acaro.sketches.io.SmartWriter;
import org.acaro.sketches.util.obs.OpenBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 
 *	Originally from org.apache.cassandra.util.BloomFilter
 */
public class BloomFilter {
	private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);
	private static final int EXCESS = 20;
	public OpenBitSet bitset;
	private int hashCount;

	BloomFilter(int hashes, OpenBitSet bs) {
		this.hashCount = hashes;
		this.bitset    = bs;
	}

	/**
	 * @return A BloomFilter with the lowest practical false positive probability
	 * for the given number of elements.
	 */
	public static BloomFilter getFilter(long numElements, int targetBucketsPerElem) {
		int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
		int bucketsPerElement    = Math.min(targetBucketsPerElem, maxBucketsPerElement);
		
		if (bucketsPerElement < targetBucketsPerElem)
			logger.warn(String.format("Cannot provide an optimal BloomFilter for %d elements (%d/%d buckets per element).",
					numElements, bucketsPerElement, targetBucketsPerElem));

		BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);

		return new BloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
	}

	/**
	 * @return The smallest BloomFilter that can provide the given false positive
	 * probability rate for the given number of elements.
	 *
	 * Asserts that the given probability can be satisfied using this filter.
	 */
	public static BloomFilter getFilter(long numElements, double maxFalsePosProbability) {
		Preconditions.checkArgument(maxFalsePosProbability < 1.0 &&
									maxFalsePosProbability > 0 , "Invalid probability");
		
		int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
		BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);

		return new BloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
	}

	public static void serialize(BloomFilter bf, SmartWriter writer) throws IOException {
		long[] bits   = bf.bitset.getBits();
		int bitLength = bits.length;

		writer.writeInt(bf.getHashCount());
		writer.writeInt(bitLength);

		for (int i = 0; i < bitLength; i++)
			writer.writeLong(bits[i]);
	}

	public static BloomFilter deserialize(SmartReader reader) throws IOException {
		int hashes    = reader.readInt();
		int bitLength = reader.readInt();
		long[] bits   = new long[bitLength];

		for (int i = 0; i < bitLength; i++)
			bits[i] = reader.readLong();
		
		OpenBitSet bs = new OpenBitSet(bits, bitLength);

		return new BloomFilter(hashes, bs);
	}

	public void add(byte[] key) {
		for (long bucketIndex : getHashBuckets(key))
			bitset.set(bucketIndex);
	}

	public boolean isPresent(byte[] key) {
		for (long bucketIndex : getHashBuckets(key))
			if (!bitset.get(bucketIndex))
				return false;

		return true;
	}

	public void clear() {
		bitset.clear(0, bitset.size());
	}

	private static OpenBitSet bucketsFor(long numElements, int bucketsPer) {
		return new OpenBitSet(numElements * bucketsPer + EXCESS);
	}

	private long buckets() {
		return bitset.size();
	}

	private long[] getHashBuckets(byte[] key) {
		return BloomFilter.getHashBuckets(key, hashCount, buckets());
	}

	private int getHashCount() {
		return hashCount;
	}

	// Murmur is faster than an SHA-based approach and provides as-good collision
	// resistance.  The combinatorial generation approach described in
	// http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
	// does prove to work in actual tests, and is obviously faster
	// than performing further iterations of murmur.
	private static long[] getHashBuckets(byte[] b, int hashCount, long max) {
		long[] result = new long[hashCount];
		long hash1    = MurmurHash3.MurmurHash3_x64_64(b, 0L);
		long hash2    = MurmurHash3.MurmurHash3_x64_64(b, hash1);
		
		for (int i = 0; i < hashCount; ++i)
			result[i] = Math.abs((hash1 + (long)i * hash2) % max);

		return result;	
	}
}
