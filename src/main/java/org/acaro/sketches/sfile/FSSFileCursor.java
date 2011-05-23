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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.acaro.sketches.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.UnsignedBytes;

/**
 * Iterable set of MuralIterator. Returns element with smallest key.
 * It expects the iterators List passed to the constructor to be sorted by time.
 * iterators[0].getTimestamp() > iterators[1].getTimestamp() > ... > iterators[n].getTimestamp()
 * When two elements with the same key are found, the youngest is returned.
 * The result is a live "merging" of the Murals. Used to implement Compaction.
 * 
 * @author Claudio Martella
 *
 */

public class FSSFileCursor implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(FSSFileCursor.class);
	private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
	private final List<FSSFileIterator> iterators;
	private final List<SFileCursor> cursors;

	public FSSFileCursor(List<FSSFileIterator> iterators) throws IOException {
		this.iterators = iterators;
		this.cursors   = new LinkedList<SFileCursor>();
		for (FSSFileIterator iterator: iterators)
			this.cursors.add(new SFileCursor(iterator));
	}
	
	public boolean hasNext() {
		return cursors.size() > 0;
	}

	/**
	 * @return the next youngest element with the smallest key among the Murals
	 */
	public Operation next() throws IOException {
		if (!hasNext()) throw new NoSuchElementException();
		
		return getMinimum(); 
	}

	public void close() throws IOException {
		for (FSSFileIterator iterator: iterators)
			iterator.close();
	}
	
	// XXX: for performance this chould be an ArrayList
	private LinkedList<SFileCursor> minima = new LinkedList<SFileCursor>();
	
	private Operation getMinimum() throws IOException {
		Iterator<SFileCursor> iter = cursors.iterator();
		SFileCursor cursor = iter.next();
		Operation minimum  = cursor.getValue();
		minima.add(cursor);
		
		while (iter.hasNext()) {
			cursor = iter.next();
			Operation value = cursor.getValue();
			int comparison = comparator.compare(minimum.getKey(), value.getKey()); 
			if (comparison == 0) { 
				// next one would be a bug: iterators/cursors are sorted by time (youngest first), this shouldn't happen
				assert value.getTimestamp() > minimum.getTimestamp() : "Older FSSFileIterator with younger data!";
				minima.add(cursor);
			} else if (comparison > 0) {
				minimum = value;
				minima.clear();
				minima.add(cursor);
			}
		}
		advance(minima);
		minima.clear();
		
		return minimum;
	}
	
	private void advance(List<SFileCursor> minima) throws IOException {
		
		for (SFileCursor cursor: minima) {
			if (cursor.hasNext()) {
				cursor.advance();
			} else {
				cursors.remove(cursor);
			}
		}
	}
	
	/*
	 * XXX: this probably skips the last element 
	 */
	private class SFileCursor {
		private final FSSFileIterator iterator;
		private Operation value;
		
		public SFileCursor(FSSFileIterator iterator) throws IOException {
			this.iterator = iterator;
			if (hasNext())
				advance();
			else
				throw new IOException("Empty SFile");
		}
		
		public Operation getValue() {
			return value;
		}
		
		public void advance() throws IOException {
			if (!hasNext()) throw new NoSuchElementException();
			
			value = iterator.next();
		}
		
		public boolean hasNext() {
			return iterator.hasNext();
		}
	}
}
