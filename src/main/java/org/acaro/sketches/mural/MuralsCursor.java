/*Copyright 2011 Claudio Martella

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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.acaro.sketches.sketch.Sketch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.UnsignedBytes;

/**
 * Iterable set of MuralIterator. Returns element with smallest key.
 * It expects the iterators List passed to the constructor to be sorted by time.
 * iterators[0].getTimestamp() < iterators[1].getTimestamp() < ... < iterators[n].getTimestamp()
 * When two elements with the same key are found, the youngest is returned.
 * The result is a live "merging" of the Murals. Used to implement Compaction.
 * 
 * @author Claudio Martella
 *
 */

public class MuralsCursor implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(MuralsCursor.class);
	private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
	private final List<MuralIterator> iterators;
	private final List<MuralCursor> cursors;

	public MuralsCursor(List<MuralIterator> iterators) throws IOException {
		this.iterators = iterators;
		this.cursors = new LinkedList<MuralCursor>();
		for (MuralIterator iterator: iterators)
			this.cursors.add(new MuralCursor(iterator));
	}
	
	public boolean hasNext() {
		return cursors.size() > 0;
	}

	/**
	 * @return the next youngest element with the smallest key among the Murals
	 */
	public Sketch next() throws IOException {
		if (!hasNext()) throw new NoSuchElementException();
		
		return getMinimum(); 
	}

	public void close() throws IOException {
		for (MuralIterator iterator: iterators)
			iterator.close();
	}

	private Sketch getMinimum() throws IOException {
		List<MuralCursor> minima = new LinkedList<MuralCursor>(); 
		
		Iterator<MuralCursor> iter = cursors.iterator();
		MuralCursor cursor = iter.next();
		Sketch minimum = cursor.getValue();
		minima.add(cursor);
		
		while (iter.hasNext()) {
			cursor = iter.next();
			int comparison = comparator.compare(minimum.getKey(), cursor.getValue().getKey()); 
			if (comparison == 0) { 
				// next one is a bug: iterators/cursors are sorted by time (youngest first), this shouldn't happen
				if (cursor.getValue().getTimestamp() < minimum.getTimestamp())
					throw new IOException("Older MuralIterator with younger data!");
				minima.add(cursor);
			} else if (comparison > 0) {
				minimum = cursor.getValue();
				minima.clear();
				minima.add(cursor);
			}
		}
		advance(minima);
		
		return minimum;
	}
	
	private void advance(List<MuralCursor> minima) throws IOException {
		
		for (MuralCursor cursor: minima) {
			if (cursor.hasNext()) {
				cursor.advance();
			} else {
				cursors.remove(cursor);
			}
		}
	}
		
	private class MuralCursor {
		private final MuralIterator iterator;
		private Sketch value;
		
		public MuralCursor(MuralIterator iterator) throws IOException {
			this.iterator = iterator;
			if (hasNext())
				advance();
			else
				throw new IOException("Empty Mural");
		}
		
		public Sketch getValue() {
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
