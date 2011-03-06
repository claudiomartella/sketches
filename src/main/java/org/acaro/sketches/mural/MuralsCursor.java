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
import java.util.List;
import java.util.NoSuchElementException;

import org.acaro.sketches.sketch.Sketch;

import com.google.common.primitives.UnsignedBytes;

public class MuralsCursor implements Closeable {
	private MuralIterator[] iterators;
	private Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
	private Sketch[] sketches;
	private boolean finished;

	public MuralsCursor(List<MuralIterator> iterators) throws IOException {
		this.iterators = (MuralIterator[]) iterators.toArray();
		sketches = new Sketch[this.iterators.length];
		finished = advance();
	}
	
	public boolean hasNext() {
		return finished;
	}
	
	public Sketch next() throws IOException {
		if (!hasNext()) throw new NoSuchElementException();
		
		Sketch minimum = getMinimum();
		finished = advance();
		
		return minimum;
	}

	public void close() throws IOException {
		for (MuralIterator iterator: iterators)
			iterator.close();
	}
	
	private Sketch getMinimum() {
		Sketch minimum = sketches[0];
		
		for (int i = 1; i < sketches.length; i++)
			if (comparator.compare(minimum.getKey(), sketches[i].getKey()) > 0)
				minimum = sketches[i];
		
		return minimum;
	}
	
	private boolean advance() throws IOException {
		boolean advanced = true;
		int i = 0;
		
		for (MuralIterator mural: iterators) {
			if (mural.hasNext()) {
				sketches[i] = mural.next();
				advanced = false;
			} else { 
				sketches[i] = null;
			}
			i++;
		}
		
		return advanced;
	}
}
