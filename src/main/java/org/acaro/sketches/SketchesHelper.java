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

package org.acaro.sketches;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.acaro.sketches.mindsketches.MindSketches;
import org.acaro.sketches.mural.FSMuralIterator;
import org.acaro.sketches.mural.FSMuralWriter;
import org.acaro.sketches.mural.FSMuralsCursor;
import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.sketch.SketchHelper;
import org.acaro.sketches.util.SketchComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.UnsignedBytes;

/**
 * 
 * @author Claudio Martella
 * 
 * Helper Class
 *
 */

public class SketchesHelper {
	static final Logger logger = LoggerFactory.getLogger(SketchesHelper.class);
	private static final String EXTENSION = ".sb";

	public static MindSketches loadSketchBook(String file) throws IOException {
		MindSketches memory = new MindSketches();
		RandomAccessFile book = null;
		FileChannel ch = null;
		int loaded = 0;
		
		try {
			book = new RandomAccessFile(file, "rw");
			ch = book.getChannel();
			MappedByteBuffer buffer = ch.map(MapMode.READ_WRITE, 0, ch.size());
			buffer.load();
			
			while (buffer.hasRemaining()) {
				Sketch s = SketchHelper.readItem(buffer);				
				memory.put(s.getKey(), s);
				loaded++;
			}
			
			book.close();
		} catch (BufferUnderflowException e) {
			logger.info("Truncated file, we probably died without synching correctly");
			ch.truncate(ch.position());
			ch.force(true);
			book.close();
		}

		logger.debug(loaded + " loaded: " + memory.getSize());
		
		return memory;
	}
	
	public static String getFilename(String path, String name) {
		return path + "/" + name + EXTENSION;
	}
	
	
	public static void bombMindSketches(MindSketches memory, String filename) throws IOException {
		long start = System.currentTimeMillis();
		logger.info("burning started: " + start);

		Map<byte[], Sketch> map = memory.getMap();
		ArrayList<Sketch> sorted = new ArrayList<Sketch>(map.size());
		sorted.addAll(map.values());

		Collections.sort(sorted, new SketchComparator());
		logger.debug("MindSketches is sorted: "+ (System.currentTimeMillis()-start));

		FSMuralWriter writer = new FSMuralWriter(filename);
		
		start = System.currentTimeMillis();
		for (Sketch sketch: sorted)
			writer.write(sketch);
		
		writer.close();
		logger.info("burning finished: " + (System.currentTimeMillis()-start));
	}
	
	public static void scribeMurals(List<FSMuralIterator> muralIterators, String filename) throws IOException {
		if (muralIterators.size() < 2) throw new IllegalArgumentException("Can't merge less than 2 Murals");
		
		FSMuralsCursor cursor = new FSMuralsCursor(muralIterators);
		FSMuralWriter writer = new FSMuralWriter(filename);
		
		while (cursor.hasNext()) {
			writer.write(cursor.next());
		}
		cursor.close();
		writer.close();
	}
}
