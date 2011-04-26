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
import java.util.List;
import java.util.TreeMap;

import org.acaro.sketches.mindsketches.MindSketches;
import org.acaro.sketches.mural.MuralIterator;
import org.acaro.sketches.mural.MuralWriter;
import org.acaro.sketches.mural.MuralsCursor;
import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.sketch.SketchHelper;
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
		
		TreeMap<byte[], Sketch> sortedMap = new TreeMap<byte[], Sketch>(UnsignedBytes.lexicographicalComparator());
		sortedMap.putAll(memory.getMap());
		logger.debug("MindSketches is sorted");

		MuralWriter writer = new MuralWriter(filename);
		
		for (Sketch sketch: sortedMap.values())
			writer.write(sketch);
		
		writer.close();
		logger.info("burning finished: " + (System.currentTimeMillis()-start));
	}
	
	public static void scribeMurals(List<MuralIterator> muralIterators, String filename) throws IOException {
		if (muralIterators.size() < 2) throw new IllegalArgumentException("Can't merge less than 2 Murals");
		
		MuralsCursor cursor = new MuralsCursor(muralIterators);
		MuralWriter writer = new MuralWriter(filename);
		
		while (cursor.hasNext()) {
			writer.write(cursor.next());
		}
		cursor.close();
		writer.close();
	}
}
