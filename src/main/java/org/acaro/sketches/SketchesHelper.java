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
import java.util.List;
import java.util.Map;

import org.acaro.sketches.memstore.Memstore;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.OperationComparator;
import org.acaro.sketches.operation.OperationHelper;
import org.acaro.sketches.sfile.FSSFile;
import org.acaro.sketches.sfile.FSSFileIterator;
import org.acaro.sketches.sfile.FSSFileWriter;
import org.acaro.sketches.sfile.FSSFileCursor;
import org.acaro.sketches.sfile.SFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public static Memstore loadSketchBook(String file) throws IOException {
		Memstore memory = new Memstore();
		FileChannel ch  = null;
		int loaded = 0;
		
		try {
			ch = new RandomAccessFile(file, "rw").getChannel();
			MappedByteBuffer buffer = ch.map(MapMode.READ_WRITE, 0, ch.size());
			buffer.load();
			
			while (buffer.hasRemaining()) {
				Operation s = OperationHelper.readItem(buffer);				
				memory.put(s.getKey(), s);
				loaded++;
			}
			
			ch.close();
		} catch (BufferUnderflowException e) {
			logger.info("Truncated file, we probably died without synching correctly");
			ch.truncate(ch.position());
			ch.force(true);
			ch.close();
		}

		logger.debug(loaded + " loaded: " + memory.getSize());
		
		return memory;
	}
	
	public static String getFilename(String path, String name) {
		return path + "/" + name + EXTENSION;
	}
	
	
	public static SFile scribe(Memstore memory) throws IOException {
		long start = System.currentTimeMillis();
		logger.info("burning started: " + start);

		Map<byte[], Operation> map = memory.getMap();
		ArrayList<Operation> sorted = new ArrayList<Operation>(map.size());
		sorted.addAll(map.values());

		Collections.sort(sorted, new OperationComparator());
		logger.debug("MindSketches is sorted: "+ (System.currentTimeMillis()-start));

		FSSFileWriter writer = new FSSFileWriter(memory.getFilename());
		
		start = System.currentTimeMillis();
		for (Operation sketch: sorted)
			writer.write(sketch);
		
		writer.close();
		logger.info("burning finished: " + (System.currentTimeMillis()-start));
		
		return new FSSFile("sticazzi");
	}
	
	public static void compact(List<FSSFileIterator> muralIterators, String filename) throws IOException {
		if (muralIterators.size() < 2) throw new IllegalArgumentException("Can't merge less than 2 Murals");
		
		FSSFileCursor cursor = new FSSFileCursor(muralIterators);
		FSSFileWriter writer = new FSSFileWriter(filename);
		
		while (cursor.hasNext()) {
			writer.write(cursor.next());
		}
		
		cursor.close();
		writer.close();
	}
}
