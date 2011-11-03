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

package org.acaro.sketches.utils;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.acaro.sketches.io.MappedSmartReader;
import org.acaro.sketches.memstore.Memstore;
import org.acaro.sketches.operation.Delete;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.OperationComparator;
import org.acaro.sketches.operation.OperationHelper;
import org.acaro.sketches.sfile.FSSFile;
import org.acaro.sketches.sfile.FSSFileIndexer;
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

	public static Memstore loadLogfile(String file) 
	throws IOException {
	
		Memstore memory = new Memstore();
		FileChannel ch  = null;

		ch = new RandomAccessFile(file, "rw").getChannel();
		MappedByteBuffer buffer = ch.map(MapMode.READ_WRITE, 0, ch.size());
		buffer.load();
		DataInput in = new MappedSmartReader(buffer);

		int loaded = 0;
		while (true) {
			
			try {
				Operation o = OperationHelper.readOperation(in);
				memory.put(o.getKey(), o);
				loaded++;
			} catch (EOFException e) {
				break;
			} finally {
				ch.close();
			}
		}

		logger.debug(loaded + " loaded: " + memory.getSize());

		return memory;
	}
	
	public static void scribe(Memstore memory, String filename) 
	throws IOException {
		
		long start = System.currentTimeMillis();
		logger.info("burning started: " + start);

		Map<byte[], Operation> map  = memory.getMap();
		ArrayList<Operation> sorted = new ArrayList<Operation>(map.size());
		sorted.addAll(map.values());

		Collections.sort(sorted, new OperationComparator());
		logger.debug("Memstore is sorted: "+ (System.currentTimeMillis()-start));

		FSSFileWriter writer = new FSSFileWriter(filename);
		
		start = System.currentTimeMillis();
		for (Operation sketch: sorted)
			writer.write(sketch);
		
		writer.close();
		logger.info("burning finished: " + (System.currentTimeMillis()-start));
		
		FSSFileIndexer indexer = new FSSFileIndexer(filename);
		indexer.index();
	}

	public static void compact(String younger, String older, String filename, boolean major) 
	throws IOException {

		FSSFileIterator f1 = new FSSFileIterator(younger);
		FSSFileIterator f2 = new FSSFileIterator(older);
		List<FSSFileIterator> iterators = new ArrayList<FSSFileIterator>();
		iterators.add(f1);
		iterators.add(f2);
		
		FSSFileCursor cursor = new FSSFileCursor(iterators);
		FSSFileWriter writer = new FSSFileWriter(filename);
		
		while (cursor.hasNext()) {
			Operation o = cursor.next();
			if (major && o instanceof Delete)
				continue;

			writer.write(o);
		}
			
		cursor.close();
		writer.close();
		
		FSSFileIndexer indexer = new FSSFileIndexer(filename);
		indexer.index();
	}
}
