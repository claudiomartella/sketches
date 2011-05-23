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

package org.acaro.sketches.memstore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.acaro.sketches.io.OperationReader;
import org.acaro.sketches.io.OperationWriter;
import org.acaro.sketches.logfile.BufferedLogfile;
import org.acaro.sketches.operation.Delete;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.util.Configuration;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This class represents the Memstore, where the written data is kept before
 * being flushed to disk. It builds around a NonBlockingHashMap for the data and
 * an AtomicLong that counts the total amout of data passed through the store.
 * 
 * @author Claudio Martella
 *
 */
public class Memstore implements OperationReader, OperationWriter {
	private final int initialCapacity = Configuration.getConf().getInt("sketches.mindsketches.initialcapacity", 100000);
	private final NonBlockingHashMap<byte[],Operation> map = new NonBlockingHashMap<byte[], Operation>(initialCapacity);
	private final AtomicLong size      = new AtomicLong(0);
	private final AtomicLong timestamp = new AtomicLong(0);
	private BufferedLogfile log;
	
	public Memstore() { }
	
	public Memstore(String logFilename) {
		
	}
	
	public Operation get(byte[] key) {
		return map.get(key);
	}

	public void put(byte[] key, Operation o) throws IOException {
		updateSize(o.getSize());
		updateTimestamp(o.getTimestamp());

		map.put(key, o);
		log.write(o);
	}
	
	public void delete(byte[] key) throws IOException {
		put(key, new Delete(key));
	}
	
	public void flush() throws IOException {
		log.flush();
	}

	public long getSize() {
		return this.size.get();
	}

	public long getTimestamp() {
		return this.timestamp.get();
	}

	public Map<byte[], Operation> getMap() {
		return this.map;
	}
	
	private void updateSize(int valueSize) {
		size.addAndGet(valueSize);
	}
	
	private void updateTimestamp(long ts) {
		
		while (true) {
			long oldTs = timestamp.get();

			if (ts < oldTs || timestamp.compareAndSet(oldTs, ts))
				break;
		}
	}
}
