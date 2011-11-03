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
import org.acaro.sketches.io.OperationMutator;
import org.acaro.sketches.logfiles.BufferedLogfile;
import org.acaro.sketches.operation.Delete;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.sfile.SFile;
import org.acaro.sketches.utils.Configuration;
import org.acaro.sketches.utils.FilenamesFactory;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * The Memstore is where the written data is kept before it is flushed to disk. 
 * It builds around a NonBlockingHashMap for the data, an AtomicLong that 
 * counts the total amout of data passed through the store and an AtomicLong for
 * the timestamp of oldest entry.
 * 
 * @author Claudio Martella
 *
 */
public class Memstore 
implements OperationReader, OperationMutator {

	private final int initialCapacity = Configuration.getConf().getInt("sketches.memstore.initialcapacity", 100000);
	private final NonBlockingHashMap<byte[], Operation> map = new NonBlockingHashMap<byte[], Operation>(initialCapacity);
	private final AtomicLong size      = new AtomicLong(0);
	private final AtomicLong timestamp = new AtomicLong(0);
	private BufferedLogfile log;
	
	public Memstore() 
	throws IOException { 
		
		this.log = new BufferedLogfile(FilenamesFactory.getLogFilename());
	}
	
	public Memstore(String logFilename) 
	throws IOException { 
	
		this.log = new BufferedLogfile(logFilename, false, true);
	}
	
	public Operation get(byte[] key) {
		return map.get(key);
	}

	public void put(byte[] key, Operation o) 
	throws IOException {
	
		updateSize(o.getSize());
		updateTimestamp(o.getTimestamp());

		log.write(o);
		map.put(key, o);
	}
	
	public void delete(byte[] key) 
	throws IOException {
	
		put(key, new Delete(key));
	}
	
	public void flush() 
	throws IOException {
	
		log.flush();
	}

	public long getSize() {
		return this.size.get();
	}
	
	public String getName() {
		return log.getName();
	}

	public long getTimestamp() {
		return this.timestamp.get();
	}
	
	public boolean isCompactable() {
		return false;
	}
	
	public void close() 
	throws IOException {

		// log should flush & sync
		log.close();
	}

	public int compareTo(OperationReader other) {
		long otherTS = other.getTimestamp();
		
		if (this.getTimestamp() < otherTS)
			return -1;
		else if (this.getTimestamp() > otherTS)
			return 1;
		else
			return 0;
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

			if (ts <= oldTs || timestamp.compareAndSet(oldTs, ts))
				break;
		}
	}
}
