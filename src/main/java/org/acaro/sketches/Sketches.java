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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.acaro.sketches.io.OperationReader;
import org.acaro.sketches.io.OperationWriter;
import org.acaro.sketches.memstore.Memstore;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.Update;
import org.acaro.sketches.sfile.RAMSFile;
import org.acaro.sketches.sfile.SFile;
import org.acaro.sketches.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 
 * The class is the entry point to the KV store.
 * 
 * Important: Both the key and the value should be immutable. 
 * We are not going to change them, we expect you to do the same.

 * @author Claudio Martella
 *
 */

public class Sketches {
	private static final Logger logger = LoggerFactory.getLogger(OldSketches.class);

	private OperationWriter writer;
	private final List<OperationReader> readers = new ArrayList<OperationReader>();
	/* 
	 * This lock protects modification of readers and writer, not the modification of a contained Sketch.
	 * - lock for read when you access readers or writer
	 * - lock for write when you modify readers (at bombing & scribing) and writer reference 
	 */
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock readLock  = lock.readLock();
	private final Lock writeLock = lock.writeLock();

	private final Configuration conf = Configuration.getConf();

	// this will flush the log every 10 seconds by default. It stops at first Exception
	private final ScheduledExecutorService flushScheduler = Executors.newScheduledThreadPool(1);
	private final ScheduledFuture<?> flusherHandle = flushScheduler.scheduleAtFixedRate(
																	new Flusher(), 
																	conf.getInt("sketches.flusherdelay", 10000), 
																	conf.getInt("sketches.flusherdelay", 10000), 
																	TimeUnit.MILLISECONDS);

	// this will try to scribe the Memestore if it's bigger than default 64MB. It stops at first Exception
	private final ScheduledExecutorService scriberScheduler = Executors.newScheduledThreadPool(1);
	private final ScheduledFuture<?> scriberHandle = scriberScheduler.scheduleAtFixedRate(
																	  new Scriber(conf.getInt("sketches.memstore.maxsize", 64)), 
																	  conf.getInt("sketches.scriberdelay", 30), 
																	  conf.getInt("sketches.scriberdelay", 30), 
																	  TimeUnit.SECONDS);

	private final ScheduledExecutorService compactorScheduler = Executors.newScheduledThreadPool(1);
	private ScheduledFuture<?> compactorHandle;
	
	private String path;
	private String name;

	public Sketches(String path, String name) throws IOException {
		Preconditions.checkNotNull(path);
		Preconditions.checkNotNull(name);
		this.path = path;
		this.name = name;
		init(path, name);
	}

	public void put(byte[] key, byte[] value) throws IOException {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(key.length <= Short.MAX_VALUE, "key length can not be bigger than %s", Short.MAX_VALUE);

		doPut(key, new Update(key, value));
	}

	public byte[] get(byte[] key) throws IOException {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.length <= Short.MAX_VALUE, "key length can not be bigger than %s", Short.MAX_VALUE);

		Operation o;
		byte[] value = null;

		if ((o = doGet(key)) != null) // it was here... 
			value = o.getValue();
		
		return value;
	}

	public void delete(byte[] key) throws IOException {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.length <= Short.MAX_VALUE, "key length can not be bigger than %s", Short.MAX_VALUE);

		doDelete(key);
	}

	public void shutdown() throws IOException {
		flusherHandle.cancel(false); // will these block or fail?
		scriberHandle.cancel(false); 
		
		// stop everthing and close things
	}

	private Operation doGet(byte[] key) throws IOException {
		Operation o = null;

		readLock.lock();

		try {

			for (OperationReader reader: readers)
				if ((o = reader.get(key)) != null)
					break;

		} finally {
			readLock.unlock();
		}

		return o;
	}

	private void doPut(byte[] key, Operation o) throws IOException {

		readLock.lock();

		try {

			writer.put(key, o);

		} finally {
			readLock.unlock();
		}
	}
	
	private void doDelete(byte[] key) throws IOException {

		readLock.lock();

		try {

			writer.delete(key);

		} finally {
			readLock.unlock();
		}
	}
	
	private void init(String path, String name) {
		// initialize Memstore, possibly from a Logfile
		// load possible SFiles
	}

	/*
	 * ScheduledTask Robots start here.
	 */
	private class Flusher implements Runnable {

		public void run() {

			readLock.lock();

			try {
				
				writer.flush();

			} catch (IOException e) {
				logger.error("Flusher: couldn't flush!", e);
			} finally {
				readLock.unlock();
			}
		}
	}
	
	private class Scriber implements Runnable {
		private final int threshold; // maxSize of Memstore in bytes
		
		public Scriber(int threshold) {
			this.threshold = threshold * 1024 * 1024; 
		}

		public void run() {

			readLock.lock();
			
			if (writer.getSize() > threshold) {
				readLock.unlock();
				writeLock.lock();
			
				// as we are the only one modifying this datastructure and
				// we have just one bomber each time, we don't really need
				// to check again for the condition.
				assert writer.getSize() > threshold: "Scriber: memory has shrunk between two checks!";
				assert readers.get(0) instanceof Memstore: "head of readers is not a Memstore";
				
				Memstore oldStore = (Memstore) readers.get(0);
				RAMSFile ramSFile = new RAMSFile(oldStore);
				Memstore newStore = new Memstore();

				readers.set(0, newStore); // substitute old with new clean store
				readers.add(1, ramSFile); // insert temporary SFile
				writer = newStore;
				
				writeLock.unlock();
				
				// convert to SFile
				SFile newSFile = null;
				try {
					newSFile = SketchesHelper.scribe(oldStore);
				} catch (IOException e) {
					logger.error("Scriber: failed scribing " + oldStore, e);
				}
				
				writeLock.lock();
				
				// switch temporary RAMSFile with fresh new SFile
				readers.set(readers.indexOf(ramSFile), newSFile);
				
				writeLock.unlock();
				
				scheduleCompaction();
			}
		}

		private void scheduleCompaction() {
			// run/schedule only if not already running
			if (compactorHandle != null && compactorHandle.isDone())
				compactorHandle = compactorScheduler.schedule(new Compactor(), 0, TimeUnit.SECONDS);
		}
	}
	
	private class Compactor implements Runnable {
		
		public void run() {
			
		}
	}
}
