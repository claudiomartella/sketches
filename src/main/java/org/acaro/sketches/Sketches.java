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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.acaro.sketches.io.OperationMutator;
import org.acaro.sketches.io.OperationReader;
import org.acaro.sketches.io.Writable;
import org.acaro.sketches.logfiles.Logfile;
import org.acaro.sketches.logfiles.state.NewCompactedSFile;
import org.acaro.sketches.logfiles.state.NewLogfile;
import org.acaro.sketches.logfiles.state.NewScribedSFile;
import org.acaro.sketches.logfiles.state.StateLog;
import org.acaro.sketches.logfiles.state.StateLog.StateLogReader;
import org.acaro.sketches.memstore.Memstore;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.Update;
import org.acaro.sketches.sfile.FSSFile;
import org.acaro.sketches.sfile.RAMSFile;
import org.acaro.sketches.sfile.SFile;
import org.acaro.sketches.utils.Configuration;
import org.acaro.sketches.utils.FilenamesFactory;
import org.acaro.sketches.utils.OperationReaders;
import org.acaro.sketches.utils.SketchesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private static final Logger logger = LoggerFactory.getLogger(Sketches.class);
	private final SketchesState state  = new SketchesState();
	private final Configuration conf   = Configuration.getConf();

	private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);
	private final ExecutorService executor = Executors.newCachedThreadPool();
	
	// directory where we store our files.
	private final String path;

	public Sketches(String path) 
	throws IOException {
		
		checkNotNull(path);
		this.path = path;
		init(path);
	}
	
	public void put(byte[] key, byte[] value) 
	throws IOException {

		checkState(!state.isShutdown());
		checkNotNull(key);
		checkNotNull(value);
		checkArgument(key.length <= Short.MAX_VALUE);

		doPut(key, new Update(key, value));
	}

	public byte[] get(byte[] key) 
	throws IOException {

		checkState(!state.isShutdown());
		checkNotNull(key);
		checkArgument(key.length <= Short.MAX_VALUE);

		Operation o;
		byte[] value = null;

		if ((o = doGet(key)) != null) // it was here... 
			value = o.getValue();
		
		return value;
	}

	public void delete(byte[] key) 
	throws IOException {

		checkState(!state.isShutdown());
		checkNotNull(key);
		checkArgument(key.length <= Short.MAX_VALUE);

		doDelete(key);
	}

	public void shutdown() 
	throws IOException {

		/*
		 * Stop the API. Acquiring writeLock should guarantee that all user
		 * paths to API that were already down the road should have returned.
		 */
		checkState(!state.setShutdown());
		state.writeLock.lock();
		state.writeLock.unlock();
		
		// Stop the machinery. This could let us wait for a while...
		scheduledExecutor.shutdown();
		executor.shutdown();
				
		// freeze state
		state.shutdown();
	}

	private Operation doGet(byte[] key) 
	throws IOException {
		
		state.readLock.lock();
		try {
			
			Operation o = null;
			for (OperationReader reader: state.getReaders())
				if ((o = reader.get(key)) != null)
					break;

			return o;
			
		} finally {
			state.readLock.unlock();
		}
	}

	private void doPut(byte[] key, Operation o) 
	throws IOException {

		state.readLock.lock();
		try {

			state.getMutator().put(key, o);

		} finally {
			state.readLock.unlock();
		}
	}
	
	private void doDelete(byte[] key) 
	throws IOException {

		state.readLock.lock();
		try {

			state.getMutator().delete(key);

		} finally {
			state.readLock.unlock();
		}
	}
	
	private void init(String path) 
	throws IOException {
		
		scheduledExecutor.scheduleAtFixedRate(new Flusher(), 
											  conf.getInt("sketches.flusherdelay", 10000), 
											  conf.getInt("sketches.flusherdelay", 10000), 
											  TimeUnit.MILLISECONDS);
		scheduledExecutor.scheduleAtFixedRate(new ScriberScheduler(), 
				  							  conf.getInt("sketches.scriberdelay", 30), 
				  							  conf.getInt("sketches.scriberdelay", 30), 
				  							  TimeUnit.SECONDS);
		
		state.init();
	}
	
	private void scheduleCompaction() {
		int maxFiles = conf.getInt("sketches.sfile.maxfiles", 4);
		
		state.readLock.lock();
		try {
			
			if (state.calculateCompactables() > maxFiles && state.startCompaction())
				executor.submit(new Compactor());
				
		} finally {
			state.readLock.unlock();
		}
	}
	
	/* +---------------------------------+
	 * | ScheduledTask Robots start here |
	 * +---------------------------------+
	 */
	
	/*
	 * Flusher is executed every sketches.flusherdelay (default: 10s)
	 * and makes sure written data doesn't stay in buffer too long
	 * for durability. 
	 */
	private class Flusher 
	implements Runnable {

		@Override
		public void run() {

			state.readLock.lock();
			try {
				
				state.getMutator().flush();

			} catch (Exception e) {
				logger.error("Error while flushing", e);
			} finally {
				state.readLock.unlock();
			}
		}
	}
	
	/*
	 * This is executed every sketches.scriberdelay (default: 30s) and checks
	 * whether Memstore has seen over sketches.memstore.maxsize (default: 64MB)
	 * of data. In that case it scribes it to SFile. Only one of these threads
	 * is running at each time and it triggers a compaction after it's done.
	 * It can run concurrently to a Flusher and to a Compactor.
	 * 
	 * Important: timing is an issue here, if filling memstore takes less time
	 * than scribing, we will have delays. This should only happen in a write-only
	 * scenario as writing to logfile or to SFile has the same cost (append-only 
	 * writes), but memstore has hashmap's overhead and scribing has the overhead 
	 * of sorting and indexing.
	 */ 
	private class ScriberScheduler 
	implements Runnable {

		@Override
		public void run() {
			int threshold = conf.getInt("sketches.memstore.maxsize", 64) * 1024 * 1024;

			state.readLock.lock();
			try {
				
				if (state.getMutator().getSize() >= threshold)
					executor.submit(new Scriber());
				
			} catch (Exception e) {
				logger.error("Error while running a scriber schedule", e);
			} finally {
				state.readLock.unlock();
			}
		}
	}
	
	private class Scriber
	implements Runnable {

		@Override
		public void run() {

			try {

				OperationReaders readers = state.getReaders();
				Memstore oldStore, newStore;
				SFile ramSFile;
				
				// 1st: create a new Memstore and create an in-memory SFile with the old one
				state.writeLock.lock();
				try {

					newStore = new Memstore();
					oldStore = readers.setMemstore(newStore);
					ramSFile = new RAMSFile(oldStore);
					readers.add(ramSFile);
					
					state.setMutator(newStore);
					state.log(new NewLogfile(newStore.getName()));
					
				} finally {
					state.writeLock.unlock();
				}

				String filename = FilenamesFactory.getSFileName();
				
				// 2nd: scribe the old memstore to a proper SFile
				SketchesHelper.scribe(oldStore, filename);
				
				// 3rd: substitute the in-memory SFile with the one on-disk
				state.writeLock.lock();
				try {

					// switch temporary RAMSFile with fresh new SFile
					readers.remove(ramSFile);
					readers.add(new FSSFile(filename));
					
					state.log(new NewScribedSFile(filename, oldStore.getName()));

				} finally {
					state.writeLock.unlock();
				}

				// 4th: see if there's work for the Compactor
				scheduleCompaction();
				
			} catch (Exception e) {
				logger.error("Error while running scribing", e);
			} 
		}
		
	}
	/*
	 * Compactor is triggered by the Scriber or by itself and there will be just one
	 * Compactor running each time. For long-running Compactors it can happen that
	 * Scriber has created new SFiles in the meantime (thus exceding sketches.sfile.maxfiles),
	 * therefor Compactor triggers a possible new Compactor when it's finished.
	 */ 
	private class Compactor 
	implements Runnable {

		private final OperationReaders readers = state.getReaders();
		
		@Override
		public void run() { 

			try {

				SFile younger, older;
				boolean major;

				// 1st: select the SFiles to compact
				state.readLock.lock();
				try {

					OperationReader[] readersArray = readers.toArray();
					
					try {

						int idx = selectSFile(readersArray);
					
						major   = (idx == readersArray.length - 1);
						older   = (SFile) readersArray[idx];
						younger = (SFile) readersArray[idx - 1];
					
					} catch (UncompactableException e) {
						logger.debug("Aborting Compaction", e);
						return;
					}
					
				} finally { 
					state.readLock.unlock();
				}
				
				String filename = FilenamesFactory.getSFileName();

				// 2nd: compact them
				SketchesHelper.compact(younger.getName(), older.getName(), filename, major);

				// 3rd: remove the old SFiles and insert the fresh compact SFile
				state.writeLock.lock();
				try {
				
					readers.remove(younger);
					readers.remove(older);
					readers.add(new FSSFile(filename));
					
					state.log(new NewCompactedSFile(younger.getName(), older.getName(), filename, major));

				} finally {
					state.writeLock.unlock();
				}
				
			} catch (Exception e) {
				logger.error("Error while running compaction", e);
			} finally {
				state.stopCompaction();
				scheduleCompaction(); // is there more work for us?
			}
		}
		
		/*
		 * Selection policy. Try to merge files that are close in size, so we try to avoid small 
		 * adds to extablished big SFiles.
		 * Also, by taking minimum (possibly negative), we try to have bigger old files instead 
		 * of bigger young ones.
		 */
		private int selectSFile(OperationReader[] readersArray) {

			boolean compactable[] = new boolean[readersArray.length];
			long sizes[] = new long[readersArray.length];
			long diffs[] = new long[readersArray.length - 1];
			long min = Long.MAX_VALUE;
			int idx	 = -1;

			int i = 0;
			for (OperationReader reader: readersArray) {
				sizes[i] = reader.getSize();
				compactable[i++] = reader.isCompactable();
			}

			for (i = 0; i < diffs.length; i++) {
				diffs[i] = sizes[i + 1] - sizes[i];

				if (diffs[i] < min && compactable[i] && compactable[i + 1]) {
					min = diffs[i];
					idx = i;
				}
			}

			if (idx == -1)
				throw new UncompactableException("Can't find 2 compactable SFiles close");
				
			return idx;
		}
		
		private class UncompactableException extends RuntimeException {
			
			private static final long serialVersionUID = -8568111599942143236L;

			public UncompactableException(String error) {
				super(error);
			}
		}
	}
		
	public class SketchesState {

		private final AtomicBoolean isUnderCompaction = new AtomicBoolean(false);
		private final AtomicBoolean isShutdown        = new AtomicBoolean(false);
		private final OperationReaders readers        = new OperationReaders();
		private final ReentrantReadWriteLock lock     = new ReentrantReadWriteLock();
		private OperationMutator mutator;
		private Logfile stateLog;
		public final Lock readLock  = lock.readLock();
		public final Lock writeLock = lock.writeLock();

		public void init()
		throws IOException {

			StateLogReader stateLogReader = new StateLog.StateLogReader();
			stateLogReader.replay();
			
			String logfile = stateLogReader.getLogfile();
			List<String> sfiles    = stateLogReader.getSFiles();
			List<String> screibees = stateLogReader.getScribees();

		}

		public void shutdown() 
		throws IOException {
		
			for (OperationReader reader: readers)
				reader.close();
			
			stateLog.close();
		}

		/*
		 * Grants access to the readers List. Guarded by readLock for reading and by writeLock for
		 * modification.
		 */
		public OperationReaders getReaders() {
			return this.readers;
		}

		/*
		 * Grants access to the mutator. Guarded by readLock.
		 */
		public OperationMutator getMutator() {
			return this.mutator;
		}

		/*
		 * Sets a new mutator. Guarded by writeLock.
		 */
		public void setMutator(OperationMutator mutator) {
			this.mutator = mutator;
		}

		public boolean isUnderCompaction() {
			return isUnderCompaction.get();
		}
		
		public boolean startCompaction() {
			return isUnderCompaction.compareAndSet(false, true);
		}
		
		public boolean stopCompaction() {
			return isUnderCompaction.compareAndSet(true, false);
		}
		
		public int calculateCompactables() {

			readLock.lock();
			try {

				int compactableFiles = 0;
				for (OperationReader reader: readers)
					if (reader.isCompactable())
						compactableFiles++;

				return compactableFiles;

			} finally {
				readLock.unlock();
			}
		}
		
		public boolean isShutdown() {
			return isShutdown.get();
		}
		
		public boolean setShutdown() {
			return isShutdown.getAndSet(true);
		}
		
		public void log(Writable o) 
		throws IOException {

			stateLog.write(o);
		}
	}
}
