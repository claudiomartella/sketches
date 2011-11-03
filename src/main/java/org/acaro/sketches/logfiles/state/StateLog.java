package org.acaro.sketches.logfiles.state;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.acaro.sketches.io.SmartReader;
import org.acaro.sketches.io.SmartWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateLog {

	private static final byte COMPACTED = 'c';
	private static final byte SCRIBED   = 's';
	private static final byte NEWLOG    = 'l';
	private SmartWriter log;
	
	public StateLog() {
		
	}
	
	public synchronized void compacted(String older, String younger, String fresh) 
	throws IOException {
		
		log.writeByte(COMPACTED);
		log.writeUTF(fresh);
		log.writeUTF(older);
		log.writeUTF(younger);
		sync();
	}

	public synchronized void scribed(String logfile, String sfile) 
	throws IOException {
		
		log.writeByte(SCRIBED);
		log.writeUTF(logfile);
		log.writeUTF(sfile);
		sync();
	}
	
	public synchronized void scribed(String logfile) 
	throws IOException {
		
		log.writeByte(NEWLOG);
		log.writeUTF(logfile);
		sync();
	}
	
	public synchronized void close() 
	throws IOException {
		
		if (log != null) {
			log.close();
			log = null;
		}
	}
	
	private void sync() 
	throws IOException {
		
		log.flush();
		log.sync();
	}
	
	public class StateLogReader {
		
		private final Logger logger = LoggerFactory.getLogger(StateLog.StateLogReader.class);
		private SmartReader reader;
		// the logfile to load to restore the memstore
		private String logfile;
		// the list of ssfiles that should be opened to build the database
		private ArrayList<String> ssfiles  = new ArrayList<String>();
		// the list of logfiles that need to be converted into ssfiles before we're ready
		private ArrayList<String> scribees = new ArrayList<String>();
		private boolean replayed = false;
		
		public StateLogReader(String filename) 
		throws IOException {
		
			this.reader = new SmartReader(new RandomAccessFile(filename, "r").getChannel());
		}
		
		public String getLogfile() {
			return this.logfile;
		}
		
		public List<String> getSFiles() {
			return this.ssfiles;
		}
		
		public List<String> getScribees() {
			return this.scribees;
		}
		
		public void replay() 
		throws IOException {
			
			if (replayed == true)
				return;
			
			try {

				while (reader.hasRemaining()) {

					byte type = reader.readByte();
					switch (type) {
					case NEWLOG: 
					{ 
						/*
						 * new logfile created with fname name as a result of a scribe.
						 * we put the old one in scribee because it means it's being scribed.
						 * the entry should be removed from scribee by the specific log entry.
						 * if at the end of load() it's still in scribee it means we died while
						 * scribing. 
						 */
						
						String l = reader.readUTF();
						if (logfile != null)
							scribees.add(logfile);
						
						logfile = l;

						break;
					} 
					case SCRIBED: 
					{ 
						/*
						 * new ssfile as a result of scribing the logfile. The file is now
						 * in the ssfiles list and can be removed from the scribees. The process
						 * who wrote this log entry should have deleted the old logfile.
						 */
						String f1 = reader.readUTF();
						String f2 = reader.readUTF();
						
						ssfiles.add(f2); // new ssfile filename
						scribees.remove(f1); // old logfile filename

						break;
					} 
					case COMPACTED: 
					{ 
						/*
						 * new ssfile as a result of the compaction of two ssfiles. the file should
						 * be put with the other ssfiles and the two compacted ssfiles removed. the
						 * process who wrote this log entry should have deleted the two ssfiles.
						 */

						String f1 = reader.readUTF();
						String f2 = reader.readUTF();
						String f3 = reader.readUTF();
						
						ssfiles.add(f1); // new ssfile filename
						ssfiles.remove(f2); // first ssfile filename
						ssfiles.remove(f3); // second ssfile filename

						break;
					} 
					default: 
						logger.debug("StateLogFile malformed, unknown type " + type);
					}
				}
			} catch (EOFException e) {
				logger.info("Truncated StateLog, hit unexpected EOF");
			} finally {
				replayed = true;
				reader.close();
			}
		}
	}	
}
