package org.acaro.sketches;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 
 * @author Claudio Martella
 * 
 * Replay of the log to MindSketches. 
 * If no file is found an empty MindSketch is returned ready to be filled and used.
 *
 * No buffering is used for reading, cross your fingers and relay on the I/O Scheduler's read-ahead.
 */

public class SketchesHelper {
	
	private static final String EXTENSION = ".sb";

	public static MindSketches loadSketchBook(String file) throws IOException {
		MindSketches memory = new MindSketches();
		int totalLoaded = 0;
		
		System.err.println("Looking for " + file);
		
		try {
			// replay the log
			RandomAccessFile book = new RandomAccessFile(file, "r");
			
			System.err.println("Old logfile found, replaying...");
			
			FileChannel ch = book.getChannel();
			ByteBuffer header = ByteBuffer.allocate(Sketch.HEADER_SIZE);
			int size;	
			while ((size = ch.read(header)) == Sketch.HEADER_SIZE) {
				//System.err.println("we read the header:" + size);
				
				byte type = header.get(0);
				long ts = header.getLong(1);
				short keySize = header.getShort(9);
				int valueSize = header.getInt(11);

				Sketch s;
				ByteBuffer key, value;
				switch (type) {
				
				case Sketch.THROWUP: 

					key = ByteBuffer.allocate(keySize);
					value = ByteBuffer.allocate(valueSize);
					ByteBuffer[] tokens = { key, value };
					ch.read(tokens);
					s = new Throwup(key, value, ts);
					break;

				case Sketch.BUFF:

					key = ByteBuffer.allocate(keySize);
					ch.read(key);
					s = new Buff(key, ts); 
					break;

				default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
				}
				header.rewind();
				memory.put(s.getKey().array(), s);
				//System.err.println(totalLoaded++);
				totalLoaded++;
			}
			if (size != 0) {
				throw new IOException("Corrupted SketchBook: some data left: " + size);
			}
			System.err.println("Log replayed: " + totalLoaded + " entries loaded\n"+
								"memory is now " + memory.getSize());
			
			book.close();
			
		} catch (FileNotFoundException e) {
			System.err.println("No old log found, starting from scratch");
		} 
		
		return memory;
	}

	public static String getFilename(String path, String name) {
		return path + "/" + name + EXTENSION;
	}
}
