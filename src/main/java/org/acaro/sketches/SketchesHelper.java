package org.acaro.sketches;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * 
 * @author Claudio Martella
 * 
 * Replay of the log to MindSketches. 
 * If no file is found an empty MindSketch is returned ready to be filled and used.
 *
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
					s = new Throwup(key.array(), value.array(), ts);
					break;

				case Sketch.BUFF:

					key = ByteBuffer.allocate(keySize);
					ch.read(key);
					s = new Buff(key.array(), ts); 
					break;

				default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
				}
				header.rewind();
				memory.put(s.getKey(), s);
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

	public static MindSketches loadMappedSketchBook(String file) throws IOException {
		MindSketches memory = new MindSketches();
		RandomAccessFile book = null;
		FileChannel ch = null;
		int loaded=0;
		
		try {
			book = new RandomAccessFile(file, "rw");
			ch = book.getChannel();
			MappedByteBuffer buffer = ch.map(MapMode.READ_WRITE, 0, ch.size());
			buffer.load();
			
			while (buffer.hasRemaining()) {
				byte type = buffer.get();
				long ts = buffer.getLong();
				short keySize = buffer.getShort();
				int valueSize = buffer.getInt();
				
				Sketch s;
				byte[] key, value;
				
				switch (type) {
				
				case Sketch.THROWUP: 

					key = new byte[keySize];
					value = new byte[valueSize];
					buffer.get(key); 
					buffer.get(value);
					s = new Throwup(key, value, ts);
					break;

				case Sketch.BUFF:

					key = new byte[keySize];
					buffer.get(key);
					s = new Buff(key, ts); 
					break;

				default: throw new IOException("Corrupted SketchBook: read unknown type: " + type); 
				}
				memory.put(s.getKey(), s);
				loaded++;
			}
			
			book.close();
		} catch (FileNotFoundException e) {
			System.err.println("No old log found, starting from scratch");
		} catch (BufferUnderflowException e) {
			System.err.println("Truncated file, we probably died without synching correctly");
			ch.truncate(ch.position());
			ch.force(true);
			book.close();
		}

		System.err.println(loaded + " loaded: " + memory.getSize());
		
		return memory;
	}
	
	public static String getFilename(String path, String name) {
		return path + "/" + name + EXTENSION;
	}
}
