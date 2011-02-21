package org.acaro.sketches;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 
 * @author Claudio Martella
 * 
 * Append-only log file.
 *
 */

public class SketchBook {
	final public static String EXTENSION = ".sb";
	private FileChannel ch;

	/**
	 * @param filename to write logs to
	 * @throws IOException
	 */
	public SketchBook(String filename) throws IOException {
		RandomAccessFile f = new RandomAccessFile(filename, "rwd");
		f.seek(f.length());
		this.ch = f.getChannel();
	}
	
	public synchronized void write(Sketch b) throws IOException {
		this.ch.write(b.getBytes());
	}
	
	public synchronized void close() throws IOException {
		this.ch.force(true);
		this.ch.close();
	}
	
	public static String getName(String path, String name) {
		return path + "/" + name + EXTENSION;
	}
}
