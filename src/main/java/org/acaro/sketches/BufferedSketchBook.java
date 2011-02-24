package org.acaro.sketches;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferedSketchBook implements SketchBook {
	private FileOutputStream fos;
	private BufferedOutputStream bos;
	private final int bufferSize = 64*1024;
	
	public BufferedSketchBook(String filename) throws IOException {
		fos = new FileOutputStream(filename, true);
		bos = new BufferedOutputStream(fos, bufferSize);
	}
	
	public synchronized void write(Sketch s) throws IOException {
		for (ByteBuffer buffer: s.getBytes()) {
			bos.write(buffer.array());
		}
	}

	public synchronized void close() throws IOException {
		bos.flush();
		fos.getFD().sync();
		bos.close();
	}
}
