package org.acaro.sketches;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 
 * @author Claudio Martella
 * 
 * Append-only log file.
 *
 */

public class SyncSketchBook implements SketchBook {
	private FileOutputStream fos;
	private FileChannel ch;

	public SyncSketchBook(String filename) throws IOException {
		fos = new FileOutputStream(filename, true);
		ch  = fos.getChannel();
	}
	
	public synchronized void write(Sketch b) throws IOException {
		ByteBuffer[] bb = b.getBytes();
		while (ch.write(bb) > 0);
		ch.force(false);
	}
	
	public synchronized void close() throws IOException {
		fos.getFD().sync();
		fos.close();
	}
	
/*	private void writeFully(FileChannel ch, ByteBuffer[] bytes) throws IOException {
		long fullLength = calculateFullLength(bytes);
		long fullyWritten = 0;

		while (fullyWritten < fullLength) {
			long written = ch.write(bytes);
			if (written > 0) {
				fullyWritten += written;
			} else  {
				Thread.yield();
			}
		}
	}

	private long calculateFullLength(ByteBuffer[] bytes) {
        long fullLength = 0;
        
        for (int i = 0; i < bytes.length; i++)
        	fullLength += bytes[i].remaining();
        
        return fullLength;
	}*/
}
