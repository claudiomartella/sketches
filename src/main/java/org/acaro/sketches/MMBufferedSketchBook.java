package org.acaro.sketches;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class MMBufferedSketchBook implements SketchBook {
	private MappedByteBuffer buf;
	private FileChannel fc;
	
	public MMBufferedSketchBook(String filename) throws IOException {
		fc  = new RandomAccessFile(filename, "rw").getChannel();
		//buf = fc.map(MapMode.READ_WRITE, 0, fc.size());
	}
	
	public void write(Sketch s) throws IOException {
		/*for (ByteBuffer b: s.getBytes()) {
			buf.put(b);
		}*/
		ByteBuffer[] bb = s.getBytes();
		while (fc.write(bb) > 0);
	}

	public void close() throws IOException {
		//buf.force();
		fc.close();
	}
}
