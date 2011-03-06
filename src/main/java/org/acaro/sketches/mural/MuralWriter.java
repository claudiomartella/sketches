package org.acaro.sketches.mural;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.sketch.Sketch;

public class MuralWriter implements Closeable {
	private FileOutputStream file;
	private BufferedOutputStream bos;
	private long timestamp = 0;
	private int totalItems = 0;
	
	public MuralWriter(String filename) throws IOException {
		this.file = new FileOutputStream(filename);
		this.bos = new BufferedOutputStream(file);
		init();
	}
	
	public void write(Sketch sketch) throws IOException {
		for (ByteBuffer buffer: sketch.getBytes()) {
			bos.write(buffer.array());
		}
		updateTimestamp(sketch);
		totalItems++;
	}

	public void close() throws IOException {
		ByteBuffer header = ByteBuffer.allocate(Mural.HEADER_SIZE);
		header.put(Mural.CLEAN);
		header.putLong(timestamp);
		header.putInt(totalItems);
		writeHeader(header);
		sync();
		bos.close();
	}

	private void init() throws IOException {
		ByteBuffer header = ByteBuffer.allocate(Mural.HEADER_SIZE);
		header.put(Mural.DIRTY);
		header.putLong(0);
		header.putInt(0);
		writeHeader(header);
		sync();
	}
	
	private void writeHeader(ByteBuffer header) throws IOException {
		FileChannel channel = file.getChannel();
		channel.position(0);
		while (channel.write(header) > 0);
	}

	private void sync() throws IOException {
		bos.flush();
		file.getFD().sync();
	}
	
	private void updateTimestamp(Sketch sketch) {
		if (sketch.getTimestamp() > timestamp)
			timestamp = sketch.getTimestamp();
	}
}
