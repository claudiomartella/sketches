package org.acaro.sketches.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SmartBlockManager {
	private static final int DEFAULT_BLOCKSIZE = 6;
	private static final int MAX_IO_RETRIES = 3;
	private RandomAccessFile file;
	private ByteBuffer buffer;
	private FileChannel channel;

	public SmartBlockManager(String filename, int blockSize) throws IOException {
		this.file    = new RandomAccessFile(filename, "rw");
		this.buffer  = ByteBuffer.allocateDirect(blockSize);
		this.channel = this.file.getChannel();
	}
	
	public SmartBlockManager(String filename) throws IOException {
		this(filename, DEFAULT_BLOCKSIZE);
	}
	
	public long addNewBlock(long first, long second) throws IOException {
		buffer.putLong(first).putLong(second);
		
		long position = channel.size();
		writeBlock(position);
		
		return position;
	}

	public long addToBlock(long last, long offset) throws IOException {
		readBlock(last);
		
		while (buffer.hasRemaining()) {
			
		}
		
		return 0;
	}
		
	private void readBlock(long position) throws IOException {
		channel.position(position);
		
		int ret   = 0;
		int retry = MAX_IO_RETRIES;

		buffer.clear();
		
		while (buffer.hasRemaining() && retry > 0) {
			ret = channel.read(buffer);
			if (ret > 0)
				retry = MAX_IO_RETRIES;
			else if (ret == 0) 
				retry--; // let's try again
			else // if (ret < 0) hit EOF! 
				break;
		}
		
		if (retry == 0)
			throw new IOException("couldn't read the block after " + MAX_IO_RETRIES + " tries");
		
		buffer.flip();
	}
	
	private void writeBlock(long position) throws IOException {
		int retry = MAX_IO_RETRIES;
		int ret;

		buffer.flip();
		
		while (buffer.remaining() > 0 && retry > 0) {
			ret = channel.write(buffer);
			if(ret == 0)
				retry--;
			else
				retry = MAX_IO_RETRIES;
		}

		if (buffer.remaining() > 0)
			throw new IOException("couldn't write the block after " + MAX_IO_RETRIES + " tries");
		
		buffer.clear();
	}
}
