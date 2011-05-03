package org.acaro.sketches.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.acaro.sketches.mural.Mural;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyBufferedRandomAccessFile extends RandomAccessFile {
	private static final Logger logger = LoggerFactory.getLogger(MyBufferedRandomAccessFile.class);
	private static final int BUFF_SIZE = 65535;
	private final byte[] oneByte = new byte[1];
	private byte[] buffer;
	private boolean isDirty = false;
	private long left, right, current;
	private long fileLength;
	private FileChannel channel;

	public MyBufferedRandomAccessFile(String filename, String mode) throws IOException {
		this(new File(filename), mode);
	}

	public MyBufferedRandomAccessFile(File file, String mode) throws IOException {
		super(file, mode);
		buffer  = new byte[BUFF_SIZE];
		channel = super.getChannel();
		// if in read-only mode, caching file size
		fileLength = (!mode.contains("w")) ? this.channel.size() : -1;
		syncBuffer();
	}

	public void sync() throws IOException {
		flush();
		if (channel.isOpen())
			channel.force(true);
	}

	public void flush() throws IOException {
		if (isDirty) {
			if (channel.position() != left)
				channel.position(left);
			
			super.write(buffer, 0, (int) (right - left));
			isDirty = false;
		}
	}

	public void syncBuffer() throws IOException {
		flush(); // handle dirty buffer

		left = current;

		if (left >= channel.size()) {
			right = 0;
		} else {
			channel.position(left);

			int read = 0;
			while (read < buffer.length) {
				int n = super.read(buffer, read, buffer.length - read);
				if (n < 0)
					break;
				read += n;
			}
			right = left + read;
		}
	}

	@Override 
	public int read() throws IOException {
		if (isEOF())
			return -1;

		if (bufferExhausted())
			syncBuffer();

		return (int) buffer[(int) (current++ - left)] & 0xFF;
	}

	@Override
	public int read(byte[] buffer) throws IOException {
		return read(buffer, 0, buffer.length);
	}

	@Override
	public int read(byte[] buff, int offset, int length) throws IOException {
		if (length == 0)
			return 0;

		if (isEOF())
			return -1;

		if (bufferExhausted())
			syncBuffer();

		int numberOfBytes = Math.min((int) (right - current), length);

		System.arraycopy(buffer, (int) (current - left), buff, offset, numberOfBytes);
		current += numberOfBytes;

		return numberOfBytes;
	}

	@Override
	public void write(int val) throws IOException {
		oneByte[0] = (byte) val;
		this.write(oneByte, 0, 1);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] buff, int offset, int length) throws IOException {
		if (isReadOnly())
			throw new IOException("Can't write to a read-only channel");

		while (length > 0) {
			int wrtn = doWrite(buff, offset, length);
			offset  += wrtn;
			length  -= wrtn;
			isDirty  = true;
		}
	}

	private int doWrite(byte[] buff, int offset, int length) throws IOException {
		if (current >= left + buffer.length)
			syncBuffer();

		//System.out.println("left: " + left + " current: " + current + " position: " + channel.position());
		
		int internalPos   = (int) (current - left);
		int numberOfBytes = Math.min(buffer.length - internalPos, length);
		System.arraycopy(buff, offset, buffer, internalPos, numberOfBytes);
		current += numberOfBytes;
		right = Math.max(right, current); // have we gone further?

		return numberOfBytes;
	}

	@Override
	public void seek(long position) throws IOException {
		if (position < 0)
			throw new IllegalArgumentException("position should be positive");

		current = position;

		// outside of the buffer
		if (position < left || position >= right)
			syncBuffer();
	}

	@Override
	public int skipBytes(int count) throws IOException {
		int skipped = 0;

		if (count > 0) { 
			long currentOnFile = getFilePointer(), endOfFile = length();
			// cannot skip beyond EOF!
			skipped = (currentOnFile + count > endOfFile) ? (int) (endOfFile - currentOnFile) : count;
			seek(currentOnFile + skipped);
		}

		return skipped;
	}

	public long length() throws IOException {
		return (fileLength == -1) ? (Math.max(channel.size(), right)) : fileLength;
	}

	public long getFilePointer() {
		return current;
	}

	private boolean isReadOnly() {
		return fileLength != -1;
	}

	private boolean bufferExhausted() {
		return current == right;
	}

	public boolean isEOF() throws IOException {
		return getFilePointer() == length();
	}

	@Override
	public void close() throws IOException {
		sync();
		buffer = null;

		super.close();
	}
}
