package org.acaro.sketches.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;

public class MappedSmartReader 
implements DataInput {

	private MappedByteBuffer buffer;
	
	public MappedSmartReader(MappedByteBuffer buffer) {
		this.buffer = buffer;
	}
	
	public boolean readBoolean() 
	throws IOException {
	
		try {
			return buffer.get() != 0;
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public byte readByte() 
	throws IOException {
	
		try {
			return buffer.get();
		} catch(BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public char readChar() 
	throws IOException {
		
		try {
			return buffer.getChar();
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public double readDouble() 
	throws IOException {
		
		try {
			return buffer.getDouble();
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public float readFloat() 
	throws IOException {
	
		try { 
			return buffer.getFloat();
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public void readFully(byte[] b) 
	throws IOException {
		
		try {
			buffer.get(b);
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public void readFully(byte[] b, int offset, int length) 
	throws IOException {
	
		try {
			buffer.get(b, offset, length);
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public int readInt() 
	throws IOException {
	
		try {
			return buffer.getInt();
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public String readLine() 
	throws IOException {
	
		throw new UnsupportedOperationException("readLine not supported");
	}

	public long readLong() 
	throws IOException {
		
		try { 
			return buffer.getLong();
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public short readShort() 
	throws IOException {
	
		try {
			return buffer.getShort();
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public String readUTF() 
	throws IOException {

		short l  = readShort();
		byte[] b = new byte[l];
		readFully(b);
		
		return new String(b, "UTF-8");
	}

	public int readUnsignedByte() 
	throws IOException {
		
		try {
			int ch = buffer.get();
			
			return ch;
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public int readUnsignedShort() 
	throws IOException {
		
		try { 
			int ch1 = buffer.get();
			int ch2 = buffer.get();

			return (ch1 << 8) + (ch2 << 0);
		} catch (BufferUnderflowException e) {
			throw new EOFException();
		}
	}

	public int skipBytes(int offset) 
	throws IOException {
	
		int actual = offset;
		if (offset > buffer.remaining())
			actual = buffer.remaining();
		
		buffer.position(buffer.position() + actual);
			
		return actual;
	}
}
