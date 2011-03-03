package org.acaro.graffiti;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class NioTest {

	public static void main(String[] args) throws IOException {
		String first = "first";
		String second = "second";
		
		ByteBuffer buff = ByteBuffer.allocate(first.getBytes().length+second.getBytes().length);
		buff.put(first.getBytes());
		buff.put(second.getBytes());
		buff.rewind();
		
		buff.limit(first.getBytes().length);
		ByteBuffer firstB = buff.slice();
		buff.position(buff.limit()).limit(buff.capacity());
		ByteBuffer secondB = buff.slice();
		byte[] b1 = new byte[firstB.capacity()];
		firstB.get(b1);
		byte[] b2 = new byte[secondB.capacity()];
		secondB.get(b2);
		System.out.println(new String(b1));
		System.out.println(new String(b2));
	}
}
