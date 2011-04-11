package org.acaro.sketches;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class Experiment {
	public static void main(String[] args) throws IOException {
		
		RandomAccessFile file = new RandomAccessFile("prova.idx", "rw");
		FileChannel channel = file.getChannel();
		
		file.writeByte((byte) 1);
		file.writeLong(10);
		
		file.write(new byte[1024]);
		
		MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, 9, 1024);
		
		file.writeInt(1);
		System.out.println(file.getFilePointer());
		file.writeInt(2);
		System.out.println(file.getFilePointer());
		file.writeInt(3);
		System.out.println(file.getFilePointer());
		buffer.putInt(1);
		System.out.println(file.getFilePointer());
		buffer.putInt(2);
		System.out.println(file.getFilePointer());
		buffer.putInt(3);
		System.out.println(file.getFilePointer());
		file.writeInt(4);
		System.out.println(file.getFilePointer());
		file.writeInt(5);
		System.out.println(file.getFilePointer());
		file.writeInt(6);
		System.out.println(file.getFilePointer());
		
		file.getFD().sync();
		file.close();
	}
}
