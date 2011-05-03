package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.util.MyBufferedRandomAccessFile;

public class MyBufferedRandomAccessFileTester {

	public static void main(String[] args) throws IOException {
		MyBufferedRandomAccessFile file = new MyBufferedRandomAccessFile("./resources/prova.txt", "rw");
		
		for (int i = 0; i < 1000000; i++) {
			file.writeByte(1);
		}
		
		file.close();
		
		file = new MyBufferedRandomAccessFile("./resources/prova.txt", "r");
		
		int i = 0;
		while (!file.isEOF()) {
			System.out.println(file.readByte());
			i++;
		}
		System.out.println(i);
		file.close();
	}
}
