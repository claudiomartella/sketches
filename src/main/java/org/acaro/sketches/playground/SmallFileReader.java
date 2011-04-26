package org.acaro.sketches.playground;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;

import org.acaro.sketches.mural.BigIndex;
import org.acaro.sketches.mural.SmallIndex;

public class SmallFileReader {

	public static void main(String[] args) throws IOException {
		RandomAccessFile file  = new RandomAccessFile("./resources/smallfilewithlongs.dat", "r");
		SmallIndex bf = new SmallIndex(file.getChannel(), MapMode.READ_ONLY, 0, file.length());
		
/*		long i = 0;
		try {
			for (i = 0; i < 500000000; i++) {
				long data = bf.getOffset(i*8);
				//System.out.println("i=" + i + " read=" + data);
				if (data != i) {
					System.out.println("ERROR: " + data);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(i);
		}
*/		
		long i = 0;
		while (bf.hasRemaining()) {
			long data = bf.getOffset();
			if (data != i++) {
				System.out.println("ERROR: " + data);
			}
		}
	}
}
