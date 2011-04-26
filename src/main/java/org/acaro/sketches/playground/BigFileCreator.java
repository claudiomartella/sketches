package org.acaro.sketches.playground;

import java.io.*;

public class BigFileCreator {

	public static void main(String[] args) throws IOException {
		DataOutputStream file = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("./resources/bigfilewithlongs.dat", true), 65536));
		
		for (long i = 0; i < 500000000; i++) {
			file.writeLong(i);
		}
		file.flush();
		file.close();
	}
}
