package org.acaro.sketches.playground;

import java.io.*;

public class SmallFileCreator {

	public static void main(String[] args) throws IOException {
		DataOutputStream file = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("./resources/smallfilewithlongs.dat", true), 65536));
		
		for (long i = 0; i < 100000000; i++) {
			file.writeLong(i);
		}
		file.flush();
		file.close();
	}
}
