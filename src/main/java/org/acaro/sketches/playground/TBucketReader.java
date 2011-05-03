package org.acaro.sketches.playground;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;

public class TBucketReader {
	static RandomAccessFile file;
	
	public static void main(String[] args) throws IOException {
		long bucket = -1;
		
		if (args.length == 1)
			bucket = Long.parseLong(args[0]);
		else {
			System.out.println("pass me the bucket address");
			System.exit(-1);
		}
		
		file = new RandomAccessFile("./resources/test1.br.tmp", "r");

		while (bucket != 0) {
			file.seek(bucket);
			
			bucket = file.readLong();
			long offset = file.readLong();
			
			System.out.print("nextItem: " + bucket + " offset: " + offset + " --> ");
			System.out.flush();
		}		
		
		file.close();
	}
}
