package org.acaro.sketches.playground;

import java.io.IOException;
import java.util.Random;

import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.sfile.FSSFile;

public class T5MRandomReader {

	public static void main(String[] args) throws IOException {
		Random rand = new Random();
		FSSFile mural = new FSSFile("./resources/5merged.br");
		
		long totalTime = 0;
		for (int i = 0; i < 1000000; i++) {
			int k = rand.nextInt(1000000);
			byte[] key = Bytes.fromLong(k).toByteArray();
			long start = System.currentTimeMillis();
			Operation s = mural.get(key);
			totalTime += (System.currentTimeMillis() - start);
			
			if (s == null) {
				System.out.println("PROBLEM! Couldn't find entry: " + k);
			}
		}
		
		System.out.print("Totaltime: " + totalTime);
		System.out.println(" avg: " + (double) totalTime /1000000);
	}
}
