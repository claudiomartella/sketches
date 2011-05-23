package org.acaro.sketches.playground;

import java.io.IOException;
import java.util.Random;

import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.sfile.FSSFile;
import org.acaro.sketches.util.Bytes;

public class T5MReader {

	public static void main(String[] args) throws IOException {
		FSSFile mural = new FSSFile("./resources/5m.br");
		
		long totalTime = 0;
		for (int i = 0; i < 1000000; i++) {
			byte[] key = Bytes.fromLong(i).toByteArray();
			long start = System.currentTimeMillis();
			Operation s = mural.get(key);
			totalTime += (System.currentTimeMillis() - start);
			
			if (s == null)
				System.out.println("PROBLEM! Couldn't find entry: " + i);
		}
		
		System.out.print("Totaltime: " + totalTime);
		System.out.println(" avg: " + (double) totalTime /1000000);
	}
}
