package org.acaro.sketches.playground;

import java.io.IOException;
import java.util.Random;

import org.acaro.sketches.OldSketches;

public class T5MEvenInserter {

	public static void main(String[] args) throws IOException {
		OldSketches sketches = new OldSketches("./resources/", "5even");
		Random rand = new Random();
		
		long totalTime = 0;
		
		byte[] value = new byte[1024];
		byte[] key;
		
		for (long i = 0; i < 1000000; i++) {
			rand.nextBytes(value);
			key = Bytes.fromLong(i).toByteArray();
			
			long start = System.currentTimeMillis();
			
			if (i % 2 == 0)
				sketches.put(key, value);
			else
				sketches.delete(key);
			
			totalTime += System.currentTimeMillis()-start;
		}
		
		System.out.println("Total write time: " + totalTime);
		
		long start = System.currentTimeMillis(); 
		sketches.bomb();
		System.out.println("Bombing time: " + (System.currentTimeMillis() - start));
		sketches.shutdown();
	}
}
