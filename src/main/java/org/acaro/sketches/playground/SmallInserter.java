package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.OldSketches;
import org.acaro.sketches.util.Bytes;

public class SmallInserter {

	public static void main(String[] args) throws IOException {
		OldSketches sketches = new OldSketches("./resources/", "smallfile");
		
		byte[] value = new byte[256];
		
		long start = System.currentTimeMillis();
		long totalTime = 0;
		
		for (int i = 0; i < 150000; i++) {
			byte[] key = Bytes.fromInt(i).toByteArray();
			long localStart = System.currentTimeMillis();
			sketches.put(key, value);
			totalTime += System.currentTimeMillis()-localStart;
		}
		sketches.shutdown();

		System.out.println("TotalTime: " + (System.currentTimeMillis()-start));
		System.out.println("Sketches: " + totalTime);
	}
}
