package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.mural.Mural;
import org.acaro.sketches.sketch.Sketch;

public class T5MReader {
	public static void main(String[] args) throws IOException {
		Mural mural = new Mural("./resources/test1.br");
	
		long totalTime = 0;
		
		for (int i = 0; i < 5000000; i++) {
			byte[] key = T5MInserter.intToByteArray(i);
			long start = System.currentTimeMillis();
			Sketch s = mural.get(key);
			totalTime += (System.currentTimeMillis()-start);
			if (s == null)
				System.out.println(i + ": item not found!");
			//else if (!s.getKey().equals(key))
			//	System.out.println(i + ": wrong item, key mismatch: " + byteArrayToInt(s.getKey()));

		}
		
		System.out.println("Totaltime: " + (double) totalTime);
		
//		System.out.println(byteArrayToInt(mural.get(intToByteArray(5)).getKey()));
		mural.close();
	}
}
