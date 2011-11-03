package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.OldSketches;
import org.acaro.sketches.Sketches;

public class BigInserter {

	public static void main(String[] args) throws IOException {
		Sketches sketches = new Sketches("./resources/");
		byte[] value = new byte[1];
		
		long start = System.currentTimeMillis();
		
		for (int i = 0; i < 5000000; i++)
			sketches.put(Bytes.fromInt(i).toByteArray(), value);
		
		System.out.println(System.currentTimeMillis()-start);
		
		sketches.bomb();
		sketches.shutdown();
	}
}
