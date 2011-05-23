package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.OldSketches;
import org.acaro.sketches.util.Bytes;

public class BigInserter {

	public static void main(String[] args) throws IOException {
		OldSketches sketches = new OldSketches("./resources/", "bigfile");
		byte[] value = new byte[1];
		
		long start = System.currentTimeMillis();
		
		for (int i = 0; i < 5000000; i++)
			sketches.put(Bytes.fromInt(i).toByteArray(), value);
		
		System.out.println(System.currentTimeMillis()-start);
		
		sketches.bomb();
		sketches.shutdown();
	}
}
