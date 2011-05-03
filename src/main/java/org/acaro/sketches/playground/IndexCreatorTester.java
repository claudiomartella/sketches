package org.acaro.sketches.playground;

import java.io.IOException;

public class IndexCreatorTester {

	public static void main(String[] args) {
		long idx;
		for (long i = 1; i < 50000000; i++)
			if ((idx = createIndex(i)) != i) 
				System.out.println("mismatch! " + i + " " + idx);
	}

	
	public static long createIndex(long indexSize) {
		long totalSize = 0;
		
		int CHUNK_SIZE = 65535;
		if (indexSize <= CHUNK_SIZE) {
			totalSize += indexSize;
		} else {
			int n = (int) ((indexSize % CHUNK_SIZE == 0) ? indexSize / CHUNK_SIZE : indexSize / CHUNK_SIZE + 1);
			
			int l = CHUNK_SIZE;
			for (int i = 0; i < n; i++) {
				if (i == n - 1) { // last chunk
					l = (int) (indexSize - i * CHUNK_SIZE);
				} 
				totalSize += l;
			}
		}
		
		return totalSize;
	}
}
