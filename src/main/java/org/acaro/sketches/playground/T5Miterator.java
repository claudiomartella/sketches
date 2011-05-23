package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.Update;
import org.acaro.sketches.sfile.FSSFileIterator;

public class T5Miterator {

	public static void main(String[] args) throws IOException {
		FSSFileIterator iter = new FSSFileIterator("./resources/5merged.br");

		while (iter.hasNext()) {
			Operation s    = iter.next();
			long offset = iter.getLastOffset();

			if (s instanceof Update)
				System.out.print("Update => ");
			else
				System.out.print("Delete => ");
			
			System.out.println("key: " + fromArrayToLong(s.getKey()));
		}
		
		iter.close();
	}

	public static long fromArrayToLong(byte[] array) {
		long l = 0;

		for(int i =0; i < 8; i++){    
			l <<= 8;
			l ^= (long)array[i] & 0xFF;    
		}
		
		return l;
	}
}
