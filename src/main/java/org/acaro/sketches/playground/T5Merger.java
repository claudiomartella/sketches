package org.acaro.sketches.playground;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

import org.acaro.sketches.sfile.FSSFileIterator;
import org.acaro.sketches.sfile.FSSFileWriter;
import org.acaro.sketches.sfile.FSSFileCursor;

public class T5Merger {

	public static void main(String[] args) throws IOException {
		FSSFileIterator odd  = new FSSFileIterator("./resources/5odd.br");
		FSSFileIterator even = new FSSFileIterator("./resources/5even.br");
		
		LinkedList<FSSFileIterator> iterators = new LinkedList<FSSFileIterator>();
		iterators.add(odd);
		iterators.add(even);
	
		Collections.sort(iterators, Collections.reverseOrder());
		
		FSSFileCursor cursor = new FSSFileCursor(iterators);
		FSSFileWriter writer  = new FSSFileWriter("./resources/5merged.br");
		
		long start = System.currentTimeMillis();
		
		while (cursor.hasNext())
			writer.write(cursor.next());
		
		System.out.println("Murals merged in: " + (System.currentTimeMillis()-start));
		
		writer.close();
		cursor.close();
	}
}
