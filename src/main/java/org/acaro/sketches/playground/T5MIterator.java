package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.mural.MuralIterator;
import org.acaro.sketches.sketch.Sketch;

public class T5MIterator {

	public static void main(String[] args) throws IOException {
		MuralIterator iterator = new MuralIterator("./resources/test1.br");
		
		System.out.println(iterator.getNumberOfItems());
		
		Sketch last = iterator.next();
		while (iterator.hasNext()) {
			Sketch s = iterator.next();
			if (T5MInserter.byteArrayToInt(last.getKey()) != T5MInserter.byteArrayToInt(s.getKey())-1) {
				System.out.println("we jumped more than 1 step: " + 
						T5MInserter.byteArrayToInt(last.getKey()) + 
						", " + T5MInserter.byteArrayToInt(s.getKey()));
			}
			last = s;
		}
		
		iterator.close();
	}
}
