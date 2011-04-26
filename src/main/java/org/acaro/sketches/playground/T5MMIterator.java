package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.mural.MuralIterator;
import org.acaro.sketches.sketch.Sketch;

public class T5MMIterator {

	public static void main(String[] args) throws IOException {
		MuralIterator iter = new MuralIterator("./resources/test1.br");
		
		System.out.println("dirty byte: "+ iter.getDirtyByte());
		System.out.println("total: "+ iter.getNumberOfItems());
		System.out.println("ts: "+ iter.getTimestamp());
		
		while (iter.hasNext()) {
			Sketch s = iter.next();
			System.out.println(byteArrayToInt(s.getKey()));
		}
		iter.close();
	}
	
	public static final int byteArrayToInt(byte [] b) {
        return (b[0] << 24)
                + ((b[1] & 0xFF) << 16)
                + ((b[2] & 0xFF) << 8)
                + (b[3] & 0xFF);
	}
}
