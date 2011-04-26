package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.mural.MuralIndexer;

public class T5MIndexer {

	public static void main(String[] args) throws IOException {
		MuralIndexer indexer = new MuralIndexer("./resources/test1.br");
		long start = System.currentTimeMillis();
		indexer.call();
		System.out.println("file index in: " + (System.currentTimeMillis()-start)/1000);
	}
}
