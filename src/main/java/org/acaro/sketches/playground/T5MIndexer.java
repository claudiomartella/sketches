package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.sfile.FSSFileIndexer;

public class T5MIndexer {

	public static void main(String[] args) throws IOException {
		FSSFileIndexer indexer = new FSSFileIndexer("./resources/5merged.br");
		long start = System.currentTimeMillis();
		indexer.call();
		System.out.println(System.currentTimeMillis() - start);
	}
}
