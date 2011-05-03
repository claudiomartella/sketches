package org.acaro.sketches.playground;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;

import org.acaro.sketches.mural.Index;
import org.acaro.sketches.mural.MuralIterator;
import org.acaro.sketches.mural.SmallIndex;
import org.acaro.sketches.util.MurmurHash3;

public class IndexReader {

	public static void main(String[] args) throws IOException {
		RandomAccessFile file = new RandomAccessFile("./resources/test1.br", "r");
		MuralIterator iterator = new MuralIterator("./resources/test1.br");
		Index index = new SmallIndex(file.getChannel(), MapMode.READ_ONLY, iterator.getIndexOffset(), file.length()-iterator.getIndexOffset());
		System.out.println(iterator.getIndexOffset());
		System.out.println(index.getOffset((Math.abs(MurmurHash3.hash(T5MInserter.intToByteArray(951)))%iterator.getNumberOfItems())*8));
		iterator.close();
		file.close();
	}
}
