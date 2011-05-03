package org.acaro.sketches.playground;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.acaro.sketches.io.BufferedRandomAccessFile;
import org.acaro.sketches.mural.Index;
import org.acaro.sketches.mural.MuralIterator;
import org.acaro.sketches.mural.SmallIndex;

public class TMuralIndexerText {

	public static void main(String[] args) throws IOException {
		MuralIterator iter     = new MuralIterator("./resources/test1.br");
		RandomAccessFile mural = new RandomAccessFile("./resources/test1.br", "r");
		BufferedRandomAccessFile tfile = new BufferedRandomAccessFile("./resources/test1.br.tmp", "r");
		Index index = new SmallIndex(mural.getChannel(), MapMode.READ_ONLY, iter.getIndexOffset(), mural.length()-iter.getIndexOffset());
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("./resources/test1b.br.txt")));
		TreeMap<Integer, List<Long>> sorted = new TreeMap<Integer, List<Long>>();
		
		long indexOffset  = iter.getIndexOffset();
		
		int bucketNo = -1;
		while (index.hasRemaining()) {
			bucketNo++;
			long off = index.getOffset();
			if (off == 0) continue;
			
			LinkedList<Long> items = new LinkedList<Long>();
			if (off < indexOffset)
				items.add(off);
			else {
				TBucketIterator tb = new TBucketIterator(off-indexOffset, tfile);
				while (tb.hasNext())
					items.addFirst(tb.next());
			}
			
			sorted.put(bucketNo, items);
		}
		
		for (Entry<Integer, List<Long>> entry: sorted.entrySet()) {
			writer.print(entry.getKey() + ": ");
			for (Long item: entry.getValue())
				writer.print(item + " ");
			
			writer.println();
		}
		tfile.close();
		iter.close();
		mural.close();
		writer.flush();
		writer.close();
	}
}
