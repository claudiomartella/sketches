package org.acaro.sketches.playground;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.acaro.sketches.mural.MuralIterator;
import org.acaro.sketches.sketch.Sketch;
import org.acaro.sketches.util.MurmurHash3;

public class MuralIndexerText {
	private static int numberOfItems;
	
	public static void main(String[] args) throws IOException {
		MuralIterator iter = new MuralIterator("./resources/test1.br");
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("./resources/test1.br.txt")));
		numberOfItems = iter.getNumberOfItems();
		HashMap<Integer, List<Long>> mappa = new HashMap<Integer, List<Long>>(numberOfItems);
		
		while (iter.hasNext()) {
			Sketch s = iter.next();
			long off = iter.getLastOffset();
		
			int bucket = calculateBucket(s.getKey());
			int key    = T5MInserter.byteArrayToInt(s.getKey());
			
			List<Long> items = mappa.get(bucket);
			if (items == null) {
				items = new ArrayList<Long>();
				mappa.put(bucket, items);
			}

			items.add(off);
		}

		TreeMap<Integer, List<Long>> sorted = new TreeMap<Integer, List<Long>>();
		sorted.putAll(mappa);
		
		for (Entry<Integer, List<Long>> entry: sorted.entrySet()) {
			writer.print(entry.getKey() + ": ");
			for (Long item: entry.getValue())
				writer.print(item + " ");
			
			writer.println();
		}
		
		iter.close();
		writer.flush();
		writer.close();
	}
	
	private static int calculateBucket(byte[] key) {
		return Math.abs(MurmurHash3.hash(key)) % numberOfItems;
	}
}
