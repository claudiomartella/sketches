package org.acaro.sketches.util;

import java.util.Comparator;

import org.acaro.sketches.sketch.Sketch;

import com.google.common.primitives.UnsignedBytes;

public class SketchComparator implements Comparator<Sketch> {
	private Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

	public int compare(Sketch s1, Sketch s2) {
		return comparator.compare(s1.getKey(), s2.getKey());
	}
}
