package org.acaro.sketches.operation;

import java.util.Comparator;


import com.google.common.primitives.UnsignedBytes;

public class OperationComparator implements Comparator<Operation> {
	private Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

	public int compare(Operation s1, Operation s2) {
		return comparator.compare(s1.getKey(), s2.getKey());
	}
}
