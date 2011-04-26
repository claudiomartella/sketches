package org.acaro.sketches.playground;

import org.acaro.sketches.util.MurmurHash3;

public class MurMurChecker {

	public static void main(String[] args) {
		int[] keys = {951, 4364, 7264, 8170};
		
		for (int i: keys) 
			System.out.println(Math.abs(MurmurHash3.hash(T5MInserter.intToByteArray(i)))% 5000000);
		
		System.out.println((Math.abs(MurmurHash3.hash(T5MInserter.intToByteArray(951)))% 5000000)*8);
	}
}
