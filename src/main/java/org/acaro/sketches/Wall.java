package org.acaro.sketches;

public class Wall {
	//private BookIndex index;
	// sizeof(byte)+sizeof(long)
	final public static int HEADER_SIZE = 1+8;
	final public static byte CLEAN = 1;
	final public static byte DIRTY = 2;
	
	public Sketch get(byte[] key) {
		Sketch s = null;
		
		//long offset = index.get(key);
		//if (offset > 0) {
		//	s = getData(offset);
		//}
		
		return s;
	}
}
