package org.acaro.sketches.playground;

import java.io.IOException;

import org.acaro.sketches.Sketches;

public class T5MInserter {

	public static void main(String[] args) throws IOException {
		Sketches sketches = new Sketches("./resources/", "test1");
		
		for (int i = 0; i < 5000000; i++) {
			byte[] data = intToByteArray(i);
			sketches.put(data, data);
			if (i % 1000 == 0)
				System.out.println("1000 elements inserted: "+ i);
		}
		sketches.bomb();
		sketches.shutdown();
	}
	
	public static final byte[] intToByteArray(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value};
	}
	
	public static final int byteArrayToInt(byte [] b) {
        return (b[0] << 24)
                + ((b[1] & 0xFF) << 16)
                + ((b[2] & 0xFF) << 8)
                + (b[3] & 0xFF);
	}
}
