package org.acaro.sketches.mural;

import java.io.IOException;

import org.acaro.sketches.sketch.Sketch;

public interface Mural { 
	public Sketch get(byte[] key) throws IOException;
	public void close() throws IOException;
	public int compareTo(Mural other);
	public long getTimestamp();
}
