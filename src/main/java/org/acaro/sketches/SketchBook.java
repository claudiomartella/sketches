package org.acaro.sketches;

import java.io.Closeable;
import java.io.IOException;

public interface SketchBook extends Closeable {
	public void write(Sketch s) throws IOException;
}
