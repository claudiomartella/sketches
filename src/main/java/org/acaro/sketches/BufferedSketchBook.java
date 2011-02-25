/*Copyright 2011 Claudio Martella

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


package org.acaro.sketches;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferedSketchBook implements SketchBook {
	private FileOutputStream fos;
	private BufferedOutputStream bos;
	private final int bufferSize = 64*1024;
	
	public BufferedSketchBook(String filename) throws IOException {
		fos = new FileOutputStream(filename, true);
		bos = new BufferedOutputStream(fos, bufferSize);
	}
	
	public synchronized void write(Sketch s) throws IOException {
		for (ByteBuffer buffer: s.getBytes()) {
			bos.write(buffer.array());
		}
	}

	public synchronized void close() throws IOException {
		bos.flush();
		fos.getFD().sync();
		bos.close();
	}
}
