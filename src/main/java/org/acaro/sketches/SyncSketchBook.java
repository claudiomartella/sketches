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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 
 * @author Claudio Martella
 * 
 * Append-only log file.
 *
 */

public class SyncSketchBook implements SketchBook {
	private FileOutputStream fos;
	private FileChannel ch;

	public SyncSketchBook(String filename) throws IOException {
		fos = new FileOutputStream(filename, true);
		ch  = fos.getChannel();
	}
	
	public synchronized void write(Sketch b) throws IOException {
		ByteBuffer[] bb = b.getBytes();
		while (ch.write(bb) > 0);
		ch.force(false);
	}
	
	public synchronized void close() throws IOException {
		fos.getFD().sync();
		fos.close();
	}
	
/*	private void writeFully(FileChannel ch, ByteBuffer[] bytes) throws IOException {
		long fullLength = calculateFullLength(bytes);
		long fullyWritten = 0;

		while (fullyWritten < fullLength) {
			long written = ch.write(bytes);
			if (written > 0) {
				fullyWritten += written;
			} else  {
				Thread.yield();
			}
		}
	}

	private long calculateFullLength(ByteBuffer[] bytes) {
        long fullLength = 0;
        
        for (int i = 0; i < bytes.length; i++)
        	fullLength += bytes[i].remaining();
        
        return fullLength;
	}*/
}
