/* Copyright 2011 Claudio Martella

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

package org.acaro.sketches.mural;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.acaro.sketches.sketch.Sketch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuralWriter implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(MuralWriter.class);
	private FileOutputStream file;
	private BufferedOutputStream bos;
	private long timestamp = 0;
	private int totalItems = 0;
	
	public MuralWriter(String filename) throws IOException {
		this.file = new FileOutputStream(filename);
		this.bos  = new BufferedOutputStream(file, 65536);
		init();
	}
	
	public void write(Sketch sketch) throws IOException {
		
		for (ByteBuffer buffer: sketch.getBytes()) {
			bos.write(buffer.array());
		}
		updateTimestamp(sketch);
		totalItems++;
	}

	public void close() throws IOException {
		logger.debug("Closing mural with ts: " + timestamp + " total: " + totalItems);

		bos.flush();
		ByteBuffer header = ByteBuffer.allocate(Mural.HEADER_SIZE);
		header.put(Mural.CLEAN);
		header.putLong(timestamp);
		header.putInt(totalItems);
		header.putLong(0);
		header.rewind();
		writeHeader(header);
		sync();
		bos.close();
	}

	private void init() throws IOException {
		ByteBuffer header = ByteBuffer.allocate(Mural.HEADER_SIZE);
		header.put(Mural.DIRTY);
		header.putLong(0);
		header.putInt(0);
		header.putLong(0);
		header.rewind();
		writeHeader(header);
		sync();
	}
	
	private void writeHeader(ByteBuffer header) throws IOException {
		FileChannel channel = file.getChannel();
		channel.position(0);
		while (channel.write(header) > 0);
		System.out.println(channel.position());
	}

	private void sync() throws IOException {
		file.getFD().sync();
	}
	
	private void updateTimestamp(Sketch sketch) {
		if (sketch.getTimestamp() > timestamp)
			timestamp = sketch.getTimestamp();
	}
}
