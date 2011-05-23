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

package org.acaro.sketches.sfile;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.acaro.sketches.io.SmartWriter;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSSFileWriter implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(FSSFileWriter.class);
	private SmartWriter writer;
	private long timestamp     = 0;
	private long numberOfItems = 0;
	private float loadFactor   = 0;
	
	public FSSFileWriter(String filename) throws IOException {
		this.writer     = new SmartWriter(new RandomAccessFile(filename, "rw").getChannel());
		this.loadFactor = Configuration.getConf().getFloat("sketches.sfile.loadfactor", 1.0f);
		init();
	}
	
	public void write(Operation o) throws IOException {
		
		for (ByteBuffer buffer: o.getBytes())
			writer.write(buffer.array());

		updateTimestamp(o);
		numberOfItems++;
	}

	public void close() throws IOException {
		logger.debug("Closing mural with ts: " + timestamp + " total: " + numberOfItems);

		writer.flush();
		writer.getChannel().position(0);
		writer.writeByte(FSSFile.CLEAN);
		writer.writeLong(timestamp);
		writer.writeLong(numberOfItems);
		writer.writeFloat(loadFactor);
		writer.close();
	}

	private void init() throws IOException {
		writer.writeByte(FSSFile.DIRTY);
		writer.writeLong(0);
		writer.writeLong(0);
		writer.writeFloat(0);
		writer.writeLong(0);
		writer.writeLong(0);
		writer.sync();
	}
	
	private void updateTimestamp(Operation o) {
		if (o.getTimestamp() > timestamp)
			timestamp = o.getTimestamp();
	}
}
