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

package org.acaro.sketches.mindsketches;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.acaro.sketches.sketch.Sketch;

/**
 * This class represents the Memstore, where the written data is kept before being
 * being flushed to disk. It builds around a ConcurrentHashMap for the data and
 * an AtomInteger that counts the total amout of data passed through the store.
 * 
 * @author Claudio Martella
 *
 */

public class MindSketches {
	private ConcurrentHashMap<byte[], Sketch> map = new ConcurrentHashMap<byte[], Sketch>(100000);
	private AtomicInteger size = new AtomicInteger(0);
	
	public Sketch get(byte[] key) {
		return map.get(key);
	}
	
	public Sketch put(byte[] key, Sketch value) {
		size.addAndGet(value.getSize());
		return map.put(key, value);			
	}
	
	public int getSize() {
		return size.get();
	}
	
	public Map<byte[], Sketch> getMap() {
		return this.map;
	}
}
