package org.acaro.sketches;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
