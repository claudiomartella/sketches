package org.acaro.graffiti.sketch;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * 
 * @author Claudio Martella
 * 
 * The class is the entry point to the KV store.
 * 
 * Important: Both the key and the value should be immutable. 
 * We are not going to change them, we expect you to do the same.
 *
 */

public class Sketches {
	private HashMap<byte[], Sketch> memory = new HashMap<byte[], Sketch>();
	private SketchBook book;
	private int bookSize = 0;
	private List<Wall> walls;
	private String path;
	private String name;
	
	public Sketches(String path, String name) {
		this.path = path;
		this.name = name;
		initSketchBook(path, name);
		initWalls(path, name);
	}
	
	public void put(byte[] key, byte[] value) throws IOException {
		if(key.length > Short.MAX_VALUE) 
			throw new IllegalArgumentException("key length can not be bigger than "+ Short.MAX_VALUE);

		Throwup t = new Throwup(key, value);
		book.write(t);
		memory.put(key, t);
		bookSize += t.getSize();
	}
	
	public byte[] get(byte[] key) {
		if(key.length > Short.MAX_VALUE) 
			throw new IllegalArgumentException("key length can not be bigger than "+ Short.MAX_VALUE);
		
		Sketch s;
		byte[] value = null;
		
		if((s = doGet(key)) != null){ // it was here... 
			if (s instanceof Buff) { // but it was deleted
				value = null;
			} else if (s instanceof Throwup) { // and we found it
				value = ((Throwup) s).getValue().array();
			}
		}
		
		return value;
	}
	
	// XXX: what about just writing the Buff and avoid hitting the Walls?
	public byte[] delete(byte[] key) throws IOException {
		if(key.length > Short.MAX_VALUE) 
			throw new IllegalArgumentException("key length can not be bigger than "+ Short.MAX_VALUE);

		Sketch s;
		byte[] value = null;
		
		// it's still around
		if ((s = doGet(key)) != null && !(s instanceof Buff)) {
			Buff b = new Buff(key);
			book.write(b);
			memory.put(key, b);
			value = ((Throwup) s).getValue().array();
		}
		
		return value;
	}
	
	private Sketch doGet(byte[] key){
		Sketch s;
		
		if((s = memory.get(key)) == null) {
			s = wallsGet(key);
		}
		
		return s;
	}
	
	private Sketch wallsGet(byte[] key) {
		Sketch s = null;
		
		for (Wall w: walls) {
			if ((s = w.get(key)) != null) {
				break;
			}
		}
		
		return s;
	}
}
