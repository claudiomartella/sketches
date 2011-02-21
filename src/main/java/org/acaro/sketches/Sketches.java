package org.acaro.sketches;

import java.io.IOException;
import java.util.ArrayList;
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
	private MindSketches memory;
	private SketchBook book;
	private List<Wall> walls = new ArrayList<Wall>();
	private String path;
	private String name;
	
	public Sketches(String path, String name) throws IOException {
		this.path = path;
		this.name = name;
		initSketchBook(path, name);
		initWalls(path, name);
	}
	
	public void put(byte[] key, byte[] value) throws IOException {
		checkKey(key);
		checkValue(value);
		
		Throwup t = new Throwup(key, value);
		book.write(t);
		memory.put(key, t);
	}
	
	public byte[] get(byte[] key) {
		checkKey(key);
		
		Sketch s;
		byte[] value = null;
		
		if ((s = doGet(key)) != null) { // it was here... 
			if (s instanceof Buff) { // but it was deleted
				value = null;
			} else if (s instanceof Throwup) { // and we found it
				value = ((Throwup) s).getValue().array();
			}
		}
		
		return value;
	}
	
	public byte[] getAndDelete(byte[] key) throws IOException {
		checkKey(key);
		
		Sketch s;
		byte[] value = null;
		
		// it's still around
		if ((s = doGet(key)) != null && !(s instanceof Buff)) {
			doDelete(key);
			value = ((Throwup) s).getValue().array();
		}
		
		return value;
	}
	
	public void delete(byte[] key) throws IOException {
		checkKey(key);
		
		doDelete(key);
	}
	
	public void shutdown() throws IOException {
		book.close();
	}
	
	private void doDelete(byte[] key) throws IOException {
		Buff b = new Buff(key);
		book.write(b);
		memory.put(key, b);
	}
	
	private Sketch doGet(byte[] key) {
		Sketch s;
		
		if ((s = memory.get(key)) == null) {
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
	
	private void checkKey(byte[] key) {
		if (key == null)
			throw new IllegalArgumentException("key argument is null");
		if (key.length > Short.MAX_VALUE) 
			throw new IllegalArgumentException("key length can not be bigger than "+ Short.MAX_VALUE);
	}
	
	private void checkValue(byte[] value) {
		if (value == null)
			throw new IllegalArgumentException("value argument is null");
	}
	
	private void initWalls(String path, String name) {
		walls.add(new Wall());
	}

	private void initSketchBook(String path, String name) throws IOException {
		String filename = SketchesHelper.getFilename(path, name);
		this.memory = SketchesHelper.loadSketchBook(filename);
		this.book   = new SketchBook(filename);
	}
}