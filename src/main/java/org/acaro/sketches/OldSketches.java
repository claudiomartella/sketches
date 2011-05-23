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

package org.acaro.sketches;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.acaro.sketches.logfile.BufferedLogfile;
import org.acaro.sketches.logfile.Logfile;
import org.acaro.sketches.memstore.SketchReader;
import org.acaro.sketches.operation.Delete;
import org.acaro.sketches.operation.Operation;
import org.acaro.sketches.operation.Update;
import org.acaro.sketches.sfile.FSSFile;
import org.acaro.sketches.sfile.SFile;

import com.google.common.base.Preconditions;

/**
 * 
 * 
 * The class is the entry point to the KV store.
 * 
 * Important: Both the key and the value should be immutable. 
 * We are not going to change them, we expect you to do the same.

 * @author Claudio Martella
 *
 */

public class OldSketches {
	private SketchReader memory;
	private Logfile book;
	private List<SFile> murals = new ArrayList<SFile>();
	private String path;
	private String name;
	
	public OldSketches(String path, String name) throws IOException {
		this.path = path;
		this.name = name;
		initSketchBook(path, name);
		initMurals(path, name);
	}
	
	public void put(byte[] key, byte[] value) throws IOException {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(key.length <= Short.MAX_VALUE, "key length can not be bigger than %s", Short.MAX_VALUE);
		
		Update t = new Update(key, value);
		book.write(t);
		memory.put(key, t);
	}
	
	public byte[] get(byte[] key) throws IOException {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.length <= Short.MAX_VALUE, "key length can not be bigger than %s", Short.MAX_VALUE);
		
		Operation s;
		byte[] value = null;
		
		if ((s = doGet(key)) != null) { // it was here... 
			if (s instanceof Delete) { // but it was deleted
				value = null;
			} else if (s instanceof Update) { // and we found it
				value = ((Update) s).getValue();
			}
		}
		
		return value;
	}
	
	public byte[] getAndDelete(byte[] key) throws IOException {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.length <= Short.MAX_VALUE, "key length can not be bigger than %s", Short.MAX_VALUE);
		
		Operation s;
		byte[] value = null;
		
		// it's still around
		if ((s = doGet(key)) != null && !(s instanceof Delete)) {
			doDelete(key);
			value = ((Update) s).getValue();
		}
		
		return value;
	}
	
	public void delete(byte[] key) throws IOException {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.length <= Short.MAX_VALUE, "key length can not be bigger than %s", Short.MAX_VALUE);
		
		doDelete(key);
	}
	
	public void shutdown() throws IOException {
		book.close();
	}

	public void bomb() throws IOException {
		SketchReader oldMemory = memory;
		memory = new SketchReader();
		SketchesHelper.scribe(oldMemory, path+"/"+name+".br");
	}
	
	private void doDelete(byte[] key) throws IOException {
		Delete b = new Delete(key);
		book.write(b);
		memory.put(key, b);
	}
	
	private Operation doGet(byte[] key) throws IOException {
		Operation s;
		
		if ((s = memory.get(key)) == null) 
			s = muralsGet(key);
		
		return s;
	}
	
	private Operation muralsGet(byte[] key) throws IOException {
		Operation s = null;
		
		for (SFile w: murals)
			if ((s = w.get(key)) != null)
				break;
		
		return s;
	}
	
	private void initMurals(String path, String name) {
		//murals.add(new Mural());
	}

	private void initSketchBook(String path, String name) throws IOException {
		String filename = SketchesHelper.getFilename(path, name);
		this.memory     = SketchesHelper.loadSketchBook(filename);
		this.book       = new BufferedLogfile(filename);
	}
}
