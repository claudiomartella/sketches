package org.acaro.sketches.utils;

import java.util.ArrayList;
import java.util.Iterator;

import org.acaro.sketches.io.OperationReader;
import org.acaro.sketches.memstore.Memstore;

public class OperationReaders 
implements Iterable<OperationReader> {

	private Memstore memstore;
	private ArrayList<OperationReader> list = new ArrayList<OperationReader>();

	public OperationReaders() { }
	
	public OperationReaders(Memstore memstore) {
		setMemstore(memstore);
	}
	
	public Memstore setMemstore(Memstore memstore) {
		Memstore old  = this.memstore;
		this.memstore = memstore;
		
		return old;
	}
	
	public Memstore getMemstore() {
		return this.memstore;
	}
	
	public void add(OperationReader reader) {
		insertSorted(reader);
	}
	
	public void remove(OperationReader reader) {
		list.remove(reader);
	}
	
	public OperationReader[] toArray() {
		
		OperationReader[] array = new OperationReader[list.size() + 1];

		int i = 0;
		for (OperationReader reader: this)
			array[i++] = reader;
		
		return array;
	}
	
	public Iterator<OperationReader> iterator() {
		return new OperationReadersIterator();
	}

    private void insertSorted(OperationReader reader) {
    	
        list.add(reader); // add it to the tail and move it to the right place
        Comparable<OperationReader> cmp = (Comparable<OperationReader>) reader;
        for (int i = list.size() - 1; i > 0 && cmp.compareTo(list.get( i - 1)) < 0; i--) {
            OperationReader tmp = list.get(i);
            list.set(i, list.get(i - 1));
            list.set(i - 1, tmp);
        }
    }
	
	private class OperationReadersIterator
	implements Iterator<OperationReader> {

		private boolean first = true;
		private Iterator<OperationReader> i = list.iterator();
		
		public boolean hasNext() {

			if (first) {
				return true;
			} else { 
				return i.hasNext();
			}
		}

		public OperationReader next() {

			if (first) {
				first = false;
				return memstore;
			} else {
				return i.next();
			}
		}

		public void remove() {
			throw new UnsupportedOperationException("OperationReadersIterator doesn't support remove()");
		}
	}
}
