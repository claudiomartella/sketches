package org.acaro.sketches.sfile.index;

public interface Index {
	public Index force();
	public boolean hasRemaining();
	public long position();
	public Index position(long position);
	public long getOffset();
	public long getOffset(long position);
	public Index putOffset(long offset);
	public Index putOffset(long position, long offset);
	public void load();
}
