package org.acaro.sketches.mural;

public interface Index {
	public Index force();
	public boolean hasRemaining();
	public long position();
	public Index position(long position);
	public long getOffset();
	public long getOffset(long position);
	public Index putOffset(long offset);
	public Index putOffset(long position, long offset);
}
