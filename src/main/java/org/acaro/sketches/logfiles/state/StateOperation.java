package org.acaro.sketches.logfiles.state;

import org.acaro.sketches.io.Writable;

public interface StateOperation 
extends Writable {
	
	static final byte NEWLOG = 0;
	static final byte NEWSCRIBED_SFILE = 1;
	static final byte NEWCOMPACTED_SFILE = 2;
}
