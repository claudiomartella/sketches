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


package org.acaro.sketches.sketch;

import java.nio.ByteBuffer;

import org.acaro.sketches.util.Util;

/**
 * 
 * @author Claudio Martella
 * 
 * this is the interface for operations on the DB. You can think of it as Operation. 
 * "a rapidly executed freehand drawing that is not intended as a finished work"
 *
 */

public interface Sketch {
	static final byte THROWUP = 1;
	static final byte BUFF = 2;
	static final int HEADER_SIZE = Util.SIZEOF_BYTE+Util.SIZEOF_LONG+Util.SIZEOF_SHORT+Util.SIZEOF_INT;
	
	public byte[] getKey();
	
	public long getTimestamp();
	
	public abstract ByteBuffer[] getBytes();
	
	public abstract int getSize();	
}
