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


package org.acaro.sketches.logfiles;

import java.io.Closeable;
import java.io.IOException;

import org.acaro.sketches.io.Writable;
import org.acaro.sketches.utils.Sizes;

/**
 * @author Claudio Martella
 * 
 * This is the Log. 
 * 
 * 
 * Format:
 * +----------+------+   +------+
 * |     1    |   N  |   |   N  |
 * |Dirty Byte|Sketch|...|Sketch|
 * +----------+------+   +------+ 
 * 
 */


public interface Logfile 
extends Closeable {

	public static final byte CLEAN = 0;
	public static final byte DIRTY = 1;
	public static final int HEADER_SIZE = Sizes.SIZEOF_BYTE;
	
	public Logfile write(Writable o) throws IOException;
	public Logfile flush() throws IOException;
	public Logfile sync() throws IOException;
	public String getName();
}
