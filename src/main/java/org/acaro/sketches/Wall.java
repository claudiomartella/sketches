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


package org.acaro.sketches;

public class Wall {
	//private BookIndex index;
	static final public int HEADER_SIZE = Util.SIZEOF_BYTE+Util.SIZEOF_LONG;
	static final public byte CLEAN = 1;
	static final public byte DIRTY = 2;
	
	public Sketch get(byte[] key) {
		Sketch s = null;
		
		//long offset = index.get(key);
		//if (offset > 0) {
		//	s = getData(offset);
		//}
		
		return s;
	}
}
