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

package org.acaro.sketches.operation;

import org.acaro.sketches.io.Writable;
import org.acaro.sketches.utils.Sizes;

/**
 * 
 * @author Claudio Martella
 * 
 * Interface for operations on the DB. 
 *
 */

public interface Operation 
extends Writable {
	
	public static final byte UPDATE = 1;
	public static final byte DELETE = 2;
	
	public byte[] getKey();
	
	public byte[] getValue();
	
	public long getTimestamp();
	
	public int getSize();	
}
