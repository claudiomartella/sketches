package org.acaro.graffiti;

import java.io.IOException;
import java.util.UUID;

import org.acaro.sketches.Sketches;

/**
 * Hello world!
 *
 */
public class App2 
{
    public static void main( String[] args ) throws IOException
    {
    	Sketches data = new Sketches(".", "test");
    	
    	long start = System.currentTimeMillis();
    	for (int i = 0; i < 1000000; i++) {
    		byte[] key = UUID.randomUUID().toString().getBytes();
    		byte[] value = UUID.randomUUID().toString().getBytes();
    	
    		data.put(key, value);
    	}
    	data.shutdown();
    	long stop = System.currentTimeMillis();
    	
    	System.out.println("Time: " + (stop-start)/1000);
    }
}
