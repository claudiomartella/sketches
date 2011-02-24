package org.acaro.graffiti;

import java.io.IOException;
import java.util.UUID;

import org.acaro.sketches.Sketches;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	long start = System.currentTimeMillis();
    	Sketches data = new Sketches(".", "test");    	
    	data.shutdown();
    	long stop = System.currentTimeMillis();
    	
    	System.out.println("Time: " + (stop-start)/1000);
    }
}
