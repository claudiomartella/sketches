package org.acaro.sketches.logfiles.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.acaro.sketches.io.Writable;

public class NewLogfile 
implements StateOperation {

	private String filename;

	private NewLogfile() { }
	
	public NewLogfile(String filename) {
		this.filename = filename;
	}
	
	@Override
	public void readFrom(DataInput in) 
	throws IOException {

		this.filename = in.readUTF();
	}

	@Override
	public void writeTo(DataOutput out) 
	throws IOException {
		
		out.writeByte(NEWLOG);
		out.writeUTF(filename);
	}
	
	public static NewLogfile read(DataInput in) 
	throws IOException {
		
		NewLogfile o = new NewLogfile();
		o.readFrom(in);
		
		return o;
	}
}
