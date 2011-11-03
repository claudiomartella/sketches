package org.acaro.sketches.logfiles.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NewScribedSFile 
implements StateOperation {

	private String filename;
	private String logfilename;

	private NewScribedSFile() { }
	
	public NewScribedSFile(String filename, String logfilename) {
		this.filename    = filename;
		this.logfilename = logfilename;
	}
	
	@Override
	public void readFrom(DataInput in) 
	throws IOException {
		
		this.filename    = in.readUTF();
		this.logfilename = in.readUTF();
	}

	@Override
	public void writeTo(DataOutput out) 
	throws IOException {
		
		out.writeByte(NEWSCRIBED_SFILE);
		out.writeUTF(filename);
		out.writeUTF(logfilename);
	}
	
	public static NewScribedSFile read(DataInput in)
	throws IOException {
		
		NewScribedSFile o = new NewScribedSFile();
		o.readFrom(in);
		
		return o;
	}
}
