package org.acaro.sketches.logfiles.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class NewCompactedSFile 
implements StateOperation {

	private String younger;
	private String older;
	private String filename;
	private boolean major;

	private NewCompactedSFile() { }

	public NewCompactedSFile(String younger, String older, String filename, boolean major) {
		this.younger  = younger;
		this.older    = older;
		this.filename = filename;
		this.major    = major;
	}
	
	@Override
	public void readFrom(DataInput in) 
	throws IOException {

		this.younger  = in.readUTF();
		this.older    = in.readUTF();
		this.filename = in.readUTF();
		this.major    = in.readBoolean();
	}

	@Override
	public void writeTo(DataOutput out) 
	throws IOException {

		out.writeByte(NEWCOMPACTED_SFILE);
		out.writeUTF(younger);
		out.writeUTF(older);
		out.writeUTF(filename);
		out.writeBoolean(major);
	}
	
	public static NewCompactedSFile read(DataInput in) 
	throws IOException {
		
		NewCompactedSFile o = new NewCompactedSFile();
		o.readFrom(in);
		
		return o;
	}
}
