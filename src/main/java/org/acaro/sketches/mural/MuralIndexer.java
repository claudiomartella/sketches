package org.acaro.sketches.mural;

import java.io.File;
import java.util.concurrent.Callable;

public class MuralIndexer implements Callable<String> {
	private String muralFilename;

	public MuralIndexer(String muralFilename) {
		this.muralFilename = muralFilename;
	}
	
	public String call() throws Exception {
		TMuralIndexBuilder tIndexBuilder = new TMuralIndexBuilder(muralFilename);
		String tIndexFilename = tIndexBuilder.call();
		MuralIndexBuilder indexBuilder = new MuralIndexBuilder(muralFilename, tIndexFilename);
		indexBuilder.call();
		
		delete(tIndexFilename);
		
		return muralFilename;
	}

	private void delete(String tIndexFilename) {
		File f = new File(tIndexFilename);

		if (!f.exists())
			throw new IllegalArgumentException(
					"Delete: no such file or directory: " + tIndexFilename);

		boolean success = f.delete();

		if (!success)
			throw new IllegalArgumentException("Delete: deletion failed");
	}
}
