package org.acaro.sketches.utils;

import java.io.File;

public class FSUtils {
	
	public static void delete(File f) {

		if (!f.exists())
			throw new IllegalArgumentException(
					"Delete: no such file or directory: " + f.getName());

		boolean success = f.delete();

		if (!success)
			throw new IllegalArgumentException("Delete: deletion failed: " + f.getAbsolutePath());
	}
}
