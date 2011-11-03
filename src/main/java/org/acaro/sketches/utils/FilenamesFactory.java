package org.acaro.sketches.utils;

public class FilenamesFactory {
	
	public static final String SFILE_EXTENSION    = ".sfile";
	public static final String LOG_EXTENSION      = ".log";
	public static final String STATELOG_EXTENSION = ".slog";
	
	public static String getSFileName() {
		return getBasename() + SFILE_EXTENSION;
	}
	
	public static String getLogFilename() {
		return getBasename() + LOG_EXTENSION;
	}
	
	public static String getStateLogFilename() {
		return getBasename() + STATELOG_EXTENSION;
	}
	
	private static String getBasename() {
		return String.valueOf(System.currentTimeMillis());
	}
}
