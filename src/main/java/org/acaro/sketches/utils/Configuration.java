package org.acaro.sketches.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
	private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
	private static final String defaultConfig = "./conf/default.properties";
	private static final String userConfig    = "./conf/user.properties";
	private Properties userProps;

	private static class ConfigurationHolder { 
		public static final Configuration INSTANCE = new Configuration();
	}

	public static Configuration getConf() {
		return ConfigurationHolder.INSTANCE;
	}

	private Configuration() {
		Properties defaultProps = new Properties();
		FileInputStream in;

		// load defaults
		try { 
			in = new FileInputStream(defaultConfig);
			defaultProps.load(in);
			in.close();
		} catch (IOException e) {
			throw new RuntimeException("Cannot parse default properties file ", e);
		}

		userProps = new Properties(defaultProps);

		// add user overrides
		try {
			in = new FileInputStream(userConfig);
			userProps.load(in);
			in.close();
		} catch (IOException e) {
			logger.info("Cannot load user properties file, sticking with defaults: "
					+ e.getMessage());
		}
		
		// set OUR system properties overrides
		for (Entry<Object, Object> property: System.getProperties().entrySet()) {
			String key   = (String) property.getKey();
			String value = (String) property.getValue();
			
			if (key.startsWith("sketches."))
				userProps.setProperty(key, value);
		}
	}

	public String getString(String key, String defaultValue) {
		String value = get(key);

		if (value == null)
			return defaultValue;
		else
			return value;
	}

	public int getInt(String key, int defaultValue) {
		String value = get(key);

		if (value == null)
			return defaultValue;

		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			logger.info("NumberFormatException for property: " + key);
			return defaultValue;
		}
	}

	public double getDouble(String key, double defaultValue) {
		String value = get(key);

		if (value == null)
			return defaultValue;

		try {
			return Double.parseDouble(value);
		} catch (NumberFormatException e) {
			logger.info("NumberFormatException for property: " + key);
			return defaultValue;
		}
	}
	
	public float getFloat(String key, float defaultValue) {
		String value = get(key);

		if (value == null)
			return defaultValue;

		try {
			return Float.parseFloat(value);
		} catch (NumberFormatException e) {
			logger.info("NumberFormatException for property: " + key);
			return defaultValue;
		}
	}

	public boolean getBoolean(String key, boolean defaultValue) {
		String value = get(key);

		if ("true".equals(value))
			return true;
		else if ("false".equals(value))
			return false;
		else 
			return defaultValue;
	}

	private String get(String key) {
		return userProps.getProperty(key);
	}
}
