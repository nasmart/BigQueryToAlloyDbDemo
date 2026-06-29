package com.google.db3;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

  private static final Properties properties = new Properties();

  static {
    String configFile = System.getenv("CONFIG_FILE");
    if (configFile == null) {
      configFile = "config.properties";
    }

    try (InputStream input = new FileInputStream(configFile)) {
      properties.load(input);
    } catch (IOException ex) {
      // If a specific config file was requested and failed, log it.
      if (System.getenv("CONFIG_FILE") != null) {
        System.err.println("Warning: Unable to load config file " + configFile + ": " + ex.getMessage());
      }
      // If default config.properties doesn't exist, we just rely on environment variables.
    }
  }

  public static String get(String key) {
    String envValue = System.getenv(key);
    if (envValue != null) {
      return envValue;
    }
    return properties.getProperty(key);
  }
}
