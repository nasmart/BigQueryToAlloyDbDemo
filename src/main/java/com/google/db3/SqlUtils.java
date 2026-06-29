package com.google.db3;

/** Utility class for SQL string escaping. */
public final class SqlUtils {
  private SqlUtils() {
    // Prevent instantiation
  }

  public static String escapeBigQuerySqlString(String value) {
    if (value == null) {
      return null;
    }
    return value.replace("\\", "\\\\").replace("'", "\\'");
  }

  public static String escapeBigQueryIdentifier(String value) {
    if (value == null) {
      return null;
    }
    return value.replace("\\", "\\\\").replace("`", "\\`");
  }

  public static String escapeDuckDbSqlString(String input) {
    if (input == null) {
      return null;
    }
    return input.replace("\\", "\\\\").replace("'", "''");
  }

  public static String escapeDuckDbIdentifier(String input) {
    if (input == null) {
      return null;
    }
    return "\"" + input.replace("\"", "\"\"") + "\"";
  }
}
