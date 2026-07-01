package com.google.db3;

public class SqlUtils {

    public static String escapeDuckDbSqlString(String input) {
        if (input == null) {
            return null;
        }
        return input.replace("'", "''");
    }

    public static String escapeDuckDbIdentifier(String input) {
        if (input == null) {
            return null;
        }
        return input.replace("\"", "\"\"");
    }

    public static String escapeBigQuerySqlString(String input) {
        if (input == null) {
            return null;
        }
        return input.replace("\\", "\\\\").replace("'", "''");
    }

    public static String escapeBigQueryIdentifier(String input) {
        if (input == null) {
            return null;
        }
        return input.replace("\\", "\\\\").replace("`", "\\`");
    }
}
