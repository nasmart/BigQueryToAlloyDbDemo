package com.google.db3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/** Copy Parquet files from Google Cloud Storage to AlloyDB. */
public class GcsToAlloyDb {

  public static void main(String[] args) throws InterruptedException {

    String bucketName = Config.get("GCS_BUCKET");
    String prefix = Config.get("GCS_PREFIX");
    String gcsUri = String.format("gs://%s/%s", bucketName, prefix);

    String alloyDbIp = Config.get("ALLOYDB_IP");
    String alloyDbPortStr = Config.get("ALLOYDB_PORT");
    String alloyDbDatabase = Config.get("ALLOYDB_DATABASE");
    String alloyDbSchema = Config.get("ALLOYDB_SCHEMA");
    String alloyDbTableId = Config.get("ALLOYDB_TABLE");
    String alloyDbUser = Config.get("ALLOYDB_USER");
    String alloyDbPassword = Config.get("ALLOYDB_PASSWORD");

    String gcsKeyId = Config.get("GCS_KEY_ID");
    String gcsSecret = Config.get("GCS_SECRET");

    int totalWorkers;
    int workerId;

    if (args.length != 2) {
      System.err.println("Error: Expected exactly two arguments: 1) Number of workers, 2) My worker number.");
      return;
    }

    try {
      totalWorkers = Integer.parseInt(args[0]);
      workerId = Integer.parseInt(args[1]);
    } catch (NumberFormatException e) {

      System.err.println("Error parsing arguments: " + e.getMessage());
      return;
    }

    int alloyDbPort;
    try {
      alloyDbPort = Integer.parseInt(alloyDbPortStr);
    } catch (NumberFormatException e) {
      System.err.println("Error parsing ALLOYDB_PORT: " + e.getMessage());
      return;
    }

    String jdbcUrl = "jdbc:duckdb:";

    try (Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement()) {

      System.out.println("Connected to DuckDB in-memory database.");

      // Load Postgres extension
      statement.execute("INSTALL postgres;");
      statement.execute("LOAD postgres;");
      System.out.println("Postgres extension installed and loaded.");

      // Create a postgres secret
      // Note: DuckDB does not support PreparedStatements parameters for DDL statements (like CREATE SECRET).
      // We rely on explicit string escaping and concatenation to prevent SQL injection vulnerabilities.
      String createAlloyDbSecretSql = "CREATE SECRET alloydb (TYPE postgres, HOST '" + SqlUtils.escapeDuckDbSqlString(alloyDbIp)
          + "', PORT " + alloyDbPort
          + ", DATABASE '" + SqlUtils.escapeDuckDbSqlString(alloyDbDatabase)
          + "', USER '" + SqlUtils.escapeDuckDbSqlString(alloyDbUser)
          + "', PASSWORD '" + SqlUtils.escapeDuckDbSqlString(alloyDbPassword)
          + "')";
      statement.execute(createAlloyDbSecretSql);
      System.out.println("AlloyDB secret created.");

      // Create a Google Cloud Storage secret
      // Note: As above, PreparedStatement parameters cannot be used here due to DuckDB limitations.
      String createGcsSecretSql = "CREATE SECRET gcs (TYPE gcs, KEY_ID '" + SqlUtils.escapeDuckDbSqlString(gcsKeyId) + "', SECRET '" + SqlUtils.escapeDuckDbSqlString(gcsSecret) + "')";
      statement.execute(createGcsSecretSql);
      System.out.println("Google Cloud Storage secret created.");

      // Connect to AlloyDB
      statement.execute("ATTACH '' AS alloydb (TYPE postgres, SECRET alloydb);");
      System.out.println("Connected to AlloyDB.");

      String filePattern = String.format("%s/*.parquet", gcsUri);

      // Copy for Google Cloud Storage to AlloyDB
      statement.execute(
          String.format(
              "INSERT INTO alloydb.\"%s\".\"%s\" SELECT * EXCLUDE (filename) FROM read_parquet('%s', filename=true) WHERE try_cast(regexp_extract(filename, '([0-9]+)\\.parquet$', 1) AS INT) %% %d = %d",
              SqlUtils.escapeDuckDbIdentifier(alloyDbSchema),
              SqlUtils.escapeDuckDbIdentifier(alloyDbTableId),
              SqlUtils.escapeDuckDbSqlString(filePattern),
              totalWorkers,
              workerId));
      System.out.println(String.format("Loaded files matching pattern %s for worker %d to AlloyDB.", filePattern, workerId));

    } catch (SQLException e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    } 
  }
}
