package com.google.db3;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
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
      String createAlloyDbSecretSql = "CREATE SECRET alloydb (TYPE postgres, HOST '" + escapeSql(alloyDbIp)
          + "', PORT " + alloyDbPort
          + ", DATABASE '" + escapeSql(alloyDbDatabase)
          + "', USER '" + escapeSql(alloyDbUser)
          + "', PASSWORD '" + escapeSql(alloyDbPassword)
          + "')";
      statement.execute(createAlloyDbSecretSql);
      System.out.println("AlloyDB secret created.");

      // Create a Google Cloud Storage secret
      // Note: As above, PreparedStatement parameters cannot be used here due to DuckDB limitations.
      String createGcsSecretSql = "CREATE SECRET gcs (TYPE gcs, KEY_ID '" + escapeSql(gcsKeyId) + "', SECRET '" + escapeSql(gcsSecret) + "')";
      statement.execute(createGcsSecretSql);
      System.out.println("Google Cloud Storage secret created.");

      // Connect to AlloyDB
      statement.execute("ATTACH '' AS alloydb (TYPE postgres, SECRET alloydb);");
      System.out.println("Connected to AlloyDB.");

      int totalFiles = countFiles(bucketName, prefix + "/");

      // Copy for Google Cloud Storage to AlloyDB
      for (int i = 0; i < totalFiles; i++) {
        if (i % totalWorkers == workerId) {
          String file = String.format("%s/%012d.parquet", gcsUri, i);
          statement.execute(
              String.format(
                  "COPY alloydb.%s.%s FROM '%s' (FORMAT parquet)", alloyDbSchema, alloyDbTableId, file));
          System.out.println(String.format("Loaded %s to AlloyDB.", file));
        }
      }

    } catch (SQLException e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    } 
  }

  private static String escapeSql(String input) {
    if (input == null) {
      return null;
    }
    return input.replace("'", "''");
  }

  public static int countFiles(String bucketName, String prefix) {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    int fileCount = 0;

    try {

      Page<Blob> blobs = storage.list(
          bucketName,
          Storage.BlobListOption.prefix(prefix),
          Storage.BlobListOption.currentDirectory());

      for (Blob blob : blobs.iterateAll()) {
        if (!blob.isDirectory()) {
          fileCount++;
        }
      }
    } catch (StorageException e) {
      System.err.println("Error counting files: " + e.getMessage());
      return -1;
    }

    System.out.println("Found " + fileCount + " files in " + bucketName + "/" + prefix);

    return fileCount;
  }
}
