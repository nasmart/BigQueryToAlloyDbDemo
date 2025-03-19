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

    String bucketName = System.getenv("GCS_BUCKET");
    String prefix = System.getenv("GCS_PREFIX");
    String gcsUri = String.format("gs://%s/%s", bucketName, prefix);
    String tableId = "cycle_hire";

    String alloyDbIp = System.getenv("ALLOYDB_IP");
    String alloyDbPortStr = System.getenv("ALLOYDB_PORT");
    String alloyDbDatabase = System.getenv("ALLOYDB_DATABASE");
    String alloyDbSchema = System.getenv("ALLOYDB_SCHEMA");
    String alloyDbUser = System.getenv("ALLOYDB_USER");
    String alloyDbPassword = System.getenv("ALLOYDB_PASSWORD");

    String gcsKeyId = System.getenv("GCS_KEY_ID");
    String gcsSecret = System.getenv("GCS_SECRET");

    int totalWorkers;
    int workerId;

    if (args.length != 2) {
      System.err.println("Error: Expected exactly two arguments: 1) Number of works, 2) My worker number.");
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
      statement.execute(
          String.format(
              "CREATE SECRET alloydb (TYPE postgres, HOST '%s', PORT %d, DATABASE %s, USER '%s', PASSWORD '%s')",
              alloyDbIp, alloyDbPort, alloyDbDatabase, alloyDbUser, alloyDbPassword));
      System.out.println("AlloyDB secret created.");

      // Create a Google Cloud Storage secret
      statement.execute(
          String.format(
              "CREATE SECRET gcs (TYPE gcs, KEY_ID '%s', SECRET '%s')", gcsKeyId, gcsSecret));
      System.out.println("Google Cloud Storage secret created.");

      // Connect to AlloyDB
      statement.execute("ATTACH '' AS alloydb (TYPE postgres, SECRET alloydb);");
      System.out.println("Connected to AlloyDB.");

      int totalFiles = countFiles(bucketName, prefix + "/");

      // Copy for Google Cloud Storage to AlloyDB
      for (int i = 0; i <= totalFiles - 1; i = i + 1) {
        if (i % totalWorkers == workerId) {
          String file = String.format("%s/%012d.parquet", gcsUri, i);
          statement.execute(
              String.format(
                  "COPY alloydb.%s.%s FROM '%s' (FORMAT parquet)", alloyDbSchema, tableId, file));
          System.out.println(String.format("Loaded %s to AlloyDB.", file));
        }
      }

    } catch (SQLException e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    } 
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
