package com.google.db3;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** A utility that exports a BigQuery table to Google Cloud Storage in Parquet format. */
public class BigQueryToGcs {

  public static void main(String[] args) throws InterruptedException {

    String bucketName = System.getenv("GCS_BUCKET");
    String prefix = System.getenv("GCS_PREFIX");
    String gcsUri = String.format("gs://%s/%s", bucketName, prefix);
    String datasetProjectId = "bigquery-public-data";
    String datasetId = "london_bicycles";
    String tableId = "cycle_hire";

    exportDataToGCS(datasetProjectId, gcsUri, datasetId, tableId);
  }

  private static void exportDataToGCS(
      String datasetProjectId, String gcsUri, String datasetId, String tableId)
      throws InterruptedException {

    try {
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      String query =
          String.format(
              "EXPORT DATA OPTIONS("
                  + "  uri='%s/*.parquet',"
                  + "  format='PARQUET',"
                  + "  overwrite=true,"
                  + "  compression='SNAPPY'"
                  + ") AS SELECT "
                  + "     rental_id, "
                  + "     duration, "
                  + "     duration_ms, "
                  + "     bike_id, "
                  + "     bike_model, "
                  + "     end_date, "
                  + "     end_station_id, "
                  + "     end_station_name, "
                  + "     start_date, "
                  + "     start_station_id, "
                  + "     start_station_name, "
                  + "     end_station_logical_terminal, "
                  + "     start_station_logical_terminal, "
                  + "     end_station_priority_id"
                  + "  FROM %s.%s.%s",
              gcsUri, datasetProjectId, datasetId, tableId);

      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

      JobId jobId = JobId.of(UUID.randomUUID().toString());
      Job job = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

      job = job.waitFor();

      if (job == null) {
        BigQueryError error = new BigQueryError("404", "notFound", "Job no longer exists");
        List<BigQueryError> errorList = Collections.singletonList(error);
        throw new BigQueryException(errorList);
      } else if (job.getStatus().getError() != null) {
        throw new BigQueryException(1, "BigQuery error", job.getStatus().getError());
      }

      System.out.println("BigQuery export completed successfully.");

    } catch (BigQueryException e) {
      System.err.println("BigQuery export failed: " + e.toString());
    }
  }
}
