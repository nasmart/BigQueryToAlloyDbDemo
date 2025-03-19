BigQueryToAlloyDbDemo
=====================

Example of how to copy from a BigQuery table to AlloyDB via DuckDB using Java.

The Google BigQuery public dataset table
`bigquery-public-data.london_bicycle.cycle_hire` is written to Google Cloud
Storage (com.google.db3.BigQueryToGcs), then read from Google Cloud Storage and
written to AlloyDB via COPY using DuckDB (com.google.db3.GcsToAlloyDb).

Create the destination schema and table by running
`BigQueryToAlloyDbDemo/scripts/create_cycle_hire.sql` in your AlloyDB
destination database.

Authentication to BigQuery and Google Cloud Storage relies on [default
application
credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

Build the JAR using: `./gradlew build`

Set the following environment variables:

```
export GCS_BUCKET='<REPLACE WITH GCS BUCKET NAME>'
export GCS_PREFIX='<REPLACE WITH THE PATH WITHIN THE BUCKET>'
export ALLOYDB_IP='<REPLACE WITH ALLOYDB IP ADDRESS'
export ALLOYDB_PORT='<REPLACE WITH ALLOYDB PORT>'
export ALLOYDB_DATABASE='<REPLACE WITH ALLOYDB DATABASE NAME>'
export ALLOYDB_SCHEMA='<REPLACE WITH ALLOYDB SCHEMA>'
export ALLOYDB_USER='<REPLACE WITH ALLOYDB USER>'
export ALLOYDB_PASSWORD='<REPLACE WITH ALLOYDB PASSWORD>'
export GCS_KEY_ID='<REPLACE WITH GCS BUCKET KEY ID>'
export GCS_SECRET='<REPLACE WITH GCS BUCKET SECRET>'
```

It may be necessary to set `GOOGLE_CLOUD_PROJECT` to control the project used
by the BigQuery job.

Copy from the public BigQuery dataset to Google Cloud Storage using:

```
java -cp build/libs/BigQueryToAlloyDbDemo-1.0-SNAPSHOT.jar com.google.db3.BigQueryToGcs
```

Copy from Google Cloud Storage to AlloyDB using:

```
java -cp build/libs/BigQueryToAlloyDbDemo-1.0-SNAPSHOT.jar com.google.db3.GcsToAlloyDb <TOTAL WORKERS> <WORKER ID>
```

For example, use 4 workers to load from Google Cloud Storage to AlloyDB using:

```
for i in {0..3}; do
  java -cp build/libs/BigQueryToAlloyDbDemo-1.0-SNAPSHOT.jar com.google.db3.GcsToAlloyDb 4 ${i} &
done
```
