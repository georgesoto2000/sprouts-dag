# sprouts-dag

DAG to upload data from a psuedo Sports Events API to BigQuery table, download sales data from a GCP bucket, upload it to a BigQuery table, copy it to an archive bucket, delete it from the original bucket, and trigger a DBT pipeline.