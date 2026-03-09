
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-selcuk \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.41.0.jar  \
    gs://dtc-de-course-485207-terra-bucket/code/06_spark_sql_big_query.py \
    -- \
        --input_green='gs://dtc-de-course-485207-terra-bucket/pq/green/2020/*' \
        --input_yellow='gs://dtc-de-course-485207-terra-bucket/pq/yellow/2020/*' \
        --output='dtc-de-course-485207.module_6_spark.reports-2020'





