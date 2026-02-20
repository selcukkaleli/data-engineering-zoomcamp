/* @bruin
name: reports.trips_report
type: bq.sql
connection: "gcp-bruin"
depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp
@bruin */
SELECT * -- TODO: replace with your aggregation logic
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
  AND fare_amount >= 0
  AND passenger_count > 0

