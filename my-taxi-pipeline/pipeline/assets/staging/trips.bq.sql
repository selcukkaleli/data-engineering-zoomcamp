/* @bruin

name: staging.trips
type: bq.sql
connection: "gcp-bruin"
depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

columns:
  - name: vendor_id
    type: integer
    description: "Taksi sağlayıcısının ID'si"
  - name: tpep_pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    checks:
      - name: non_negative

@bruin */
SELECT *
FROM ingestion.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
  AND fare_amount >= 0
  AND passenger_count > 0
