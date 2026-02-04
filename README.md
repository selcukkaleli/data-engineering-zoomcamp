# Data Engineering Zoomcamp

Workshop Codespaces - Module 1: Docker & SQL  
Module 2: Workflow Orchestration with Kestra  
Module 3: Data Warehousing & BigQuery

## Repository Structure

| File/Folder | Description |
|------|-------------|
| `Dockerfile` | Docker configuration for the pipeline |
| `docker-compose.yaml` | Docker Compose setup for PostgreSQL and pgAdmin |
| `ingest_data.py` | Python script for ingesting taxi data into PostgreSQL |
| `pipeline.py` | Data pipeline script |
| `main.py` | Main application entry point |
| `main.tf` | Terraform main configuration file |
| `variables.tf` | Terraform variables definition |
| `green_tripdata_2025-11.parquet` | NYC Green Taxi trip data for November 2025 |
| `taxi_zone_lookup.csv` | Taxi zone reference data |
| `module_2/hw2/` | Module 2 homework - Kestra workflow files |

---

## Module 1: Docker & SQL

### HW1 Solutions

#### Question 3. Counting short trips
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?
```sql
SELECT COUNT(*)
FROM public.green_tripdata_2025_11
WHERE trip_distance <= 1 
  AND lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01';
```

---

#### Question 4. Longest trip for each day
Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles.
```sql
SELECT
  CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
  MAX(trip_distance) AS max_trip_distance
FROM public.green_tripdata_2025_11
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY max_trip_distance DESC;
```

---

#### Question 5. Biggest pickup zone
Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?
```sql
SELECT 
  CAST(lpep_pickup_datetime AS DATE) as pickup_date,
  SUM(total_amount) AS total_amount,
  CONCAT(zpu."Borough", ' | ', zpu."Zone") AS pickup_loc,
  CONCAT(zdo."Borough", ' | ', zdo."Zone") AS dropoff_loc
FROM public.green_tripdata_2025_11 t
JOIN
  public.taxi_zone_lookup zpu
  ON t."PULocationID" = zpu."LocationID"
JOIN
  public.taxi_zone_lookup zdo
  ON t."DOLocationID" = zdo."LocationID"
GROUP BY
  pickup_date,
  pickup_loc,
  dropoff_loc
ORDER BY total_amount DESC;
```

---

#### Question 6. Largest tip
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?
```sql
SELECT 
  CAST(lpep_pickup_datetime AS DATE) as pickup_date,
  SUM(total_amount) AS total_amount,
  SUM(tip_amount) AS total_tip_amount,
  CONCAT(zpu."Borough", ' | ', zpu."Zone") AS pickup_loc,
  CONCAT(zdo."Borough", ' | ', zdo."Zone") AS dropoff_loc
FROM public.green_tripdata_2025_11 t
JOIN
  public.taxi_zone_lookup zpu 
  ON t."PULocationID" = zpu."LocationID"
JOIN
  public.taxi_zone_lookup zdo
  ON t."DOLocationID" = zdo."LocationID"
WHERE
  CONCAT(zpu."Borough", ' | ', zpu."Zone") = 'Manhattan | East Harlem North' AND
  CAST(lpep_pickup_datetime AS DATE) between '2025-11-01' and '2025-12-01'
GROUP BY
  pickup_date,
  pickup_loc,
  dropoff_loc
ORDER BY total_tip_amount DESC;
```

---

## Module 2: Workflow Orchestration with Kestra

### Assignment Overview
Extended existing Kestra flows to process NYC taxi data (Yellow and Green) for 2021. The workflow files are located in `module_2/hw2/`:

- `04_postgres_taxi.yaml` - Basic PostgreSQL ingestion flow
- `05_postgres_taxi_scheduled.yaml` - Scheduled PostgreSQL ingestion
- `08_gcp_taxi.yaml` - GCP BigQuery ingestion flow
- `09_gcp_taxi_scheduled.yaml` - Scheduled GCP BigQuery ingestion with backfill capability

### HW2 Solutions

#### Question 1. Yellow Taxi December 2020 File Size
**Question:** Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the extract task)?

**Answer:** 134.5 MiB

---

#### Question 2. Variable Rendering
**Question:** What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?

**Answer:** `green_tripdata_2020-04.csv`

**Explanation:** The file variable is configured as:
```yaml
variables:
  file: "{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv"
```
When executed with the specified inputs, it renders to `green_tripdata_2020-04.csv`.

---

#### Question 3. Yellow Taxi 2020 Total Rows
**Question:** How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

**Answer:** 24,648,499 (found: 24,648,235)

**SQL Query:**
```sql
SELECT COUNT(*) 
FROM `dtc-de-course-485207.zoomcamp_module2.yellow_tripdata` 
WHERE tpep_pickup_datetime >= TIMESTAMP("2020-01-01") 
  AND tpep_pickup_datetime < TIMESTAMP("2021-01-01");
```

---

#### Question 4. Green Taxi 2020 Total Rows
**Question:** How many rows are there for the Green Taxi data for all CSV files in the year 2020?

**Answer:** 1,734,051 (found: 1,733,987)

**SQL Query:**
```sql
SELECT COUNT(*) 
FROM `dtc-de-course-485207.zoomcamp_module2.green_tripdata` 
WHERE lpep_pickup_datetime >= TIMESTAMP("2020-01-01") 
  AND lpep_dropoff_datetime < TIMESTAMP("2021-01-01");
```

---

#### Question 5. Yellow Taxi March 2021 Rows
**Question:** How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

**Answer:** 1,925,152

**SQL Query:**
```sql
SELECT COUNT(*) 
FROM `dtc-de-course-485207.zoomcamp_module2.yellow_tripdata` 
WHERE filename = 'yellow_tripdata_2021-03.csv';
```

---

#### Question 6. Timezone Configuration
**Question:** How would you configure the timezone to New York in a Schedule trigger?

**Answer:** Add a `timezone` property set to `America/New_York` in the Schedule trigger configuration

**Explanation:** Kestra uses standard IANA timezone identifiers. The correct format for New York is `America/New_York`, not `EST` or `UTC-5`.

---

## Key Learnings from Module 2

✅ Orchestrated data pipelines with Kestra flows  
✅ Used variables and expressions for dynamic workflows  
✅ Implemented backfill for historical data (2021 taxi data)  
✅ Scheduled workflows with timezone support  
✅ Processed NYC taxi data (Yellow & Green) for 2019-2021  
✅ Built ETL pipelines that extract, transform, and load taxi trip data automatically

---

## Module 3: Data Warehousing & BigQuery

### Assignment Overview
Worked with BigQuery and Google Cloud Storage to analyze NYC Yellow Taxi data from January-June 2024. Created external tables, materialized tables, and explored partitioning and clustering strategies for query optimization.

### HW3 Solutions

#### Setup: Creating Tables

**External Table:**
```sql
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-485207.zoomcamp_module3_hw3.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-de-course-485207-terra-bucket/yellow_tripdata_2024-0*.parquet']
);
```

**Non-Partitioned Materialized Table:**
```sql
CREATE OR REPLACE TABLE dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_non_partitioned AS
SELECT * FROM `dtc-de-course-485207.zoomcamp_module3_hw3.external_yellow_tripdata`;
```

---

#### Question 1. Counting Records
**Question:** What is count of records for the 2024 Yellow Taxi Data?

**Answer:** 20,332,093

**Explanation:** Found in the "Details" page of the BigQuery table.

---

#### Question 2. Data Read Estimation
**Question:** What is the estimated amount of data that will be read when counting distinct PULocationIDs on the External Table vs the Materialized Table?

**Answer:** 0 MB for the External Table and 155.12 MB for the Materialized Table

**SQL Queries:**
```sql
SELECT COUNT(DISTINCT `PULocationID`) 
FROM dtc-de-course-485207.zoomcamp_module3_hw3.external_yellow_tripdata;

SELECT COUNT(DISTINCT `PULocationID`) 
FROM dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_non_partitioned;
```

---

#### Question 3. Understanding Columnar Storage
**Question:** Why are the estimated number of bytes different when querying one column vs two columns?

**Answer:** BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

**SQL Queries:**
```sql
SELECT PULocationID 
FROM dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_non_partitioned;

SELECT PULocationID, DOLocationID 
FROM dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_non_partitioned;
```

---

#### Question 4. Counting Zero Fare Trips
**Question:** How many records have a fare_amount of 0?

**Answer:** 8,333

**SQL Query:**
```sql
SELECT COUNT(*)
FROM dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;
```

---

#### Question 5. Partitioning and Clustering Strategy
**Question:** What is the best strategy to make an optimized table in BigQuery if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID?

**Answer:** Partition by tpep_dropoff_datetime and Cluster on VendorID

**SQL Query:**
```sql
CREATE OR REPLACE TABLE dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dtc-de-course-485207.zoomcamp_module3_hw3.external_yellow_tripdata;
```

---

#### Question 6. Partition Benefits
**Question:** What are the estimated bytes processed when retrieving distinct VendorIDs between 2024-03-01 and 2024-03-15 for non-partitioned vs partitioned tables?

**Answer:** 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

**SQL Queries:**
```sql
-- Non-partitioned table
SELECT DISTINCT(VendorID) 
FROM `dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime > "2024-03-01" 
  AND tpep_dropoff_datetime < "2024-03-15";

-- Partitioned and clustered table
SELECT DISTINCT(VendorID) 
FROM `dtc-de-course-485207.zoomcamp_module3_hw3.yellow_tripdata_partitioned_clustered`
WHERE tpep_dropoff_datetime > "2024-03-01" 
  AND tpep_dropoff_datetime < "2024-03-15";
```

**Key Insight:** Partitioning reduced the estimated bytes processed by approximately 91% (from 310.24 MB to 26.84 MB).

---

#### Question 7. External Table Storage
**Question:** Where is the data stored in the External Table you created?

**Answer:** GCS/GCP Bucket

**Explanation:** External tables in BigQuery reference data stored in Google Cloud Storage without copying it into BigQuery's native storage.

---

#### Question 8. Clustering Best Practices
**Question:** It is best practice in BigQuery to always cluster your data?

**Answer:** False

**Explanation:** Clustering is beneficial for large tables with specific query patterns, but it's not always necessary for all datasets. Consider table size, query patterns, and cost-benefit tradeoffs.

---

#### Question 9. Understanding Table Scans
**Question:** Write a `SELECT COUNT(*)` query from the materialized table. How many bytes does it estimate will be read? Why?

**Answer:** 0 bytes

**Explanation:** COUNT(*) is free (0 MB) because BigQuery answers it from table metadata without scanning the actual data.

---

## Key Learnings from Module 3

✅ Created and compared External vs Materialized tables in BigQuery  
✅ Understood columnar storage benefits for query optimization  
✅ Implemented partitioning and clustering strategies for large datasets  
✅ Analyzed query performance improvements (91% reduction in bytes processed)  
✅ Learned BigQuery cost optimization techniques  
✅ Worked with 20M+ records of NYC Yellow Taxi data  
✅ Integrated Google Cloud Storage with BigQuery

---

## Technologies Used

- **Module 1:** Docker, PostgreSQL, pgAdmin, Python, SQL
- **Module 2:** Kestra, Google Cloud Platform (BigQuery), YAML workflows, scheduled triggers
- **Module 3:** Google BigQuery, Google Cloud Storage (GCS), Parquet files, SQL, table partitioning & clustering

---

## Course Information

This repository contains homework solutions for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/) by DataTalksClub.
