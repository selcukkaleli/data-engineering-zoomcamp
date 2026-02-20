# Data Engineering Zoomcamp

Workshop Codespaces - Module 1: Docker & SQL  
Module 2: Workflow Orchestration with Kestra  
Module 3: Data Warehousing & BigQuery  
Module 4: Analytics Engineering with dbt

## Repository Structure

| File/Folder | Description |
|------|-------------|
| **Module Folders** | |
| `module_2/` | Module 2: Workflow Orchestration with Kestra |
| `module_3/` | Module 3: Data Warehousing & BigQuery homework solutions |
| `module_4/taxi_rides_ny/` | Module 4: Analytics Engineering with dbt project |
| `ny_taxi_postgres_data/18/docker/` | PostgreSQL data volume for Module 1 |
| **Core Files** | |
| `Dockerfile` | Docker configuration for the pipeline |
| `docker-compose.yaml` | Docker Compose setup for PostgreSQL and pgAdmin |
| `ingest_data.py` | Python script for ingesting taxi data into PostgreSQL |
| `pipeline.py` | Data pipeline script |
| `main.py` | Main application entry point |
| `notebook.ipynb` | Jupyter notebook for exploratory data analysis |
| **Infrastructure** | |
| `main.tf` | Terraform main configuration file |
| `variables.tf` | Terraform variables definition |
| `.terraform.lock.hcl` | Terraform dependency lock file |
| **Data Files** | |
| `taxi_zone_lookup.csv` | Taxi zone reference data |
| **Configuration** | |
| `.python-version` | Python version specification |
| `pyproject.toml` | Python project configuration |
| `uv.lock` | UV package manager lock file |

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

## Module 4: Analytics Engineering with dbt

### Assignment Overview
Built transformation models using dbt to analyze NYC taxi data (Yellow and Green) for 2019-2020. Created staging models, intermediate models, and fact tables to answer analytical questions about trip patterns and revenue.

### Setup: Data Preparation

#### Step 1: Upload Raw CSV Files to GCS

Uploaded Green and Yellow taxi data for 2019-2020 to Google Cloud Storage using a bash script with parallel downloads:
```bash
set -euo pipefail
mkdir -p /tmp/taxi_green && cd /tmp/taxi_green

bucket="gs://dtc-de-course-485207-terra-bucket/raw_csv/green"
export bucket

# Build the list of year-months
( for year in 2019 2020; do
    for month in {01..12}; do
      echo "${year}-${month}"
    done
  done ) \
| xargs -P 4 -I {} bash -lc '
    ym="{}"
    file="green_tripdata_${ym}.csv.gz"
    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/${file}"
    dst="${bucket}/${file}"

    # Skip if already exists
    if gsutil -q stat "$dst"; then
      echo "SKIP (exists): $file"
      exit 0
    fi

    echo "Downloading $file ..."
    wget -q -O "$file" "$url"

    echo "Uploading $file ..."
    gsutil cp "$file" "$dst"

    rm -f "$file"
  '
```

**Note:** Similar approach was used for Yellow taxi data.

---

#### Step 2: Create External Tables in BigQuery

Created external tables pointing to the raw CSV files in GCS:
```sql
-- Yellow taxi external table
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-485207.dbt_skaleli.ext_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc-de-course-485207-terra-bucket/raw_csv/yellow/yellow_tripdata_*.csv.gz'],
  compression = 'GZIP',
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = TRUE,
  quote = '"'
);

-- Green taxi external table (similar approach)
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-485207.dbt_skaleli.ext_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc-de-course-485207-terra-bucket/raw_csv/green/green_tripdata_*.csv.gz'],
  compression = 'GZIP',
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = TRUE,
  quote = '"'
);
```

---

#### Step 3: Materialize External Tables

Created partitioned and clustered tables for better query performance:
```sql
-- Materialize yellow taxi data
CREATE OR REPLACE TABLE `dtc-de-course-485207.dbt_skaleli.yellow_tripdata`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY vendorid AS
SELECT t.*
FROM `dtc-de-course-485207.dbt_skaleli.ext_yellow_tripdata` AS t;

-- Materialize green taxi data (similar approach with lpep_pickup_datetime)
CREATE OR REPLACE TABLE `dtc-de-course-485207.dbt_skaleli.green_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY vendorid AS
SELECT t.*
FROM `dtc-de-course-485207.dbt_skaleli.ext_green_tripdata` AS t;
```

---

### HW4 Solutions

#### Question 1. dbt Lineage and Execution
**Question:** Given a dbt project with staging models (`stg_green_tripdata`, `stg_yellow_tripdata`) and an intermediate model (`int_trips_unioned`) that depends on both staging models, if you run `dbt run --select int_trips_unioned`, what models will be built?

**Answer:** `int_trips_unioned` only

**Explanation:** By default, `dbt run --select <model>` runs only the specified model without its upstream dependencies. To include upstream models, you would need to use `dbt run --select +int_trips_unioned`.

---

#### Question 2. dbt Tests
**Question:** You have an `accepted_values` test configured for `payment_type` column with values `[1, 2, 3, 4, 5]`. A new value `6` appears in the source data. What happens when you run `dbt test --select fct_trips`?

**Answer:** dbt will fail the test, returning a non-zero exit code

**Explanation:** dbt tests enforce data quality constraints. When a test fails, dbt returns a non-zero exit code, which is useful in CI/CD pipelines to catch data quality issues.

---

#### Question 3. Counting Records in `fct_monthly_zone_revenue`
**Question:** What is the count of records in the `fct_monthly_zone_revenue` model?

**Answer:** 12,184

---

#### Question 4. Best Performing Zone for Green Taxis (2020)
**Question:** Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue for Green taxi trips in 2020.

**Answer:** East Harlem North

**SQL Query:**
```sql
SELECT 
  pickup_zone,
  service_type,
  revenue_month,
  revenue_monthly_total_amount
FROM `dtc-de-course-485207.dbt_skaleli.fct_monthly_zone_revenue` 
WHERE service_type = "Green" 
  AND revenue_month >= DATE'2020-01-01' 
  AND revenue_month <= DATE'2020-12-31'
ORDER BY revenue_monthly_total_amount DESC;
```

---

#### Question 5. Green Taxi Trip Counts (October 2019)
**Question:** What is the total number of trips for Green taxis in October 2019?

**Answer:** 384,624

**SQL Query:**
```sql
SELECT SUM(total_monthly_trips)
FROM dtc-de-course-485207.dbt_skaleli.fct_monthly_zone_revenue
WHERE revenue_month >= DATE'2019-10-01' 
  AND revenue_month <= DATE'2019-10-31' 
  AND service_type = "Green";
```

---

#### Question 6. Build a Staging Model for FHV Data
**Question:** Create a staging model for For-Hire Vehicle (FHV) trip data for 2019 and count the records after filtering out NULL `dispatching_base_num`.

**Answer:** 43,244,693

**Implementation:** Created `stg_fhv_tripdata` model in the dbt project located in `module_4/taxi_rides_ny/models/staging/`.

---

## Key Learnings from Module 4

✅ Built transformation models with dbt (staging, intermediate, fact tables)  
✅ Understood dbt model selection and lineage (`--select`, upstream/downstream)  
✅ Implemented data quality tests with dbt (accepted_values, unique, not_null)  
✅ Created analytical models for revenue analysis across NYC zones  
✅ Worked with partitioned and clustered BigQuery tables  
✅ Analyzed trip patterns and revenue trends for 2019-2020 taxi data  
✅ Applied ELT principles: Extract (GCS), Load (BigQuery), Transform (dbt)

---

## Technologies Used

- **Module 1:** Docker, PostgreSQL, pgAdmin, Python, SQL
- **Module 2:** Kestra, Google Cloud Platform (BigQuery), YAML workflows, scheduled triggers
- **Module 3:** Google BigQuery, Google Cloud Storage (GCS), Parquet files, SQL, table partitioning & clustering
- **Module 4:** dbt (data build tool), BigQuery, Jinja templating, SQL, data modeling, testing

---

## Course Information

This repository contains homework solutions for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/) by DataTalksClub.
