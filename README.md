# Data Engineering Zoomcamp

Workshop Codespaces - Module 1: Docker & SQL  
Module 2: Workflow Orchestration with Kestra

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

## Technologies Used

- **Module 1:** Docker, PostgreSQL, pgAdmin, Python, SQL
- **Module 2:** Kestra, Google Cloud Platform (BigQuery), YAML workflows, scheduled triggers

---

## Course Information

This repository contains homework solutions for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/) by DataTalksClub.
