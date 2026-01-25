# Data Engineering Zoomcamp

Workshop Codespaces - Module 1: Docker & SQL

## Repository Structure

| File | Description |
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

---

## HW1 Solutions


### Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

**Answer:** 8,007
```sql
SELECT COUNT(*)
FROM public.green_tripdata_2025_11
WHERE trip_distance <= 1 
  AND lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01';
```

---

### Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles.

**Answer:** 2025-11-14 (88.03 miles)
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

### Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

**Answer:** East Harlem North (1467.13)
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

### Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

**Answer:** Yorkville West
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
