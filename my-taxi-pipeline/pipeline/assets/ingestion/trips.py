"""@bruin
name: ingestion.trips
type: python
connection: "gcp-bruin"

materialization:
  type: table
  strategy: append

requirements: requirements.txt
@bruin"""

import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import os


def materialize(**kwargs):
    """
    Fetch NYC taxi trip data from the TLC public endpoint.
    Supports multiple taxi types via the taxi_types pipeline variable.
    """
    
    # Tarihleri kwargs yerine Bruin'in ortam değişkenlerinden (Environment Variables) alıyoruz
    start_date_env = os.getenv("BRUIN_START_DATE")
    end_date_env = os.getenv("BRUIN_END_DATE")
    
    if not start_date_env or not end_date_env:
        raise ValueError("Tarih değişkenleri bulunamadı!")

    # Bruin tarihleri bazen saat ile birlikte verebilir (2022-01-01T00:00:00Z), 
    # bu yüzden sadece ilk 10 karakteri (Y-M-D) alıyoruz.
    start_date = start_date_env[:10]
    end_date = end_date_env[:10]
    
    # Parse taxi types from BRUIN_VARS environment variable
    bruin_vars = os.getenv("BRUIN_VARS", "{}")
    vars_dict = json.loads(bruin_vars)
    taxi_types = vars_dict.get("taxi_types", ["yellow"])
    
    # TLC data endpoint base URL
    BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
    
    dfs = []
    
    # Iterate through taxi types and date range
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current_date <= end_date_obj:
        year = current_date.year
        month = current_date.month
        
        for taxi_type in taxi_types:
            # Build the URL for the parquet file
            filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = f"{BASE_URL}/trip-data/{filename}"
            
            try:
                print(f"Fetching {taxi_type} taxi data for {year}-{month:02d}...")
                
                # Fetch the parquet file directly into a pandas DataFrame
                df = pd.read_parquet(url)
                
                # Add a source column to track taxi type
                df['taxi_type'] = taxi_type
                
                dfs.append(df)
                print(f"  ✓ Loaded {len(df)} rows from {filename}")
                
            except Exception as e:
                print(f"  ⚠ Could not fetch {filename}: {str(e)}")
        
        # Move to next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    
    # Combine all DataFrames
    if dfs:
        result_df = pd.concat(dfs, ignore_index=True)
        print(f"\nTotal rows ingested: {len(result_df)}")
        return result_df
    else:
        print("No data was fetched. Check date range and taxi types.")
        return pd.DataFrame()