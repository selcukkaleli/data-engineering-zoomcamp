"""
Download NYC TLC parquet files (yellow+green, 2019-2020) and upload them to GCS.

Folder layout in your bucket:
gs://<BUCKET_NAME>/raw/<taxi_type>/<year>/<taxi_type>_tripdata_<year>-<month>.parquet

Example:
gs://my-bucket/raw/yellow/2019/yellow_tripdata_2019-01.parquet
"""

import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden


# =========================
# CONFIG (edit these)
# =========================

# Your GCS bucket (must be globally unique)
BUCKET_NAME = "dtc-de-course-485207-terra-bucket"

# Where to put files in the bucket
GCS_PREFIX = "raw"

# Download source (NYC TLC)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# What to download
TAXI_TYPES = ["yellow", "green"]
YEARS = [2019, 2020]
MONTHS = list(range(1, 13))  # 1..12

# Local download folder (keeps your project tidy)
DOWNLOAD_DIR = "./downloads_taxi_parquet"

# Concurrency
MAX_WORKERS_DOWNLOAD = 4
MAX_WORKERS_UPLOAD = 4

# Upload chunk size (helps with big files)
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB

# Retry settings (network hiccups happen)
MAX_RETRIES = 3
SLEEP_BETWEEN_RETRIES_SEC = 5


# =========================
# HELPERS
# =========================

def build_url(taxi_type: str, year: int, month: int) -> str:
    """
    Build the official NYC TLC parquet URL.
    Example:
    https://.../yellow_tripdata_2019-01.parquet
    """
    return f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def local_file_path(taxi_type: str, year: int, month: int) -> str:
    """
    Decide the local filename. Keep it identical to the source name.
    """
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    return os.path.join(DOWNLOAD_DIR, taxi_type, str(year), filename)


def gcs_blob_name(taxi_type: str, year: int, month: int) -> str:
    """
    Decide where the file will live in the bucket.
    Example:
    raw/yellow/2019/yellow_tripdata_2019-01.parquet
    """
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    return f"{GCS_PREFIX}/{taxi_type}/{year}/{filename}"


def ensure_local_dirs():
    """
    Create the local download directory tree.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def get_storage_client() -> storage.Client:
    """
    Create a GCS client.

    Best practice:
    - locally: set GOOGLE_APPLICATION_CREDENTIALS to your service account json
      export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.secrets/key.json"
    - in Docker: mount the json and also set GOOGLE_APPLICATION_CREDENTIALS
    """
    return storage.Client()


def ensure_bucket_exists(client: storage.Client, bucket_name: str) -> storage.Bucket:
    """
    Validate bucket exists and you have access.
    (We do NOT auto-create here unless you want that behavior.)
    """
    try:
        bucket = client.get_bucket(bucket_name)
        return bucket
    except NotFound:
        print(f"[ERROR] Bucket '{bucket_name}' not found. Create it first, or fix BUCKET_NAME.")
        sys.exit(1)
    except Forbidden:
        print(f"[ERROR] No access to bucket '{bucket_name}'. Check IAM permissions.")
        sys.exit(1)


def download_one(taxi_type: str, year: int, month: int) -> str | None:
    """
    Download a single parquet file to local disk.
    Returns the local file path if successful, else None.
    """
    url = build_url(taxi_type, year, month)
    path = local_file_path(taxi_type, year, month)

    # Make sure folder exists (e.g., downloads_taxi_parquet/yellow/2019/)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[DOWNLOAD] {url} (attempt {attempt})")
            urllib.request.urlretrieve(url, path)
            return path
        except Exception as e:
            print(f"[WARN] Download failed: {url} -> {e}")
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES_SEC)

    print(f"[ERROR] Giving up download: {url}")
    return None


def gcs_object_exists(bucket: storage.Bucket, blob_name: str) -> bool:
    """
    Check if object already exists in GCS.
    Useful so we can skip re-upload.
    """
    blob = bucket.blob(blob_name)
    return blob.exists()


def upload_one(bucket: storage.Bucket, taxi_type: str, year: int, month: int, local_path: str) -> bool:
    """
    Upload one local file to GCS (with retries).
    Returns True if uploaded or already exists; False if failed.
    """
    blob_name = gcs_blob_name(taxi_type, year, month)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    # Skip upload if the object is already there
    if gcs_object_exists(bucket, blob_name):
        print(f"[SKIP] Already exists: gs://{bucket.name}/{blob_name}")
        return True

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[UPLOAD] {local_path} -> gs://{bucket.name}/{blob_name} (attempt {attempt})")
            blob.upload_from_filename(local_path)

            # Verify
            if gcs_object_exists(bucket, blob_name):
                print(f"[OK] Verified upload: gs://{bucket.name}/{blob_name}")
                return True
            else:
                print(f"[WARN] Upload verification failed (will retry): {blob_name}")

        except Exception as e:
            print(f"[WARN] Upload failed: {blob_name} -> {e}")

        if attempt < MAX_RETRIES:
            time.sleep(SLEEP_BETWEEN_RETRIES_SEC)

    print(f"[ERROR] Giving up upload: {blob_name}")
    return False


def build_tasks():
    """
    Create the list of (taxi_type, year, month) tasks.
    """
    tasks = []
    for taxi_type in TAXI_TYPES:
        for year in YEARS:
            for month in MONTHS:
                tasks.append((taxi_type, year, month))
    return tasks


# =========================
# MAIN
# =========================

def main():
    ensure_local_dirs()

    # 1) Create GCS client and validate bucket access
    client = get_storage_client()
    bucket = ensure_bucket_exists(client, BUCKET_NAME)

    tasks = build_tasks()
    print(f"[INFO] Total files planned: {len(tasks)}")

    # 2) Download concurrently
    downloaded = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DOWNLOAD) as pool:
        futures = {pool.submit(download_one, t, y, m): (t, y, m) for (t, y, m) in tasks}
        for fut in as_completed(futures):
            taxi_type, year, month = futures[fut]
            path = fut.result()
            if path:
                downloaded.append((taxi_type, year, month, path))

    print(f"[INFO] Downloaded successfully: {len(downloaded)}/{len(tasks)}")

    # 3) Upload concurrently
    uploaded_ok = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_UPLOAD) as pool:
        futures = {
            pool.submit(upload_one, bucket, t, y, m, path): (t, y, m, path)
            for (t, y, m, path) in downloaded
        }
        for fut in as_completed(futures):
            ok = fut.result()
            if ok:
                uploaded_ok += 1

    print(f"[DONE] Uploaded (or already existed): {uploaded_ok}/{len(downloaded)}")

    # Small hint for next step
    print("\nNext step (important): these files are in GCS, but dbt needs BigQuery tables.")
    print("We will load these parquet files from GCS into BigQuery tables next.")


if __name__ == "__main__":
    main()