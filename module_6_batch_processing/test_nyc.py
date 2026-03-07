import pyspark
import time
import urllib.request
from pathlib import Path
import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("taxi-zones-job")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
CSV_PATH = Path("taxi_zone_lookup.csv")
PARQUET_DIR = Path("zones")


def download_if_missing(url: str, dest: Path) -> None:
    """Dosya yoksa indirir."""
    if dest.exists():
        print(f"[OK] Dosya zaten var: {dest}")
        return

    print(f"[DOWNLOAD] {url} -> {dest}")
    urllib.request.urlretrieve(url, dest)
    print("[OK] İndirildi.")


def print_head(dest: Path, n: int = 10) -> None:
    """Dosyanın ilk n satırını ekrana basar."""
    print(f"\n[HEAD] İlk {n} satır ({dest}):")
    with dest.open("r", encoding="utf-8") as f:
        for i in range(n):
            line = f.readline()
            if not line:
                break
            print(line.rstrip("\n"))


def main() -> None:
    # 2) CSV dosyasını indir
    download_if_missing(CSV_URL, CSV_PATH)

    # 3) İlk satırları göster (Jupyter'deki !head yerine)
    print_head(CSV_PATH, n=10)

    # 4) Spark Session başlat
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("taxi-zones-job")
        .getOrCreate()
    )

    try:
        # 5) CSV oku
        df = (
            spark.read
            .option("header", "true")
            .csv(str(CSV_PATH))
        )

        # 6) DataFrame göster
        print("\n[SHOW] DataFrame örnek kayıtlar:")
        df.show(20, truncate=False)

        # 7) Parquet yaz
        # Eğer klasör zaten varsa Spark hata verebilir.
        # Bu yüzden overwrite kullanmak en rahatı.
        print(f"\n[WRITE] Parquet yazılıyor -> {PARQUET_DIR}")
        (
            df.write
            .mode("overwrite")
            .parquet(str(PARQUET_DIR))
        )
        print("[OK] Parquet yazıldı.")

    finally:
        # 8) Spark kapat
        time.sleep(60)
        spark.stop()
        print("[DONE] Spark kapatıldı.")


if __name__ == "__main__":
    main()