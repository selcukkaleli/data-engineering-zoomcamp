"""REST API pipeline for NYC taxi data using dlt."""

import dlt
import requests
import logging
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

logging.getLogger("dlt").setLevel(logging.WARNING)


@dlt.resource(name="rides", write_disposition="replace")
def taxi_rides():
    """Fetch taxi rides with offset pagination. Stops on empty page."""
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
     )
    )
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page
        print(f"Fetched {len(page)} records")


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="bigquery",
    dataset_name="taxi_data",
)

if __name__ == "__main__":
    print("Starting pipeline...")
    load_info = pipeline.run(taxi_rides())
    print(f"\nPipeline finished!")
    print(load_info)