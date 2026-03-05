import dlt

@dlt.resource(name="test_data", write_disposition="replace")
def test_resource():
    yield [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]

pipeline = dlt.pipeline(
    pipeline_name="test_pipeline",
    destination="bigquery",
    dataset_name="taxi_data",
)

load_info = pipeline.run(test_resource())
print(load_info)