import json
from kafka import KafkaConsumer

server = "localhost:9092"
topic_name = "green-trips"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="green-trip-counter",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

count = 0
total = 0
max_messages = 49416

for message in consumer:
    trip = message.value
    total += 1

    if trip["trip_distance"] and trip["trip_distance"] > 5:
        count += 1

    if total >= max_messages:
        break

print("Trips with distance > 5:", count)