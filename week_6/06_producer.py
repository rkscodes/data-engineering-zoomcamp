import csv
from pathlib import Path
from typing import Dict, List

from config import CONFIG_PATH, INPUT_DATA_PATH, KAFKA_TOPIC, read_ccloud_config
from confluent_kafka import KafkaException, Producer
from rides import Ride


class JsonProducer(Producer):
    def __init__(self, props: Dict):
        super().__init__(props)

    @staticmethod
    def read_records(resource_path: str):
        records = []
        with open(resource_path, "r") as f:
            reader = csv.reader(f)
            _ = next(reader)  # header row
            for row in reader:
                records.append(Ride(arr=row))
        return records

    def publish_rides(self, topic: str, messages: List[Ride]):
        for ride in messages:
            try:
                self.produce(
                    topic=topic, key=str(ride.pu_location_id), value=ride.to_bytes()
                )
                print(f"Record {ride.pu_location_id} successfully produced")
            except (BufferError, KafkaException, NotImplementedError) as e:
                print(e.__str__())
                break


if __name__ == "__main__":
    path = Path(__file__).parent
    configs = (path / CONFIG_PATH).resolve()
    props = read_ccloud_config(configs)
    print(configs)
    producer = JsonProducer(props)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
    print(rides)

    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)
    producer.flush()
