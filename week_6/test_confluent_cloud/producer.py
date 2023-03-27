from typing import Dict, List
from confluent_kafka import Producer, KafkaException
import csv
from rides import Ride
from pathlib import Path
from config import CONFIG_PATH, INPUT_DATA_PATH, KAFKA_TOPIC, read_ccloud_config


class JsonProdcuer(Producer):
    def __init__(self, props: Dict):
        super().__init__(props)

    @staticmethod
    def read_records(resources_path: str):
        records = []
        with open(resources_path, "r") as f:
            reader = csv.reader(f)
            _ = next(reader)  # skip first line
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
            except KeyboardInterrupt:
                break


if __name__ == "__main__":
    path = Path(__file__).parent
    configs = (path / CONFIG_PATH).resolve()  # doubt
    print(configs)
    prop = read_ccloud_config(configs)

    producer = JsonProdcuer(prop)
    rides = producer.read_records(resources_path=INPUT_DATA_PATH)
    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)
    producer.flush()
