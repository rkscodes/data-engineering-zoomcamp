from json import loads
from pathlib import Path
from typing import Dict, List

from config import CONFIG_PATH, KAFKA_TOPIC, read_ccloud_config
from confluent_kafka import Consumer, Message
from rides import Ride


class JsonConsumer(Consumer):
    def __init__(self, props: Dict):
        super().__init__(props)

    def consume_from_confluent(self, topics: List[str]):
        self.subscribe(topics)
        print("Consuming from Confluent Kafka started")

        try:
            while True:
                msg: Message = self.poll(1.0)
                if msg is not None and msg.error() is None:
                    msg_value = loads(msg.value().decode("utf-8"))
                    ride = Ride.from_dict(msg_value)
                    print(ride)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()


if __name__ == "__main__":
    path = Path(__file__).parent
    configs = (path / CONFIG_PATH).resolve()
    props = read_ccloud_config(configs)
    props["group.id"] = "python-group-1"
    props["auto.offset.reset"] = "earliest"

    json_consumer = JsonConsumer(props)
    json_consumer.consume_from_confluent(topics=[KAFKA_TOPIC])
