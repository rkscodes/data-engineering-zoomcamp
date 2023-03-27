from typing import Dict

CONFIG_PATH = (
    "client.properties"  # get this file from confluent client while configuring
)
INPUT_DATA_PATH = "rides.csv"
KAFKA_TOPIC = "taxi_rides"


def read_ccloud_config(config_file) -> Dict[str, str]:
    conf = {}
    with open(config_file, "r") as fh:
        for line in fh:
            line = line.strip()
            if len(line) and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf
