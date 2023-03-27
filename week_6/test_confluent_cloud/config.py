from typing import Dict

CONFIG_PATH = "client.properties"
INPUT_DATA_PATH = "rides.csv"
KAFKA_TOPIC = "taxi_rides"


def read_ccloud_config(config_file) -> Dict[str, str]:
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                key, value = line.strip().split("=", 1)
                conf[key] = value.strip()
    return conf
