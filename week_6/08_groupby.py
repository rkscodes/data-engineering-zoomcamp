from pathlib import Path

import faust
from aiokafka.helpers import create_ssl_context
from config import CONFIG_PATH, KAFKA_TOPIC, read_ccloud_config
from faust.types.auth import AuthProtocol, SASLMechanism
from faust.types.streams import StreamT
from rides_streams import Ride


path = Path(__file__).parent
config_path = (path / CONFIG_PATH).resolve()
config = read_ccloud_config(config_file=config_path)

broker_credentials = faust.SASLCredentials(
    mechanism=SASLMechanism.PLAIN,
    ssl_context=create_ssl_context(),
    username=config["sasl.username"],
    password=config["sasl.password"],
)

broker_credentials.protocol = AuthProtocol.SASL_SSL


app = faust.App(
    f"de.zoomcamp.{path.name}",
    broker=f'kafka://{config["bootstrap.servers"]}',
    value_serializer="json",
    broker_credentials=broker_credentials,
    topic_partitions=2,
    topic_replication_factor=3,
)


topic = app.topic(KAFKA_TOPIC, value_type=Ride, partitions=2)
vendor_rides = app.Table("vendor_rides", default=int, partitions=2)


@app.agent(topic)
async def process_topic(stream: StreamT):
    async for event in stream.group_by(Ride.vendor_id):  # type : ignore
        assert isinstance(event, Ride)
        vendor_rides[event.vendor_id] += 1


@app.timer(10.0)
async def log_table_every_10_seconds():
    if app.rebalancing:
        return
    print(
        "Vendor IDs:"
        + "\n"
        + vendor_rides.as_ansitable(
            key="vendor_id",
            value="count",
            title="Total number of rides for each Vendor ID",
            sort=True,
        )
    )


if __name__ == "__main__":
    # Run by executing `faust -A groupby worker -l info` in the CLI

    # Remember to produce some event using the producer class
    # from the json example before running this

    app.main()
