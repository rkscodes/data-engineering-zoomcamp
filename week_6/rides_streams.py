from datetime import datetime
from decimal import Decimal
from typing import List

import faust


class Ride(faust.Record, validation=True):
    vendor_id: str
    tpep_dropoff_datetime: List[datetime]
    tpep_pickup_datetime: List[datetime]
    passenger_count: int
    trip_distance: Decimal
    rate_code_id: int
    store_and_fwd_flag: str
    pu_location_id: int
    do_location_id: int
    payment_type: str
    fare_amount: Decimal
    extra: Decimal
    mta_tax: Decimal
    tip_amount: Decimal
    tolls_amount: Decimal
    improvement_surcharge: Decimal
    total_amount: Decimal
    congestion_surcharge: Decimal
