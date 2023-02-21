{{ config(materialized='table') }}


with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),

fhv as (
    select * from {{ ref ('stg_fhv_tripdata')}}
)
select fhv.* from fhv
inner join dim_zones as pickup_zone
on fhv.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.DOlocationID = dropoff_zone.locationid