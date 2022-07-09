{{ config(materialized='table') }}

--yellow data
with yellow as (
SELECT
    *
FROM    
    {{ ref('stg_yellow_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

SELECT
    tripid,
    vendorid,
    trip_type,
    ratecodeid,
    pickup_locationid,
    --pickup zone
    pickup_zones.borough as pickup_borough,
    pickup_zones.zone as pickup_zone,
    --dropoff zone
    dropoff_zones.borough as dropoff_borough,
    dropoff_zones.zone as dropoff_zone,

    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    payment_type_description,
    congestion_surcharge
FROM yellow
INNER JOIN dim_zones as pickup_zones
on yellow.pickup_locationid = pickup_zones.locationid
INNER JOIN dim_zones as dropoff_zones
on yellow.pickup_locationid = pickup_zones.locationid




