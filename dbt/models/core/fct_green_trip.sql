{{ config(materialized ='table') }}

with green as (
    SELECT 
        *
    FROM 
        {{ ref('stg_green_tripdata') }}
),

dim_zones as (
    SELECT 
        *
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT
    tripid,
    vendorid,
    ratecodeid,
    pickup_locationid,

    -- pickup location
    pickup.borough as pickup_borough,
    pickup.zone as pickup_zone,

    --dropoff
    dropoff.borough as dropoff_borough,
    dropoff.zone as dropoff_zone,

    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    fare_amount
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

FROM green
INNER JOIN dim_zones pickup 
ON green.pickup_locationid = pickup.locationid
INNER JOIN dim_zones dropoff
ON green.dropoff_locationid = dropoff.locationid
