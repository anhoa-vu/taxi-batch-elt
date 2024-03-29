{{ config(materialized = 'view') }}

with greentrip as (
    SELECT 
        *
    FROM
        {{ source('staging', 'green_trip_data') }}
    WHERE vendorid is not null
)

SELECT 
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'pulocationid', 'payment_type'])}} as tripid,
    cast(vendorid as integer) as vendorid,
    cast (ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    --timestamp
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    'Green' as trip_type,

    --payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(total_amount as numeric) as total_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge

FROM greentrip

    