{{ config(materialized='view') }}

SELECT * FROM {{ source('staging', 'yellow_trip_data')}}