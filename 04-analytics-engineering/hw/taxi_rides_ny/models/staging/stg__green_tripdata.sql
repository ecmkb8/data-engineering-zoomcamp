{{ config(materialized= 'view') }}


with tripdata as (
      
      select 
      *,
      row_number() over (partition by vendor_id, lpep_pickup_datetime) as rn 
      from {{ source('staging', 'green_tripdata') }}
      where vendor_id is not null
)
    select

    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'lpep_pickup_datetime']) }} as trip_id 
    , {{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} as vendor_id
    , {{ dbt.safe_cast("rate_code_id", api.Column.translate_type("integer")) }} as rate_code_id
    , {{ dbt.safe_cast("pu_location_id", api.Column.translate_type("integer")) }} as pu_location_id
    , {{ dbt.safe_cast("do_location_id", api.Column.translate_type("integer")) }} as do_location_id

    -- Datetimes
    , cast( lpep_pickup_datetime as timestamp) as pickup_datetime
    , cast( lpep_dropoff_datetime as timestamp) as dropoff_datetime
    , cast( lpep_pickup_date as date) as pickup_date    
    -- Trip info
    , store_and_fwd_flag
    , {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count
    , {{ dbt.safe_cast("trip_distance", api.Column.translate_type("integer")) }} as trip_distance
    , {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type

    -- Fare info
    , cast( fare_amount as numeric) as fare_amount
    , cast( extra as numeric) as extra
    , cast( mta_tax as numeric) as mta_tax
    , cast( tip_amount as numeric) as tip_amount
    , cast( tolls_amount as numeric) as tolls_amount
    , cast( ehail_fee as numeric) as ehail_fee
    , cast( improvement_surcharge as numeric) as improvement_surcharge
    , cast( total_amount as numeric) as total_amount
    , cast( congestion_surcharge as numeric) as congestion_surcharge
    , coalesce( 
        {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}, 0
    ) as payment_type
    , {{ get_payment_type_description('payment_type') }} as payment_type_description
    
    from tripdata
    where rn = 1
-- dbt build --m <model.sql> --vars '{ 'is_test_run': false }'
{%- if var('is_test_run', default=true) %}
    limit 100

{% endif -%}
