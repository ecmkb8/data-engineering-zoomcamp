with source as (
      select * from {{ source('staging', 'green_tripdata') }}
),
renamed as (
    select
    "Green" as taxi_type
    , {{ dbt_utils.generate_surrogate_key(['vendor_id', 'lpep_pickup_datetime']) }} as trip_id
    , vendor_id
    , lpep_pickup_datetime as pickup_datetime
    , lpep_dropoff_datetime as dropoff_datetime
    , store_and_fwd_flag
    , rate_code_id
    , pu_location_id
    , do_location_id
    , passenger_count
    , trip_distance
    , fare_amount
    , extra
    , mta_tax
    , tip_amount
    , tolls_amount
    , ehail_fee
    , improvement_surcharge
    , total_amount
    , payment_type
    , {{ get_payment_type_description('payment_type') }} as payment_type_description
    , congestion_surcharge
    , lpep_pickup_date as pickup_date    
    
    from source
)


select * from renamed
