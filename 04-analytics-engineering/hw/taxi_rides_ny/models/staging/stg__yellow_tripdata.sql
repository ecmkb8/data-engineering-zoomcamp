with source as (
      select * from {{ source('staging', 'yellow_tripdata') }}
),
renamed as (
    select
    "Yellow" as taxi_type
    , {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as trip_id
    , vendorid as vendor_id
    , tpep_pickup_datetime as pickup_datetime
    , tpep_dropoff_datetime as dropoff_datetime
    , store_and_fwd_flag
    , ratecodeid as rate_code_id
    , pulocationid as pu_location_id
    , dolocationid as do_location_id
    , passenger_count
    , trip_distance
    , fare_amount
    , extra
    , mta_tax
    , tip_amount
    , tolls_amount
    , improvement_surcharge
    , total_amount
    , payment_type
    , {{ get_payment_type_description('payment_type') }} as payment_type_description 
    , congestion_surcharge
    , tpep_pickup_date as pickup_date    
    
    from source
)


select * from renamed
