

with 
trips_green as (
    select
        *,
        "Green" as `service_type`
    from {{ ref("stg__green_tripdata")}}
),
trips_yellow as (
    select
        *,
        "Yellow" as `service_type`
    from {{ ref("stg__yellow_tripdata")}}
),
zones as (
    select * from {{ ref("dim_zones")}}
),
all_trips as (
    select * from trips_green
    union all
    select * from trips_yellow
)

select
    trip_id
    , vendor_id
    , service_type
    , rate_code_id
    , pu_location_id as pickup_location_id
    , pu_zones.borough as pickup_borough
    , pu_zones.zone as pickup_zone
    , pu_zones.service_zone as pickup_service_zone    
    , do_location_id as dropoff_location_id
    , do_zones.borough as dropoff_borough
    , do_zones.zone as dropoff_zone
    , do_zones.service_zone as dropoff_service_zone    
    , all_trips.pickup_datetime
	, all_trips.dropoff_datetime
	, all_trips.store_and_fwd_flag
	, all_trips.passenger_count
	, all_trips.trip_distance
	, all_trips.trip_type
	, all_trips.fare_amount
	, all_trips.extra
	, all_trips.mta_tax
	, all_trips.tip_amount
	, all_trips.tolls_amount
	, all_trips.ehail_fee
	, all_trips.improvement_surcharge
	, all_trips.total_amount
	, all_trips.payment_type
	, all_trips.payment_type_description

from all_trips
left join zones as pu_zones
    on all_trips.pu_location_id = pu_zones.locationid
left join zones as do_zones
    on all_trips.do_location_id = do_zones.locationid
