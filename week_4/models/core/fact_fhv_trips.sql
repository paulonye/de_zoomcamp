{{ config(materialized='table') }}

with fhv_data as (
    select *
    from {{ ref('fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.dispatching_base_num as dispatching_base_num,
    fhv_data.pickup_datetime as pickup_datetime,
    fhv_data.dropOff_datetime as dropOff_datetime,
    fhv_data.SR_Flag as SR_Flag,
    fhv_data.Affiliated_base_number as Affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone  
    
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.DOlocationID = dropoff_zone.locationid