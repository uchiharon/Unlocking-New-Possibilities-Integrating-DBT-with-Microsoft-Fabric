{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select 
  -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) -%}

  TOP( 100) 

{%- endif %} *,
    row_number() over(partition by vendorID, lpepPickupDatetime order by lpepPickupDatetime) as rn
  from {{ source('nyc_taxi', 'nyctlc')}}
  where vendorID is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorID', 'lpepPickupDatetime']) }} as tripid,
    {{ dbt.safe_cast("vendorID", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("rateCodeID", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("puLocationId", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("doLocationId", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    (lpepPickupDatetime ) as pickup_datetime,
    (lpepDropoffDatetime ) as dropoff_datetime,
    
    -- trip info
    storeAndFwdFlag as store_and_fwd_flag,
    {{ dbt.safe_cast("passengerCount", api.Column.translate_type("integer")) }} as passenger_count,
    cast(tripDistance as numeric) as trip_distance,
    {{ dbt.safe_cast("tripType", api.Column.translate_type("integer")) }} as trip_type,

    -- payment info
    cast(fareAmount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mtaTax as numeric) as mta_tax,
    cast(tipAmount as numeric) as tip_amount,
    cast(tollsAmount as numeric) as tolls_amount,
    cast(ehailFee as numeric) as ehail_fee,
    cast(improvementSurcharge as numeric) as improvement_surcharge,
    cast(totalAmount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("paymentType", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description("paymentType") }} as payment_type_description
from tripdata
where rn = 1


