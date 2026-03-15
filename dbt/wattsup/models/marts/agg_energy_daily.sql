with source as (

    select *
    from {{ ref('fct_energy_hourly') }}

),

final as (

    select
        supplier,
        prm,
        day_date,
        month_start,

        min(period_start) as first_period_start,
        max(period_end) as last_period_end,

        count(*) as interval_count,

        sum(case when cadran = 'HP' then kwh else 0 end) as kwh_hp,
        sum(case when cadran = 'HC' then kwh else 0 end) as kwh_hc,
        sum(case when cadran = 'BASE' then kwh else 0 end) as kwh_base,
        sum(kwh) as kwh_total,

        avg(kwh_per_hour_est) as avg_kwh_per_hour_est,
        max(kwh_per_hour_est) as max_kwh_per_hour_est,

        max(case when has_invalid_period then 1 else 0 end) = 1 as has_invalid_period,
        max(case when has_negative_kwh then 1 else 0 end) = 1 as has_negative_kwh,

        max(case when is_weekend then 1 else 0 end) = 1 as is_weekend_day

    from source
    group by
        supplier,
        prm,
        day_date,
        month_start

)

select *
from final