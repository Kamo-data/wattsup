with source as (

    select *
    from {{ ref('stg_supplier_meter_readings_hourly') }}

),

final as (

    select
        supplier,
        prm,
        period_start,
        period_end,
        interval_minutes,
        cadran,
        read_type,

        kwh,
        case
            when interval_minutes > 0
                then kwh * 60.0 / interval_minutes
            else null
        end as kwh_per_hour_est,

        period_start::date as day_date,
        date_trunc('month', period_start)::date as month_start,

        extract(year from period_start) as year_num,
        extract(month from period_start) as month_num,
        extract(day from period_start) as day_num,
        extract(hour from period_start) as hour_num,
        extract(dow from period_start) as day_of_week_num,

        case
            when extract(dow from period_start) in (0, 6) then true
            else false
        end as is_weekend,

        has_invalid_period,
        has_negative_kwh,

        source_file,
        source_row,
        inserted_at,
        updated_at

    from source

)

select *
from final