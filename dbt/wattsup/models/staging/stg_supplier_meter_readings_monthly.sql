with source as (

    select
        supplier,
        prm,
        grain,
        period_start,
        period_end,
        cadran,
        kwh,
        read_type,
        index_start,
        index_end,
        source_file,
        source_row,
        inserted_at,
        updated_at
    from {{ source('raw', 'supplier_meter_readings') }}
    where grain = 'monthly'

),

cleaned as (

    select
        lower(trim(supplier)) as supplier,
        trim(prm) as prm,
        grain,
        period_start,
        period_end,

        case
            when trim(coalesce(cadran, '')) = '' then 'BASE'
            else upper(trim(cadran))
        end as cadran,

        cast(kwh as numeric(12, 6)) as kwh,

        case
            when trim(coalesce(read_type, '')) = '' then 'monthly'
            else lower(trim(read_type))
        end as read_type,

        cast(index_start as numeric(18, 6)) as index_start,
        cast(index_end as numeric(18, 6)) as index_end,

        source_file,
        source_row,
        inserted_at,
        updated_at

    from source

),

final as (

    select
        supplier,
        prm,
        grain,
        period_start,
        period_end,
        cadran,
        kwh,
        read_type,
        index_start,
        index_end,
        source_file,
        source_row,
        inserted_at,
        updated_at,

        extract(epoch from (period_end - period_start)) / 86400 as period_days,

        case
            when period_end <= period_start then true
            else false
        end as has_invalid_period,

        case
            when kwh < 0 then true
            else false
        end as has_negative_kwh,

        case
            when index_start is not null
             and index_end is not null
             and index_end < index_start then true
            else false
        end as has_decreasing_index

    from cleaned

)

select *
from final