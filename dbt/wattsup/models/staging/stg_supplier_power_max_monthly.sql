with source as (

    select
        supplier,
        prm,
        grain,
        period_date,
        pmax_kw,
        pmax_kva,
        source_file,
        source_row,
        inserted_at,
        updated_at
    from {{ source('raw', 'supplier_power_max') }}
    where grain = 'monthly'

),

cleaned as (

    select
        lower(trim(supplier)) as supplier,
        trim(prm) as prm,
        grain,
        period_date,
        cast(pmax_kw as numeric(12, 6)) as pmax_kw,
        cast(pmax_kva as numeric(12, 6)) as pmax_kva,
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
        period_date,
        pmax_kw,
        pmax_kva,
        source_file,
        source_row,
        inserted_at,
        updated_at,

        case
            when coalesce(pmax_kw, 0) < 0
              or coalesce(pmax_kva, 0) < 0
            then true
            else false
        end as has_negative_power,

        date_trunc('month', period_date)::date as month_start

    from cleaned

)

select *
from final