with source as (

    select *
    from {{ ref('stg_supplier_power_max_daily') }}

),

final as (

    select
        supplier,
        prm,
        period_date as day_date,
        date_trunc('month', period_date)::date as month_start,

        pmax_kw,
        pmax_kva,

        case
            when pmax_kw is not null and pmax_kva is not null and pmax_kva <> 0
                then pmax_kw / pmax_kva
            else null
        end as kw_to_kva_ratio,

        has_negative_power,

        source_file,
        source_row,
        inserted_at,
        updated_at

    from source

)

select *
from final