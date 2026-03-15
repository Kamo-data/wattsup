with daily as (

    select *
    from {{ ref('fct_power_max_daily') }}

),

monthly as (

    select *
    from {{ ref('stg_supplier_power_max_monthly') }}

),

daily_agg as (

    select
        supplier,
        prm,
        month_start,

        count(*) as day_count,
        avg(pmax_kw) as avg_pmax_kw_from_daily,
        max(pmax_kw) as max_pmax_kw_from_daily,
        avg(pmax_kva) as avg_pmax_kva_from_daily,
        max(pmax_kva) as max_pmax_kva_from_daily,

        max(case when has_negative_power then 1 else 0 end) = 1 as has_negative_power_daily

    from daily
    group by
        supplier,
        prm,
        month_start

),

monthly_clean as (

    select
        supplier,
        prm,
        month_start,
        pmax_kw as pmax_kw_monthly,
        pmax_kva as pmax_kva_monthly,
        has_negative_power as has_negative_power_monthly,
        source_file,
        source_row,
        inserted_at,
        updated_at
    from monthly

),

final as (

    select
        coalesce(m.supplier, d.supplier) as supplier,
        coalesce(m.prm, d.prm) as prm,
        coalesce(m.month_start, d.month_start) as month_start,

        d.day_count,
        d.avg_pmax_kw_from_daily,
        d.max_pmax_kw_from_daily,
        d.avg_pmax_kva_from_daily,
        d.max_pmax_kva_from_daily,

        m.pmax_kw_monthly,
        m.pmax_kva_monthly,

        case
            when m.pmax_kw_monthly is not null and d.max_pmax_kw_from_daily is not null
                then m.pmax_kw_monthly - d.max_pmax_kw_from_daily
            else null
        end as delta_pmax_kw_monthly_vs_daily_max,

        case
            when m.pmax_kva_monthly is not null and d.max_pmax_kva_from_daily is not null
                then m.pmax_kva_monthly - d.max_pmax_kva_from_daily
            else null
        end as delta_pmax_kva_monthly_vs_daily_max,

        coalesce(d.has_negative_power_daily, false) as has_negative_power_daily,
        coalesce(m.has_negative_power_monthly, false) as has_negative_power_monthly,

        m.source_file,
        m.source_row,
        m.inserted_at,
        m.updated_at

    from monthly_clean m
    full outer join daily_agg d
        on m.supplier = d.supplier
       and m.prm = d.prm
       and m.month_start = d.month_start

)

select *
from final