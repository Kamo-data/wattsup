-- postgres/init.sql
-- Init DB Watt'sUp - V1 RAW multi-exports Engie / Linky

create schema if not exists raw;
create schema if not exists analytics;
create schema if not exists config;
create schema if not exists audit;

create table if not exists config.tariff_hp_hc (
  effective_from date primary key,
  hp_eur_per_kwh numeric(10,4) not null,
  hc_eur_per_kwh numeric(10,4) not null,
  inserted_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists raw.supplier_meter_readings (
  supplier     text not null,
  prm          text not null default '',
  grain        text not null,
  period_start timestamp without time zone not null,
  period_end   timestamp without time zone not null,
  cadran       text not null default '',
  kwh          double precision not null,
  read_type    text not null default '',
  index_start  double precision null,
  index_end    double precision null,
  source_file  text not null,
  source_row   integer not null,
  inserted_at  timestamp without time zone not null default now(),
  updated_at   timestamp without time zone not null default now(),

  constraint supplier_meter_readings_chk_grain
    check (grain = any (array['monthly'::text, 'hourly'::text])),

  constraint supplier_meter_readings_uk
    unique (supplier, prm, grain, period_start, period_end, cadran, read_type)
);

create index if not exists supplier_meter_readings_idx_start
  on raw.supplier_meter_readings (supplier, prm, grain, period_start);

create index if not exists supplier_meter_readings_idx_end
  on raw.supplier_meter_readings (supplier, prm, grain, period_end);

create table if not exists raw.supplier_power_max (
  supplier     text not null,
  prm          text not null default '',
  grain        text not null,
  period_date  date not null,
  pmax_kw      double precision null,
  pmax_kva     double precision null,
  source_file  text not null,
  source_row   integer not null,
  inserted_at  timestamp without time zone not null default now(),
  updated_at   timestamp without time zone not null default now(),

  constraint supplier_power_max_chk_grain
    check (grain = any (array['daily'::text, 'monthly'::text])),

  constraint supplier_power_max_uk
    unique (supplier, prm, grain, period_date)
);

create index if not exists supplier_power_max_idx_date
  on raw.supplier_power_max (supplier, prm, grain, period_date);

create table if not exists audit.ingestion_runs (
  run_id      bigserial primary key,
  supplier    text not null,
  prm         text not null default '',
  started_at  timestamptz not null default now(),
  finished_at timestamptz null,
  status      text not null default 'running',
  details     jsonb null
);