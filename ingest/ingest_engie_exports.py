#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def _strip_bom(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace("\ufeff", "")


def _norm(s: str) -> str:
    s = _strip_bom(str(s)).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _to_float(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "":
        return None
    s = s.replace("\u00a0", " ").replace(" ", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _to_datetime(x) -> Optional[pd.Timestamp]:
    if pd.isna(x):
        return pd.NaT

    s = _strip_bom(str(x)).strip()
    if s == "":
        return pd.NaT

    s = s.replace("h", ":")
    return pd.to_datetime(s, dayfirst=True, errors="coerce")


def _to_date(x) -> Optional[dt.date]:
    ts = _to_datetime(x)
    if pd.isna(ts):
        return None
    return ts.date()


def _month_name_to_num_fr(x: str) -> Optional[int]:
    if x is None:
        return None
    s = _strip_bom(str(x)).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    mapping = {
        "janvier": 1,
        "fevrier": 2,
        "mars": 3,
        "avril": 4,
        "mai": 5,
        "juin": 6,
        "juillet": 7,
        "aout": 8,
        "septembre": 9,
        "octobre": 10,
        "novembre": 11,
        "decembre": 12,
    }

    if s.isdigit():
        m = int(s)
        if 1 <= m <= 12:
            return m

    return mapping.get(s)


def _guess_kind(path: str) -> str:
    name = os.path.basename(path).lower()

    if name.endswith(".xlsx") or name.endswith(".xls"):
        return "hourly_xlsx"

    if name.endswith(".csv"):
        if "puissance" in name and "journ" in name:
            return "pmax_daily_csv"
        if "puissance" in name and "mens" in name:
            return "pmax_monthly_csv"
        if "releve" in name and "mens" in name:
            return "monthly_csv"

        try:
            with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
                head = "\n".join([next(f) for _ in range(8)])
            low = head.lower()
            if ("date début" in low or "date debut" in low) and "consommation" in low:
                return "monthly_csv"
            if "puissance" in low and "date" in low:
                return "pmax_daily_csv"
            if "puissance" in low and ("annee" in low or "mois" in low):
                return "pmax_monthly_csv"
        except Exception:
            pass

    return "unknown"


@dataclass
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str


def _connect(cfg: DbConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )


DDL_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS {schema};
"""

DDL_METER = """
CREATE TABLE IF NOT EXISTS {schema}.supplier_meter_readings (
  supplier     text NOT NULL,
  prm          text NOT NULL DEFAULT '',
  grain        text NOT NULL,
  period_start timestamp without time zone NOT NULL,
  period_end   timestamp without time zone NOT NULL,
  cadran       text NOT NULL DEFAULT '',
  kwh          double precision NOT NULL,
  read_type    text NOT NULL DEFAULT '',
  index_start  double precision NULL,
  index_end    double precision NULL,
  source_file  text NOT NULL,
  source_row   integer NOT NULL,
  inserted_at  timestamp without time zone NOT NULL DEFAULT now(),
  updated_at   timestamp without time zone NOT NULL DEFAULT now(),

  CONSTRAINT supplier_meter_readings_chk_grain
    CHECK (grain = ANY (ARRAY['monthly'::text, 'hourly'::text])),

  CONSTRAINT supplier_meter_readings_uk
    UNIQUE (supplier, prm, grain, period_start, period_end, cadran, read_type)
);

CREATE INDEX IF NOT EXISTS supplier_meter_readings_idx_start
  ON {schema}.supplier_meter_readings (supplier, prm, grain, period_start);

CREATE INDEX IF NOT EXISTS supplier_meter_readings_idx_end
  ON {schema}.supplier_meter_readings (supplier, prm, grain, period_end);
"""

DDL_PMAX = """
CREATE TABLE IF NOT EXISTS {schema}.supplier_power_max (
  supplier     text NOT NULL,
  prm          text NOT NULL DEFAULT '',
  grain        text NOT NULL,
  period_date  date NOT NULL,
  pmax_kw      double precision NULL,
  pmax_kva     double precision NULL,
  source_file  text NOT NULL,
  source_row   integer NOT NULL,
  inserted_at  timestamp without time zone NOT NULL DEFAULT now(),
  updated_at   timestamp without time zone NOT NULL DEFAULT now(),

  CONSTRAINT supplier_power_max_chk_grain
    CHECK (grain = ANY (ARRAY['daily'::text, 'monthly'::text])),

  CONSTRAINT supplier_power_max_uk
    UNIQUE (supplier, prm, grain, period_date)
);

CREATE INDEX IF NOT EXISTS supplier_power_max_idx_date
  ON {schema}.supplier_power_max (supplier, prm, grain, period_date);
"""


def ensure_schema_and_tables(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_SCHEMA.format(schema=schema))
        cur.execute(DDL_METER.format(schema=schema))
        cur.execute(DDL_PMAX.format(schema=schema))
    conn.commit()


def _read_csv_any(path: str) -> pd.DataFrame:
    for sep in [";", ","]:
        try:
            df = pd.read_csv(
                path,
                sep=sep,
                dtype=str,
                encoding="utf-8-sig",
                engine="python",
            )
            if df.shape[1] > 1:
                return df
        except Exception:
            pass

    return pd.read_csv(
        path,
        sep=None,
        engine="python",
        dtype=str,
        encoding="utf-8-sig",
    )


def read_engie_monthly_csv(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines):
        low = line.lower()
        if ("date début" in low or "date debut" in low) and "date fin" in low and "consommation" in low:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("Impossible de trouver la ligne d'en-tête du CSV mensuel Engie.")

    df = pd.read_csv(
        path,
        sep=";",
        skiprows=header_idx,
        encoding="utf-8-sig",
        dtype=str,
        engine="python",
        on_bad_lines="skip",
    )

    df.columns = [_norm(c) for c in df.columns]

    col_start = "date_debut"
    col_end = "date_fin"

    col_read_type = None
    for c in ["type_releve", "type_de_releve"]:
        if c in df.columns:
            col_read_type = c
            break

    col_cadran = None
    for c in ["type_cadran", "cadran"]:
        if c in df.columns:
            col_cadran = c
            break

    col_idx_start = "index_debut" if "index_debut" in df.columns else None
    col_idx_end = "index_fin" if "index_fin" in df.columns else None

    col_kwh = None
    for c in df.columns:
        if "consommation" in c and "kwh" in c:
            col_kwh = c
            break

    missing = []
    if col_start not in df.columns:
        missing.append("Date début")
    if col_end not in df.columns:
        missing.append("Date fin")
    if col_read_type is None:
        missing.append("Type relève")
    if col_cadran is None:
        missing.append("Type cadran / Cadran")
    if col_kwh is None:
        missing.append("Consommation (kWh)")

    if missing:
        raise ValueError(
            f"Colonnes manquantes dans CSV mensuel Engie: {missing}. Colonnes trouvées: {list(df.columns)}"
        )

    out = pd.DataFrame(
        {
            "period_start": df[col_start].apply(_to_datetime),
            "period_end": df[col_end].apply(_to_datetime),
            "read_type": df[col_read_type].astype(str).fillna("").str.strip(),
            "cadran": df[col_cadran].astype(str).fillna("").str.strip(),
            "kwh": df[col_kwh].apply(_to_float),
            "index_start": df[col_idx_start].apply(_to_float) if col_idx_start else None,
            "index_end": df[col_idx_end].apply(_to_float) if col_idx_end else None,
        }
    )
    out["grain"] = "monthly"

    out = out.dropna(subset=["period_start", "period_end", "kwh"])
    return out


def read_engie_hourly_xlsx(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, dtype=str)
    df.columns = [_norm(c) for c in df.columns]

    date_col = None
    hour_col = None
    start_col = None
    end_col = None

    if "date" in df.columns:
        date_col = "date"
    if "heure" in df.columns:
        hour_col = "heure"
    if "date_debut" in df.columns:
        start_col = "date_debut"
    if "date_fin" in df.columns:
        end_col = "date_fin"

    kwh_col = None
    for c in df.columns:
        if "consommation" in c and ("kwh" in c or "wh" in c):
            kwh_col = c
            break
    if kwh_col is None and "kwh" in df.columns:
        kwh_col = "kwh"

    if kwh_col is None:
        raise ValueError(f"Colonne consommation introuvable dans l'XLSX. Colonnes: {list(df.columns)}")

    if start_col is not None:
        period_start = df[start_col].apply(_to_datetime)
        if end_col is not None:
            period_end = df[end_col].apply(_to_datetime)
        else:
            period_end = period_start + pd.Timedelta(hours=1)

    elif date_col is not None and hour_col is not None:
        def _mk_start(row):
            d = str(row[date_col]).strip()
            h = str(row[hour_col]).strip()
            if d == "" or h == "":
                return pd.NaT
            if "-" in h:
                h = h.split("-")[0].strip()
            return pd.to_datetime(f"{d} {h}", dayfirst=True, errors="coerce")

        period_start = df.apply(_mk_start, axis=1)
        period_end = period_start + pd.Timedelta(hours=1)

    elif date_col is not None:
        period_start = df[date_col].apply(_to_datetime)
        s = period_start.dropna().sort_values()
        if len(s) >= 2:
            delta = s.diff().dropna().mode().iloc[0]
            if delta <= pd.Timedelta(minutes=30):
                period_end = period_start + pd.Timedelta(minutes=30)
            else:
                period_end = period_start + pd.Timedelta(hours=1)
        else:
            period_end = period_start + pd.Timedelta(minutes=30)
    else:
        raise ValueError(f"Colonnes date/heure introuvables dans l'XLSX. Colonnes: {list(df.columns)}")

    out = pd.DataFrame(
        {
            "period_start": period_start,
            "period_end": period_end,
            "kwh": df[kwh_col].apply(_to_float),
            "index_start": None,
            "index_end": None,
        }
    )
    out["grain"] = "hourly"
    out["read_type"] = "hourly"
    out["cadran"] = ""

    out = out.dropna(subset=["period_start", "period_end", "kwh"])
    return out


def read_engie_power_daily_csv(path: str) -> pd.DataFrame:
    df = _read_csv_any(path)
    df.columns = [_norm(c) for c in df.columns]

    date_col = None
    for c in ["date", "jour"]:
        if c in df.columns:
            date_col = c
            break

    pkw_col = None
    pkva_col = None
    for c in df.columns:
        if "puissance" in c and "kw" in c:
            pkw_col = c
        if "puissance" in c and "kva" in c:
            pkva_col = c

    if date_col is None:
        raise ValueError(f"Colonne date introuvable dans puissance journalière. Colonnes: {list(df.columns)}")

    out = pd.DataFrame(
        {
            "period_date": df[date_col].apply(_to_date),
            "pmax_kw": df[pkw_col].apply(_to_float) if pkw_col else None,
            "pmax_kva": df[pkva_col].apply(_to_float) if pkva_col else None,
        }
    )
    out["grain"] = "daily"
    out = out.dropna(subset=["period_date"])
    return out


def read_engie_power_monthly_csv(path: str) -> pd.DataFrame:
    df = _read_csv_any(path)
    df.columns = [_norm(c) for c in df.columns]

    year_col = "annee" if "annee" in df.columns else None
    month_col = "mois" if "mois" in df.columns else None

    pkw_col = None
    pkva_col = None
    for c in df.columns:
        if "puissance" in c and "kw" in c:
            pkw_col = c
        if "puissance" in c and "kva" in c:
            pkva_col = c

    if year_col and month_col:
        def _mk_period_date(row):
            year = str(row[year_col]).strip()
            month_raw = row[month_col]
            if year == "":
                return None
            month_num = _month_name_to_num_fr(month_raw)
            if month_num is None:
                return None
            return dt.date(int(year), int(month_num), 1)

        period_date = df.apply(_mk_period_date, axis=1)
    else:
        raise ValueError(f"Colonnes année/mois introuvables dans puissance mensuelle. Colonnes: {list(df.columns)}")

    out = pd.DataFrame(
        {
            "period_date": period_date,
            "pmax_kw": df[pkw_col].apply(_to_float) if pkw_col else None,
            "pmax_kva": df[pkva_col].apply(_to_float) if pkva_col else None,
        }
    )
    out["grain"] = "monthly"
    out = out.dropna(subset=["period_date"])
    return out


METER_KEY = ["supplier", "prm", "grain", "period_start", "period_end", "cadran", "read_type"]
PMAX_KEY = ["supplier", "prm", "grain", "period_date"]


def _dedup_meter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    tmp = df.copy()
    tmp["prm"] = tmp["prm"].fillna("").astype(str)
    tmp["cadran"] = tmp["cadran"].fillna("").astype(str).str.strip()
    tmp["read_type"] = tmp["read_type"].fillna("").astype(str).str.strip()

    out = (
        tmp.groupby(METER_KEY, dropna=False, as_index=False)
        .agg(
            kwh=("kwh", "sum"),
            index_start=("index_start", "min"),
            index_end=("index_end", "max"),
            source_file=("source_file", "first"),
            source_row=("source_row", "min"),
        )
    )
    return out


def upsert_meter_readings(conn, schema: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    df = _dedup_meter(df)

    sql = f"""
        INSERT INTO {schema}.supplier_meter_readings
          (supplier, prm, grain, period_start, period_end, cadran, kwh, read_type, index_start, index_end, source_file, source_row)
        VALUES %s
        ON CONFLICT (supplier, prm, grain, period_start, period_end, cadran, read_type)
        DO UPDATE SET
          kwh = EXCLUDED.kwh,
          index_start = COALESCE(EXCLUDED.index_start, {schema}.supplier_meter_readings.index_start),
          index_end = COALESCE(EXCLUDED.index_end, {schema}.supplier_meter_readings.index_end),
          source_file = EXCLUDED.source_file,
          source_row = EXCLUDED.source_row,
          updated_at = now()
    """

    rows = []
    for _, r in df.iterrows():
        rows.append(
            (
                r["supplier"],
                r["prm"],
                r["grain"],
                r["period_start"].to_pydatetime() if isinstance(r["period_start"], pd.Timestamp) else r["period_start"],
                r["period_end"].to_pydatetime() if isinstance(r["period_end"], pd.Timestamp) else r["period_end"],
                r["cadran"],
                float(r["kwh"]),
                r["read_type"],
                None if pd.isna(r.get("index_start", None)) else float(r["index_start"]),
                None if pd.isna(r.get("index_end", None)) else float(r["index_end"]),
                r["source_file"],
                int(r["source_row"]),
            )
        )

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def _dedup_pmax(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    tmp = df.copy()
    tmp["prm"] = tmp["prm"].fillna("").astype(str)

    out = (
        tmp.groupby(PMAX_KEY, dropna=False, as_index=False)
        .agg(
            pmax_kw=("pmax_kw", "max"),
            pmax_kva=("pmax_kva", "max"),
            source_file=("source_file", "first"),
            source_row=("source_row", "min"),
        )
    )
    return out


def upsert_power_max(conn, schema: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    df = _dedup_pmax(df)

    sql = f"""
        INSERT INTO {schema}.supplier_power_max
          (supplier, prm, grain, period_date, pmax_kw, pmax_kva, source_file, source_row)
        VALUES %s
        ON CONFLICT (supplier, prm, grain, period_date)
        DO UPDATE SET
          pmax_kw = COALESCE(EXCLUDED.pmax_kw, {schema}.supplier_power_max.pmax_kw),
          pmax_kva = COALESCE(EXCLUDED.pmax_kva, {schema}.supplier_power_max.pmax_kva),
          source_file = EXCLUDED.source_file,
          source_row = EXCLUDED.source_row,
          updated_at = now()
    """

    rows = []
    for _, r in df.iterrows():
        rows.append(
            (
                r["supplier"],
                r["prm"],
                r["grain"],
                r["period_date"],
                None if pd.isna(r.get("pmax_kw", None)) else float(r["pmax_kw"]),
                None if pd.isna(r.get("pmax_kva", None)) else float(r["pmax_kva"]),
                r["source_file"],
                int(r["source_row"]),
            )
        )

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def parse_args():
    p = argparse.ArgumentParser(description="Ingestion Engie exports -> Postgres raw")
    p.add_argument("--supplier", required=True, help="ex: engie")
    p.add_argument("--prm", default="", help="PRM / PDL optionnel")
    p.add_argument("--files", nargs="+", required=True, help="Liste des fichiers à ingérer")

    p.add_argument("--db-host", default=os.getenv("WATTSUP_DB_HOST", "localhost"))
    p.add_argument("--db-port", type=int, default=int(os.getenv("WATTSUP_DB_PORT", "5432")))
    p.add_argument("--db-name", default=os.getenv("WATTSUP_DB_NAME", "wattsup"))
    p.add_argument("--db-user", default=os.getenv("WATTSUP_DB_USER", "energy"))
    p.add_argument("--db-password", default=os.getenv("WATTSUP_DB_PASSWORD", "energy"))
    p.add_argument("--schema", default=os.getenv("WATTSUP_DB_SCHEMA", "raw"))
    return p.parse_args()


def main():
    args = parse_args()

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        schema=args.schema,
    )

    conn = _connect(cfg)
    ensure_schema_and_tables(conn, cfg.schema)

    supplier = args.supplier.strip().lower()
    prm = (args.prm or "").strip()

    processed = 0

    for fpath in args.files:
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Fichier introuvable: {fpath}")

        kind = _guess_kind(fpath)

        try:
            if kind == "monthly_csv":
                df = read_engie_monthly_csv(fpath)
                df["supplier"] = supplier
                df["prm"] = prm
                df["source_file"] = os.path.basename(fpath)
                df["source_row"] = range(1, len(df) + 1)
                n = upsert_meter_readings(conn, cfg.schema, df)
                print(f"[OK] {os.path.basename(fpath)} (monthly) : {n} lignes upsertées")
                processed += 1
                continue

            if kind == "hourly_xlsx":
                df = read_engie_hourly_xlsx(fpath)
                df["supplier"] = supplier
                df["prm"] = prm
                df["source_file"] = os.path.basename(fpath)
                df["source_row"] = range(1, len(df) + 1)
                n = upsert_meter_readings(conn, cfg.schema, df)
                print(f"[OK] {os.path.basename(fpath)} (hourly) : {n} lignes upsertées")
                processed += 1
                continue

            if kind == "pmax_daily_csv":
                df = read_engie_power_daily_csv(fpath)
                df["supplier"] = supplier
                df["prm"] = prm
                df["source_file"] = os.path.basename(fpath)
                df["source_row"] = range(1, len(df) + 1)
                n = upsert_power_max(conn, cfg.schema, df)
                print(f"[OK] {os.path.basename(fpath)} (pmax daily) : {n} lignes upsertées")
                processed += 1
                continue

            if kind == "pmax_monthly_csv":
                df = read_engie_power_monthly_csv(fpath)
                df["supplier"] = supplier
                df["prm"] = prm
                df["source_file"] = os.path.basename(fpath)
                df["source_row"] = range(1, len(df) + 1)
                n = upsert_power_max(conn, cfg.schema, df)
                print(f"[OK] {os.path.basename(fpath)} (pmax monthly) : {n} lignes upsertées")
                processed += 1
                continue

            raise ValueError(f"Type de fichier non reconnu: {fpath}")

        except Exception as e:
            print(f"[ERROR] {os.path.basename(fpath)} : {e}")
            conn.close()
            raise

    conn.close()

    if processed > 0:
        print("[DONE] Ingestion terminée.")
    else:
        print("[WARN] Aucun fichier traité.")


if __name__ == "__main__":
    main()