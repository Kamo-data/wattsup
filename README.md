[![CI](https://github.com/Kamo-data/wattsup/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Kamo-data/wattsup/actions/workflows/ci.yml)
[![Docs](https://github.com/Kamo-data/wattsup/actions/workflows/pages.yml/badge.svg?branch=main)](https://github.com/Kamo-data/wattsup/actions/workflows/pages.yml)


# WattsUp — Suivi conso électricité (CSV fournisseur → Postgres → dbt → Metabase)

WattsUp est une mini-stack data locale destinée à **ingérer des relevés d’électricité** (CSV fournisseur), les **historiser dans PostgreSQL**, les **modéliser avec dbt** et les **visualiser dans Metabase**.

Objectif : disposer d’un pipeline simple, rejouable et maintenable, avec des contrôles de qualité de données adaptés aux cas réels (données manquantes, incohérences, reset compteur, etc.).

![Dashboard WattsUp](docs/screenshots/Dashboard.png)

---

## 1) Vue d’ensemble

### Ce que fait le pipeline
- Charge un export CSV fournisseur (HP/HC) dans `raw.supplier_meter_readings`
- Standardise les types et le format dans `analytics.stg_supplier_meter_readings`
- Calcule des faits et agrégats pour l’analyse :
  - `analytics.fct_energy_period`
  - `analytics.agg_energy_calendar_month_est`
- Applique des **tests dbt** (staging + règles métier)
- Expose les tables à Metabase

### Stack
- **Python** : ingestion CSV + upsert (idempotent)
- **PostgreSQL** : stockage (raw + analytics)
- **dbt** : staging + marts + tests
- **Metabase** : dashboard / exploration
- **Docker Compose** : reproductibilité

---

## 2) Architecture (simplifiée)

**CSV fournisseur**  
→ ingestion Python  
→ `raw.supplier_meter_readings`

`raw.supplier_meter_readings`  
→ dbt staging  
→ `analytics.stg_supplier_meter_readings`

`analytics.stg_supplier_meter_readings`  
→ dbt marts  
→ `analytics.fct_energy_period` (consommation par période)  
→ `analytics.agg_energy_calendar_month_est` (agrégat mensuel estimé)

→ Metabase (dashboards)

---

## 3) Modèles dbt

### `analytics.stg_supplier_meter_readings` (staging)
Normalisation des relevés fournisseur :
- typage (dates / numériques)
- cadran normalisé (`HP` / `HC`)
- préparation du grain utilisé en marts

### `analytics.fct_energy_period` (mart)
Consommation et estimation de coût par période (période = intervalle [period_start, period_end]) :
- `kwh_hp`, `kwh_hc`, `kwh_total`
- `period_days`, `kwh_per_day_est`
- tarification HP/HC jointe par date d’effet (`config.tariff_hp_hc`)
- `has_negative_kwh` (flag qualité)
- `cost_est_eur` (protégé : `NULL` si la période est invalide)

### `analytics.agg_energy_calendar_month_est` (mart)
Agrégation mensuelle (estimation) :
- `kwh_hp`, `kwh_hc`, `kwh_total`
- `days_covered`
- `kwh_per_day_est`
- `cost_est_eur`

---

## 4) Data Quality / Tests

Le pipeline inclut des contrôles pour éviter d’alimenter Metabase avec des valeurs incohérentes.

### 4.1 Tests “génériques” (schema tests)
Définis dans les `schema.yml`.

**Sur `stg_supplier_meter_readings`**
- `not_null` : `period_start`, `period_end`, `cadran`, `kwh`
- `accepted_values` : `cadran` ∈ {`HP`, `HC`}

**Sur `fct_energy_period`**
- `not_null` : `period_start`, `period_end`

### 4.2 Tests SQL “métier” (custom tests)
Fichiers dans `dbt/wattsup/tests/`.

1) **Unicité au bon grain**
- règle : 1 relevé max par `period_start` et `cadran`
- fichier : `test_unique_stg_period_start_cadran.sql`

2) **Pas de consommation négative**
- règle : `kwh_hp`, `kwh_hc`, `kwh_total` doivent être ≥ 0
- fichier : `test_no_negative_kwh_period.sql`

3) **Coût non calculé si données invalides**
- règle : si `has_negative_kwh = true` alors `cost_est_eur` doit être `NULL`
- fichier : `test_cost_null_when_negative_kwh.sql`

### 4.3 Pourquoi le flag `has_negative_kwh` ?
Certaines sources (CSV fournisseur) peuvent générer des deltas négatifs (ex : reset compteur, correction fournisseur).
- le modèle conserve le signal via `has_negative_kwh`
- le coût est neutralisé (`NULL`) tant que le cas n’est pas explicitement traité (data cleaning / règles de correction)

---

## 5) Démarrage rapide

### Pré-requis
- Docker Desktop
- Git
- (optionnel) Python 3.10+ si exécution ingestion en local

### 5.1 Lancer la stack
À la racine du repo :
```bash
docker compose up -d
docker compose ps
```

### 5.2 Ingestion d’un CSV (local)

Déposer un CSV dans `data/raw/` (ex : `sample_releve_mensuelles.csv`).

Lancer le pipeline complet (depuis la racine du repo) :
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1
```
> Note : `run_all.ps1` exécute typiquement  
> installation deps → ingestion → dbt build/run → dbt test

### 5.3 Exécuter dbt dans Docker (recommandé)
```bash
docker compose run --rm dbt build
docker compose run --rm dbt test
```

## 6) Vérifications rapides (PostgreSQL)
Compter les relevés raw :
```bash
docker compose exec postgres psql -U energy -d wattsup -c "select count(*) from raw.supplier_meter_readings;"
```
Afficher les agrégats mensuels :
```bash
docker compose exec postgres psql -U energy -d wattsup -c "select * from analytics.agg_energy_calendar_month_est order by month desc limit 12;"
```
Inspecter le schéma du mart :
```bash
docker compose exec postgres psql -U energy -d wattsup -c "\d+ analytics.fct_energy_period"
```

## 7) Metabase
Ouvrir Metabase :

<http://localhost:3001>

Connexion PostgreSQL depuis Metabase :
- Host : postgres
- Port : 5432
- Database : wattsup
- User : energy
- Password : energy

## 8) Structure du repo
- ingest/ : ingestion CSV (Python)
- postgres/init.sql : init DB (schemas/tables)
- dbt/wattsup/ : projet dbt (models + tests)
- dbt/wattsup/tests/ : tests SQL custom
- docs/screenshots/ : captures Metabase
- docker-compose.yml : stack Postgres + dbt + Metabase
- scripts/run_all.ps1 : pipeline local (ingest + dbt)

## 9) Runbook / Troubleshooting
### 9.1 Metabase (port déjà pris)
Metabase est mappé en 3001:3000.
Modifier le port hôte dans `docker-compose.yml` (ex : 3002:3000), puis relancer :
```bash
docker compose down
docker compose up -d
```

### 9.2 dbt : erreur de compilation sur accepted_values
Cause fréquente : versions dbt différentes entre l’environnement local et l’image Docker, ou syntaxe de test non compatible avec la version exécutée.
Actions recommandées :
- privilégier l’exécution dbt via Docker Compose (limite les écarts)
- vérifier l’alignement des versions dbt (core + adapter)
Diagnostic :
```bash
dbt --version
docker compose run --rm dbt --version
```
Rappel de compatibilité (résumé) :
- dbt plus ancien (ex : 1.9) : accepted_values attend généralement values: [...]
- dbt plus récent (ex : 1.11+) : accepted_values recommande arguments: { values: [...] }

### 9.3 dbt : erreurs SQL (ex : relation does not exist)
Causes fréquentes :
- alias/CTE non définis (ex : from tar sans CTE tar)
- renommage d’un CTE sans propagation
- dépendance non construite

Isoler un modèle :
```bash
docker compose run --rm dbt build --select fct_energy_period
```

### 9.4 PowerShell : chemin de script introuvable
Le script `scripts/run_all.ps1` se trouve à la racine du repo.

Si l’exécution se fait depuis `dbt/wattsup/`, revenir à la racine avant de lancer :
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1
```

### 9.5 Docker Desktop / virtualisation
Si Docker ne démarre pas :
- vérifier WSL2 / virtualisation BIOS / features Windows
- redémarrer Docker Desktop

## 10) Évolutions possibles
- support multi-énergies (gaz / eau) + multi-compteurs
- historisation plus complète des tarifs (HP/HC) + contrôles de couverture tarifaire
- règles de correction “reset compteur” (reconstruction de séries, interpolation, etc.)
- orchestration planifiée (Task Scheduler / cron) + logs structurés
- docs dbt + publication (dbt docs) et validation CI

## Auteur
David Limoisin — Data Engineer
Projet orienté industrialisation, SQL/ELT, data quality et reproductibilité.

<!-- ci smoke -->

