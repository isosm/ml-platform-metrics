# ML Platform Metrics — DORA for ML

[![dbt](https://img.shields.io/badge/dbt-1.11-orange)](https://www.getdbt.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue)](https://cloud.google.com/bigquery)
[![Dagster](https://img.shields.io/badge/Dagster-1.7-purple)](https://dagster.io)

**GitHub:** https://github.com/isosm/ml-platform-metrics

A production-grade analytics engineering project that applies DORA (DevOps Research and Assessment) methodology to ML platform teams. Measures both software delivery performance and ML lifecycle delivery using a unified metric framework.

**Live data:** 6 months of synthetic platform telemetry across 4 teams, loaded to BigQuery and transformed through a full dbt pipeline.

---

## The Problem

Most ML platform teams can't answer a simple question: *how healthy is our platform?*

They know if models are trained. They don't know if they're delivering with speed, quality, and recoverability. DORA metrics solve this for software teams — but no standard exists for ML delivery.

This project builds that standard.

---

## Architecture

```
Synthetic Generator (Python)
  github_events   — CI runs, PR merges, deployments, incidents
  ml_events       — training runs, model deployments, drift signals
        │
        ▼
BigQuery: raw layer  (immutable source data)
        │
        ▼
dbt: staging layer   (views — clean, typed, renamed)
        │
        ▼
dbt: intermediate    (views — daily DORA + ML metrics per team)
        │
        ▼
dbt: marts layer     (tables — facts, dims, composite health score)
  + MetricFlow semantic layer (8 metrics defined as code)
  + dbt contracts on fct_platform_health_daily
  + Elementary data observability
        │
        ▼
Dagster orchestration  (daily 06:00 UTC, asset-based lineage)
        │
        ▼
Looker Studio dashboard (health scores, DORA trends, drift analysis)
```

---

## The 8 DORA-for-ML Metrics

| # | Metric | Type | Definition |
|---|--------|------|-----------|
| 1 | Deployment Frequency | DORA | Software deployments per team per day |
| 2 | Lead Time for Change | DORA | PR created → merged (days) |
| 3 | Change Failure Rate | DORA | Failed CI runs / total CI runs |
| 4 | MTTR | DORA | incident_opened → incident_closed (hours) |
| 5 | Model Training Frequency | ML | Training runs per team per month |
| 6 | Model Deployment Frequency | ML | Model versions shipped per team |
| 7 | Drift Failure Rate | ML | Training runs with PSI > 0.20 |
| 8 | Platform Health Score | Composite | Weighted 0–100 score across all dimensions |

**PSI thresholds:** < 0.10 stable · 0.10–0.20 warning · > 0.20 critical (retrain trigger)

---

## Stack

| Layer | Tool | Why |
|-------|------|-----|
| Cloud | GCP + BigQuery | Industry standard for analytics at scale |
| Transformation | dbt-bigquery 1.11 | SQL-first, testable, version-controlled models |
| Metrics | MetricFlow (dbt Semantic Layer) | Single source of truth for KPI definitions |
| Observability | Elementary | Anomaly detection beyond basic dbt tests |
| Orchestration | Dagster | Asset-based lineage, not just task scheduling |
| CI/CD | GitHub Actions slim CI | `state:modified+` + `--defer` against prod manifest |
| Dashboard | Looker Studio | Direct BigQuery integration, zero infra |

---

## Key Design Decisions

**Schema naming macro** — dev models land in `dbt_dev_marts`, prod in `marts`. Dev and prod never share datasets.

**dbt contracts** on `fct_platform_health_daily` — any schema change requires an explicit contract bump. Protects downstream consumers.

**Slim CI** — GitHub Actions only builds `state:modified+` models and defers unchanged upstream refs to prod. Cuts CI time ~80% on large projects.

**Pre-calculated rates in marts** — `ci_failure_rate` and `drift_failure_rate` are computed in SQL, not in MetricFlow ratio metrics. Single source of truth.

---

## Project Structure

```
ml-platform-metrics/
├── generator/              # Synthetic data generator
│   ├── generate.py         # DORA + ML events, 4 teams, 6 months
│   └── upload.py           # BigQuery loader with explicit schemas
├── dbt/
│   ├── macros/
│   │   └── generate_schema_name.sql  # dev/prod isolation
│   ├── models/
│   │   ├── staging/        # views — clean + type
│   │   ├── intermediate/   # views — daily aggregations + rolling windows
│   │   └── marts/          # tables — facts, dims, health score, MetricFlow
│   ├── packages.yml        # dbt_utils + Elementary
│   └── profiles.yml        # dev / ci / prod targets
├── orchestration/          # Dagster
│   ├── assets/             # raw_data + platform_report assets
│   └── definitions.py      # Definitions, schedule, resources
├── dashboard/              # Looker Studio + Evidence pages
│   └── pages/              # Markdown SQL pages (Evidence)
└── .github/workflows/
    └── dbt-ci.yml          # Slim CI with state:modified+ and --defer
```

---

## Running Locally

```bash
# 1. Python environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Authenticate to GCP
gcloud auth application-default login

# 3. Generate synthetic data → BigQuery
python -m generator

# 4. Build dbt models (dev)
cd dbt && dbt deps && dbt build --target dev

# 5. Explore dbt docs
dbt docs generate && dbt docs serve

# 6. Start Dagster UI
cd .. && dagster dev -f orchestration/definitions.py
```

---

## Sample Results

**Platform health — January 2026 (critical period)**

| Team | Health Score | CI Failure | Drift Rate | MTTR |
|------|-------------|-----------|-----------|------|
| team-reco | 69 | 20.5% | 16.9% | 65h |
| team-churn | 68 | 18.3% | 24.1% | 71h |

All teams classified as **DORA Medium** performers. Primary bottleneck: MTTR averaging 43–83 hours vs. DORA Elite target of < 1 hour.

---

## Author

Ishaq Osman — Senior Analytics Engineer  
Portfolio project for ML platform engineering role.