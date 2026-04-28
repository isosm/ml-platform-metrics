---
title: ML Delivery Performance
---

# ML Delivery Performance

Four ML-specific DORA metrics — the unique angle of this project.
Applies software delivery thinking to ML model lifecycle management.

---

## Model Precision@10 Trend

```sql precision_trend
select
    date_day,
    team,
    avg_precision_at_10
from fct_platform_health_daily
where avg_precision_at_10 > 0
order by date_day, team
```

<LineChart
    data={precision_trend}
    x=date_day
    y=avg_precision_at_10
    series=team
    title="Model Precision@10 — higher is better"
    yMin=0.5
    yMax=1.0
/>

> **Interpretation:** The Dec 2025–Jan 2026 degradation on team-churn
> reflects real model drift (PSI > 0.20) caused by feature staleness.
> This is the same pattern MLOps teams face in production: drift is gradual,
> not sudden.

---

## Drift Failure Rate

```sql drift
select
    date_day,
    team,
    round(drift_failure_rate_7d_avg * 100, 2) as drift_rate_pct
from fct_platform_health_daily
order by date_day, team
```

<LineChart
    data={drift}
    x=date_day
    y=drift_rate_pct
    series=team
    title="Drift Failure Rate % (7-day rolling) — target: <5%"
/>

> **PSI thresholds:** <0.10 stable · 0.10–0.20 warning · >0.20 critical (retrain trigger)

---

## Model Training & Deployment Frequency

```sql ml_deploy
select
    date_day,
    team,
    training_run_count,
    model_deployment_count
from fct_platform_health_daily
order by date_day, team
```

<BarChart
    data={ml_deploy}
    x=date_day
    y=training_run_count
    series=team
    title="Training Runs per Day"
/>

---

## PSI Distribution — Latest 30 Days

```sql psi_dist
select
    team,
    model_name,
    psi_severity,
    count(*) as run_count,
    round(avg(psi_score), 3) as avg_psi
from fct_ml_runs
where run_day >= date_sub(current_date(), interval 30 day)
group by 1, 2, 3
order by avg_psi desc
```

<DataTable data={psi_dist} />
