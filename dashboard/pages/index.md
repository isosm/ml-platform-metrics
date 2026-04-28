---
title: Platform Health — DORA for ML
---

# ML Platform Health Dashboard

Composite health score across 4 platform teams, combining 8 DORA-for-ML metrics.
Data refreshes daily at 06:00 UTC via Dagster.

```sql latest_day
select max(date_day) as latest_day
from fct_platform_health_daily
```

**Last updated:** <Value data={latest_day} column=latest_day />

---

## Health Scores — Today

```sql health_today
select
    team,
    health_score,
    health_tier,
    deployments_last_7d,
    round(ci_failure_rate_7d_avg * 100, 1) as ci_failure_rate_pct,
    round(drift_failure_rate_7d_avg * 100, 1) as drift_failure_rate_pct,
    round(avg_mttr_hours, 1) as avg_mttr_hours
from fct_platform_health_daily
where date_day = (select max(date_day) from fct_platform_health_daily)
order by health_score asc
```

<DataTable data={health_today} />

---

## Health Score Trend — All Teams

```sql health_trend
select
    date_day,
    team,
    health_score,
    health_tier
from fct_platform_health_daily
order by date_day, team
```

<LineChart
    data={health_trend}
    x=date_day
    y=health_score
    series=team
    title="Platform Health Score (0–100)"
    yMin=0
    yMax=100
/>

> **Reading the chart:** Below 70 = critical alert. Above 90 = Elite DORA tier.
> The dip in Jan–Feb 2026 correlates with a P1 incident on team-reco and
> simultaneous model drift on team-churn.
