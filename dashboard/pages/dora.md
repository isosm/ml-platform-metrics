---
title: DORA Metrics — Software Delivery
---

# DORA Metrics

The four standard DORA metrics measuring software delivery performance.

---

## Deployment Frequency

```sql deploy_freq
select
    date_day,
    team,
    deployments_last_7d as deployments_7d_rolling
from fct_platform_health_daily
order by date_day, team
```

<LineChart
    data={deploy_freq}
    x=date_day
    y=deployments_7d_rolling
    series=team
    title="Deployments (7-day rolling sum)"
/>

---

## Lead Time for Change

```sql lead_time
select
    date_day,
    team,
    avg_lead_time_days,
    p50_lead_time_days,
    p95_lead_time_days
from fct_platform_health_daily
where avg_lead_time_days is not null
order by date_day, team
```

<LineChart
    data={lead_time}
    x=date_day
    y=p50_lead_time_days
    series=team
    title="Lead Time p50 (days) — lower is better"
/>

---

## Change Failure Rate

```sql cfr
select
    date_day,
    team,
    round(ci_failure_rate_7d_avg * 100, 2) as failure_rate_pct
from fct_platform_health_daily
order by date_day, team
```

<LineChart
    data={cfr}
    x=date_day
    y=failure_rate_pct
    series=team
    title="CI Failure Rate % (7-day rolling avg) — DORA Elite: <15%"
/>

---

## MTTR (Mean Time to Recover)

```sql mttr
select
    date_day,
    team,
    avg_mttr_hours
from fct_platform_health_daily
where incident_count > 0
order by date_day, team
```

<BarChart
    data={mttr}
    x=date_day
    y=avg_mttr_hours
    series=team
    title="MTTR (hours) — DORA Elite: <1h"
/>
