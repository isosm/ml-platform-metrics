"""
Synthetic ML platform event generator.

Produces six months of realistic telemetry across two event streams:
  github_events — CI runs, PR merges, deployments, incident open/close
  ml_events     — model training runs, deployments, drift detections, retrains

The data follows a narrative arc so the dashboard tells a story:
  Oct–Nov 2025  Healthy baseline
  Dec 2025      team-churn drift rising, CI failures climbing
  Jan 2026      P1 incident on team-reco, high MTTR
  Feb–Mar 2026  team-churn model degraded, slow recovery
  Apr 2026      All teams improving, new model versions shipped
"""

import uuid
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone

random.seed(42)
np.random.seed(42)

START_DATE = datetime(2025, 10, 28, tzinfo=timezone.utc)
END_DATE   = datetime(2026, 4, 28, tzinfo=timezone.utc)
DAYS       = (END_DATE - START_DATE).days

TEAMS = ["team-reco", "team-pricing", "team-churn", "team-search"]

TEAM_MODELS = {
    "team-reco":    "als_reco",
    "team-pricing": "price_elasticity",
    "team-churn":   "churn_pred",
    "team-search":  "search_rank",
}

MODEL_BASE_PRECISION = {
    "als_reco":         0.74,
    "price_elasticity": 0.81,
    "churn_pred":       0.87,
    "search_rank":      0.79,
}


def _phase(day: int) -> str:
    if day < 35:  return "healthy"
    if day < 65:  return "degrading"
    if day < 95:  return "incident"
    if day < 150: return "recovery"
    return "stable"


def _is_weekday(dt: datetime) -> bool:
    return dt.weekday() < 5


def _ts(base_date: datetime, hour: int = 0, minute: int = 0) -> datetime:
    return base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)


def generate_github_events() -> pd.DataFrame:
    rows = []
    open_incidents: dict[str, datetime] = {}

    for day_idx in range(DAYS):
        date  = START_DATE + timedelta(days=day_idx)
        phase = _phase(day_idx)
        is_wd = _is_weekday(date)

        for team in TEAMS:
            # CI runs
            ci_count = random.randint(2, 6) if is_wd else random.randint(0, 2)
            for _ in range(ci_count):
                if phase == "degrading" and team == "team-churn":
                    fail_p = 0.28
                elif phase == "incident" and team in ("team-reco", "team-churn"):
                    fail_p = 0.40
                elif phase == "recovery" and team == "team-churn":
                    fail_p = 0.18
                else:
                    fail_p = 0.07

                rows.append({
                    "event_id":       str(uuid.uuid4()),
                    "team":           team,
                    "event_type":     "ci_run",
                    "event_date":     _ts(date, hour=random.randint(8, 22), minute=random.randint(0, 59)),
                    "lead_time_days": None,
                    "ci_passed":      random.random() > fail_p,
                })

            # PR merges
            if is_wd and random.random() < 0.65:
                if phase in ("incident", "recovery") and team in ("team-reco", "team-churn"):
                    lt = round(np.random.lognormal(1.8, 0.6), 2)
                else:
                    lt = round(np.random.lognormal(0.9, 0.5), 2)

                rows.append({
                    "event_id":       str(uuid.uuid4()),
                    "team":           team,
                    "event_type":     "pr_merged",
                    "event_date":     _ts(date, hour=random.randint(10, 17), minute=random.randint(0, 59)),
                    "lead_time_days": lt,
                    "ci_passed":      None,
                })

            # Deployments
            deploy_p = 0.38 if is_wd else 0.05
            if phase == "incident" and team == "team-reco":
                deploy_p = 0.15
            if phase in ("recovery", "stable"):
                deploy_p = 0.45

            if random.random() < deploy_p:
                rows.append({
                    "event_id":       str(uuid.uuid4()),
                    "team":           team,
                    "event_type":     "deployment",
                    "event_date":     _ts(date, hour=random.randint(9, 18), minute=random.randint(0, 59)),
                    "lead_time_days": None,
                    "ci_passed":      None,
                })

            # Incidents
            incident_p = 0.03
            if phase == "degrading" and team == "team-churn":
                incident_p = 0.08
            if phase == "incident":
                incident_p = 0.14 if team in ("team-reco", "team-churn") else 0.04

            if team not in open_incidents and random.random() < incident_p:
                open_incidents[team] = _ts(date, hour=random.randint(0, 20), minute=random.randint(0, 59))
                rows.append({
                    "event_id":       str(uuid.uuid4()),
                    "team":           team,
                    "event_type":     "incident_opened",
                    "event_date":     open_incidents[team],
                    "lead_time_days": None,
                    "ci_passed":      None,
                })

            if team in open_incidents:
                age_hours = (date - open_incidents[team].replace(tzinfo=None)).total_seconds() / 3600
                resolve_p = 0.15 if phase == "incident" else 0.40

                if age_hours > 48 or random.random() < resolve_p:
                    rows.append({
                        "event_id":       str(uuid.uuid4()),
                        "team":           team,
                        "event_type":     "incident_closed",
                        "event_date":     _ts(date, hour=random.randint(8, 22), minute=random.randint(0, 59)),
                        "lead_time_days": None,
                        "ci_passed":      None,
                    })
                    del open_incidents[team]

    return pd.DataFrame(rows)


def generate_ml_events() -> pd.DataFrame:
    rows = []
    last_training:  dict[str, datetime] = {}
    last_deployment: dict[str, datetime] = {}
    drift_open:     dict[str, datetime] = {}

    for day_idx in range(DAYS):
        date  = START_DATE + timedelta(days=day_idx)
        phase = _phase(day_idx)
        is_wd = _is_weekday(date)

        for team in TEAMS:
            model = TEAM_MODELS[team]
            base_p = MODEL_BASE_PRECISION[model]

            # Training runs
            train_p = 0.18 if is_wd else 0.04
            if phase == "recovery" and team == "team-churn":
                train_p = 0.35
            if phase == "incident" and team == "team-reco":
                train_p = 0.10

            if random.random() < train_p:
                if phase == "degrading" and team == "team-churn":
                    precision = float(np.clip(np.random.normal(base_p - 0.06, 0.02), 0.4, 0.99))
                    psi = float(np.clip(np.random.beta(3, 8) + 0.10, 0.05, 0.40))
                elif phase == "incident":
                    precision = float(np.clip(np.random.normal(base_p - 0.12, 0.03), 0.4, 0.99))
                    psi = float(np.clip(np.random.beta(2, 5) + 0.15, 0.05, 0.45))
                elif phase == "recovery":
                    precision = float(np.clip(np.random.normal(base_p - 0.03, 0.015), 0.4, 0.99))
                    psi = float(np.clip(np.random.beta(5, 15), 0.01, 0.25))
                else:
                    precision = float(np.clip(np.random.normal(base_p, 0.01), 0.4, 0.99))
                    psi = float(np.clip(np.random.beta(8, 30), 0.01, 0.15))

                train_ts = _ts(date, hour=random.randint(1, 6), minute=random.randint(0, 59))
                last_training[model] = train_ts

                rows.append({
                    "event_id":        str(uuid.uuid4()),
                    "team":            team,
                    "model_name":      model,
                    "event_type":      "training_run",
                    "event_date":      train_ts,
                    "psi_score":       round(psi, 4),
                    "precision_at_10": round(precision, 4),
                    "drift_triggered": psi > 0.20,
                })

            # Model deployments
            if model in last_training:
                hours_since = (date - last_training[model]).total_seconds() / 3600
                deploy_p = 0.30 if hours_since > 6 and is_wd else 0.0
                if phase == "incident":
                    deploy_p *= 0.4

                if random.random() < deploy_p and (
                    model not in last_deployment
                    or (date - last_deployment[model]).total_seconds() / 3600 > 12
                ):
                    deploy_ts = last_training[model] + timedelta(hours=random.uniform(4, 24))
                    last_deployment[model] = deploy_ts
                    rows.append({
                        "event_id":        str(uuid.uuid4()),
                        "team":            team,
                        "model_name":      model,
                        "event_type":      "model_deployed",
                        "event_date":      deploy_ts,
                        "psi_score":       None,
                        "precision_at_10": None,
                        "drift_triggered": None,
                    })

            # Drift detection
            drift_p = 0.02
            if phase == "degrading" and team == "team-churn":
                drift_p = 0.12
            if phase == "incident":
                drift_p = 0.18 if team in ("team-reco", "team-churn") else 0.04

            if model not in drift_open and random.random() < drift_p:
                psi_val = float(np.clip(np.random.uniform(0.21, 0.50), 0.21, 0.55))
                drift_ts = _ts(date, hour=random.randint(6, 20), minute=random.randint(0, 59))
                drift_open[model] = drift_ts
                rows.append({
                    "event_id":        str(uuid.uuid4()),
                    "team":            team,
                    "model_name":      model,
                    "event_type":      "drift_detected",
                    "event_date":      drift_ts,
                    "psi_score":       round(psi_val, 4),
                    "precision_at_10": None,
                    "drift_triggered": True,
                })

            # Retrain triggered
            if model in drift_open:
                age_h = (date - drift_open[model]).total_seconds() / 3600
                retrain_p = 0.12 if phase == "incident" else 0.35

                if age_h > 72 or random.random() < retrain_p:
                    rows.append({
                        "event_id":        str(uuid.uuid4()),
                        "team":            team,
                        "model_name":      model,
                        "event_type":      "retrain_triggered",
                        "event_date":      _ts(date, hour=random.randint(8, 16), minute=random.randint(0, 59)),
                        "psi_score":       None,
                        "precision_at_10": None,
                        "drift_triggered": None,
                    })
                    del drift_open[model]

    return pd.DataFrame(rows)


def generate_all() -> dict[str, pd.DataFrame]:
    print("Generating GitHub events...")
    github = generate_github_events()

    print("Generating ML events...")
    ml = generate_ml_events()

    print(f"  github_events:  {len(github):>6,} rows")
    print(f"  ml_events:      {len(ml):>6,} rows")

    return {
        "github_events": github,
        "ml_events":     ml,
    }
