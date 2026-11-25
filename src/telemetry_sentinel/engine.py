"""Data quality validation, chatter-suppressed alarm sentinel, and OEE analytics."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List

from telemetry_sentinel.models import AlarmEvent, AlarmSeverity, QualityFlag, TagSpec, TelemetrySample


@dataclass
class OeeMetrics:
    availability_pct: float
    performance_pct: float
    quality_pct: float
    oee_pct: float
    unplanned_downtime_mins: float
    downtime_breakdown: Dict[str, float]


class DataQualityValidator:
    """Validates OT telemetry tags for out-of-range spikes, flatlines, and timestamp drift."""

    def __init__(self, specs: Dict[str, TagSpec]) -> None:
        self.specs = specs
        self._last_ts: Dict[str, datetime] = {}
        self._last_val: Dict[str, float] = {}
        self._repeat_count: Dict[str, int] = {}

    def evaluate(self, sample: TelemetrySample) -> QualityFlag:
        spec = self.specs[sample.tag_id]
        prev_ts = self._last_ts.get(sample.tag_id)
        if prev_ts is not None and sample.timestamp <= prev_ts:
            return QualityFlag.BAD_TIMESTAMP
        self._last_ts[sample.tag_id] = sample.timestamp

        if not (spec.min_valid <= sample.value <= spec.max_valid):
            return QualityFlag.OUT_OF_RANGE

        prev_val = self._last_val.get(sample.tag_id)
        if prev_val is not None and sample.value == prev_val:
            count = self._repeat_count.get(sample.tag_id, 1) + 1
            self._repeat_count[sample.tag_id] = count
            if count >= spec.max_stale_samples:
                return QualityFlag.STALE_FLATLINE
        else:
            self._last_val[sample.tag_id] = sample.value
            self._repeat_count[sample.tag_id] = 1
        return QualityFlag.GOOD


class AlarmSentinel:
    """Evaluates OT alarm thresholds with hysteresis deadbands and chatter suppression."""

    def __init__(self, specs: Dict[str, TagSpec], chatter_window_sec: int = 120, chatter_limit: int = 3) -> None:
        self.specs = specs
        self.chatter_window = timedelta(seconds=chatter_window_sec)
        self.chatter_limit = chatter_limit
        self._active: Dict[str, bool] = {}
        self._transitions: Dict[str, List[datetime]] = {}

    def _escalation_for(self, sev: AlarmSeverity) -> str:
        if sev == AlarmSeverity.CRITICAL_SAFETY:
            return "Process-Safety-Lead"
        if sev == AlarmSeverity.WARNING:
            return "Shift-Reliability-Eng"
        return "Control-Room-Log"

    def process(self, sample: TelemetrySample, quality: QualityFlag = QualityFlag.GOOD) -> AlarmEvent | None:
        if quality != QualityFlag.GOOD:
            return None
        spec = self.specs[sample.tag_id]
        currently_active = self._active.get(sample.tag_id, False)
        breached = sample.value >= spec.high_limit or sample.value <= spec.low_limit
        cleared = (spec.low_limit + spec.deadband) <= sample.value <= (spec.high_limit - spec.deadband)

        if not currently_active and breached:
            hist = [t for t in self._transitions.get(sample.tag_id, []) if sample.timestamp - t <= self.chatter_window]
            hist.append(sample.timestamp)
            self._transitions[sample.tag_id] = hist
            if len(hist) >= self.chatter_limit:
                return AlarmEvent(
                    sample.tag_id,
                    sample.timestamp,
                    sample.value,
                    spec.severity,
                    "SUPPRESSED_CHATTER",
                    self._escalation_for(spec.severity),
                    f"Chattering suppressed ({len(hist)} transitions in {int(self.chatter_window.total_seconds())}s)",
                )
            self._active[sample.tag_id] = True
            return AlarmEvent(
                sample.tag_id,
                sample.timestamp,
                sample.value,
                spec.severity,
                "ACTIVE",
                self._escalation_for(spec.severity),
                f"Tag {sample.tag_id}={sample.value}{spec.unit} exceeded bounds",
            )

        if currently_active and cleared:
            self._active[sample.tag_id] = False
            return AlarmEvent(
                sample.tag_id,
                sample.timestamp,
                sample.value,
                spec.severity,
                "CLEARED",
                self._escalation_for(spec.severity),
                f"Tag {sample.tag_id}={sample.value}{spec.unit} returned within deadband",
            )
        return None


def compute_oee(
    planned_production_mins: float,
    downtime_events_mins: Dict[str, float],
    ideal_cycle_time_mins: float,
    total_units: int,
    good_units: int,
) -> OeeMetrics:
    """Calculate OEE (Availability x Performance x Quality) and unplanned downtime attribution."""
    unplanned = sum(max(0.0, v) for v in downtime_events_mins.values())
    run_time = max(0.0, planned_production_mins - unplanned)
    availability = (run_time / planned_production_mins) if planned_production_mins > 0 else 0.0
    performance = min(1.0, (ideal_cycle_time_mins * total_units) / run_time) if run_time > 0 else 0.0
    quality = (good_units / total_units) if total_units > 0 else 0.0
    oee = availability * performance * quality
    return OeeMetrics(
        availability_pct=round(availability * 100.0, 2),
        performance_pct=round(performance * 100.0, 2),
        quality_pct=round(quality * 100.0, 2),
        oee_pct=round(oee * 100.0, 2),
        unplanned_downtime_mins=round(unplanned, 2),
        downtime_breakdown=dict(downtime_events_mins),
    )
