"""Core telemetry tag, alarm rule, and OEE domain models."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class QualityFlag(str, Enum):
    GOOD = "GOOD"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    STALE_FLATLINE = "STALE_FLATLINE"
    BAD_TIMESTAMP = "BAD_TIMESTAMP"


class AlarmSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL_SAFETY = "CRITICAL_SAFETY"


@dataclass
class TagSpec:
    tag_id: str
    unit: str
    min_valid: float
    max_valid: float
    high_limit: float
    low_limit: float
    deadband: float = 1.0
    max_stale_samples: int = 3
    severity: AlarmSeverity = AlarmSeverity.WARNING


@dataclass
class TelemetrySample:
    tag_id: str
    timestamp: datetime
    value: float


@dataclass
class AlarmEvent:
    tag_id: str
    timestamp: datetime
    value: float
    severity: AlarmSeverity
    state: str  # ACTIVE | CLEARED | SUPPRESSED_CHATTER
    escalation_group: str
    detail: str = ""
