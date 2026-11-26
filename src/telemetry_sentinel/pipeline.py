"""Resilient batch data pipeline, dead-letter routing, and lakehouse partition stage."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple, TypeVar

from telemetry_sentinel.engine import DataQualityValidator
from telemetry_sentinel.models import QualityFlag, TelemetrySample

T = TypeVar("T")


@dataclass
class DeadLetterRecord:
    sample: TelemetrySample
    flag: QualityFlag
    reason: str
    rejected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class StagedPartitionBatch:
    partition_key: str  # e.g. year=YYYY/month=MM/day=DD/hour=HH
    records: List[TelemetrySample]
    count: int = field(init=False)

    def __post_init__(self) -> None:
        self.count = len(self.records)


@dataclass
class QualityManifest:
    total_processed: int
    valid_count: int
    dlq_count: int
    completeness_pct: float
    freshness_lag_seconds: float
    staged_partitions: Dict[str, int]
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class TelemetryBatchPipeline:
    """Batch staging pipeline with data validation, temporal partitioning, and DLQ routing."""

    def __init__(self, validator: DataQualityValidator) -> None:
        self.validator = validator

    @staticmethod
    def compute_partition_key(ts: datetime) -> str:
        """Generates S3/lakehouse standard Hive-style partition path."""
        utc_ts = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        return f"year={utc_ts.year:04d}/month={utc_ts.month:02d}/day={utc_ts.day:02d}/hour={utc_ts.hour:02d}"

    def process_batch(
        self, samples: List[TelemetrySample], watermark: Optional[datetime] = None
    ) -> Tuple[List[StagedPartitionBatch], List[DeadLetterRecord], QualityManifest]:
        """Validates raw samples, partitions clean records, and emits DLQ + manifest."""
        now = watermark or datetime.now(timezone.utc)
        partition_map: Dict[str, List[TelemetrySample]] = {}
        dlq_records: List[DeadLetterRecord] = []
        valid_count = 0
        latest_sample_ts: Optional[datetime] = None

        for sample in samples:
            flag = self.validator.evaluate(sample)
            if flag == QualityFlag.GOOD:
                valid_count += 1
                pkey = self.compute_partition_key(sample.timestamp)
                partition_map.setdefault(pkey, []).append(sample)
                if latest_sample_ts is None or sample.timestamp > latest_sample_ts:
                    latest_sample_ts = sample.timestamp
            else:
                dlq_records.append(
                    DeadLetterRecord(
                        sample=sample,
                        flag=flag,
                        reason=f"Telemetry sample failed validation with flag {flag.value}",
                        rejected_at=now,
                    )
                )

        total = len(samples)
        completeness = (valid_count / total * 100.0) if total > 0 else 100.0
        freshness_lag = 0.0
        if latest_sample_ts is not None:
            sample_utc = (
                latest_sample_ts.astimezone(timezone.utc)
                if latest_sample_ts.tzinfo
                else latest_sample_ts.replace(tzinfo=timezone.utc)
            )
            freshness_lag = max(0.0, (now - sample_utc).total_seconds())

        staged_batches = [
            StagedPartitionBatch(partition_key=k, records=v) for k, v in sorted(partition_map.items())
        ]
        manifest = QualityManifest(
            total_processed=total,
            valid_count=valid_count,
            dlq_count=len(dlq_records),
            completeness_pct=round(completeness, 2),
            freshness_lag_seconds=round(freshness_lag, 2),
            staged_partitions={b.partition_key: b.count for b in staged_batches},
            processed_at=now,
        )
        return staged_batches, dlq_records, manifest


class ResilientPipelineExecutor:
    """Executes data operations with retry envelope and exponential backoff."""

    def __init__(self, max_retries: int = 3, base_delay: float = 0.01, backoff_factor: float = 2.0) -> None:
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor

    def execute_with_retry(self, operation: Callable[[], T], operation_name: str = "operation") -> T:
        """Executes operation with retry backoff. Raises RuntimeError on final failure."""
        attempts = 0
        delay = self.base_delay
        while True:
            try:
                attempts += 1
                return operation()
            except Exception as exc:
                if attempts > self.max_retries:
                    raise RuntimeError(
                        f"Pipeline {operation_name} failed after {self.max_retries} retries: {exc}"
                    ) from exc
                time.sleep(delay)
                delay *= self.backoff_factor
