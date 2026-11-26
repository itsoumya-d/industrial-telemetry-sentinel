from datetime import datetime, timedelta, timezone
import pytest

from telemetry_sentinel.engine import DataQualityValidator
from telemetry_sentinel.models import AlarmSeverity, QualityFlag, TagSpec, TelemetrySample
from telemetry_sentinel.pipeline import (
    DeadLetterRecord,
    QualityManifest,
    ResilientPipelineExecutor,
    StagedPartitionBatch,
    TelemetryBatchPipeline,
)


def _spec() -> dict[str, TagSpec]:
    return {
        "SENSOR.VIB": TagSpec(
            tag_id="SENSOR.VIB",
            unit="mm/s",
            min_valid=0.0,
            max_valid=100.0,
            high_limit=40.0,
            low_limit=1.0,
            deadband=2.0,
            max_stale_samples=3,
            severity=AlarmSeverity.WARNING,
        )
    }


def test_batch_partitioning_and_hive_keys():
    validator = DataQualityValidator(_spec())
    pipeline = TelemetryBatchPipeline(validator)

    t1 = datetime(2026, 11, 26, 8, 15, tzinfo=timezone.utc)
    t2 = datetime(2026, 11, 26, 8, 30, tzinfo=timezone.utc)
    t3 = datetime(2026, 11, 26, 9, 10, tzinfo=timezone.utc)

    samples = [
        TelemetrySample("SENSOR.VIB", t1, 10.0),
        TelemetrySample("SENSOR.VIB", t2, 12.0),
        TelemetrySample("SENSOR.VIB", t3, 15.0),
    ]

    watermark = datetime(2026, 11, 26, 9, 30, tzinfo=timezone.utc)
    staged, dlq, manifest = pipeline.process_batch(samples, watermark=watermark)

    assert len(dlq) == 0
    assert manifest.total_processed == 3
    assert manifest.valid_count == 3
    assert manifest.completeness_pct == 100.0
    assert manifest.freshness_lag_seconds == 20 * 60  # 9:30 - 9:10 = 20 mins

    assert len(staged) == 2
    assert staged[0].partition_key == "year=2026/month=11/day=26/hour=08"
    assert staged[0].count == 2
    assert staged[1].partition_key == "year=2026/month=11/day=26/hour=09"
    assert staged[1].count == 1


def test_batch_dlq_routing_and_quality_manifest():
    validator = DataQualityValidator(_spec())
    pipeline = TelemetryBatchPipeline(validator)

    t0 = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)
    samples = [
        TelemetrySample("SENSOR.VIB", t0, 20.0),
        TelemetrySample("SENSOR.VIB", t0 + timedelta(seconds=10), 999.0),  # OUT_OF_RANGE
    ]

    staged, dlq, manifest = pipeline.process_batch(samples, watermark=t0 + timedelta(seconds=15))

    assert len(staged) == 1
    assert staged[0].count == 1
    assert len(dlq) == 1
    assert dlq[0].flag == QualityFlag.OUT_OF_RANGE
    assert manifest.total_processed == 2
    assert manifest.valid_count == 1
    assert manifest.dlq_count == 1
    assert manifest.completeness_pct == 50.0


def test_resilient_executor_recovers_from_transient_failures():
    executor = ResilientPipelineExecutor(max_retries=3, base_delay=0.001, backoff_factor=1.5)
    call_count = 0

    def flaky_write():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("Temporary S3 / storage timeout")
        return "SUCCESS"

    result = executor.execute_with_retry(flaky_write, "s3_stage_upload")
    assert result == "SUCCESS"
    assert call_count == 3


def test_resilient_executor_raises_on_persistent_failure():
    executor = ResilientPipelineExecutor(max_retries=2, base_delay=0.001)

    def always_fails():
        raise ValueError("Invalid schema")

    with pytest.raises(RuntimeError, match="Pipeline failing_step failed after 2 retries"):
        executor.execute_with_retry(always_fails, "failing_step")
