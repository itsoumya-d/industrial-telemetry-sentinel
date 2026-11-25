from datetime import datetime, timezone
from telemetry_sentinel.models import AlarmSeverity, QualityFlag, TagSpec, TelemetrySample


def test_tag_spec_and_sample_models():
    spec = TagSpec("PUMP-01.TEMP", "C", 0.0, 200.0, 95.0, 10.0, severity=AlarmSeverity.CRITICAL_SAFETY)
    sample = TelemetrySample("PUMP-01.TEMP", datetime(2026, 11, 25, tzinfo=timezone.utc), 42.5)
    assert spec.tag_id == sample.tag_id
    assert QualityFlag.GOOD.value == "GOOD"
