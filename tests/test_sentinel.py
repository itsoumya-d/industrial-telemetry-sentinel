from datetime import datetime, timedelta, timezone

from telemetry_sentinel.engine import AlarmSentinel, DataQualityValidator, compute_oee
from telemetry_sentinel.models import AlarmSeverity, QualityFlag, TagSpec, TelemetrySample


def _spec() -> dict[str, TagSpec]:
    return {
        "TURBINE.TEMP": TagSpec(
            tag_id="TURBINE.TEMP",
            unit="C",
            min_valid=-20.0,
            max_valid=250.0,
            high_limit=100.0,
            low_limit=10.0,
            deadband=5.0,
            max_stale_samples=3,
            severity=AlarmSeverity.CRITICAL_SAFETY,
        )
    }


def test_data_quality_flags_flatline_out_of_range_and_bad_timestamp():
    t0 = datetime(2026, 11, 25, 2, 0, tzinfo=timezone.utc)
    val = DataQualityValidator(_spec())

    assert val.evaluate(TelemetrySample("TURBINE.TEMP", t0, 50.0)) == QualityFlag.GOOD
    assert val.evaluate(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=5), 50.0)) == QualityFlag.GOOD
    assert val.evaluate(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=10), 50.0)) == QualityFlag.STALE_FLATLINE
    assert val.evaluate(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=15), 999.0)) == QualityFlag.OUT_OF_RANGE
    assert val.evaluate(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=10), 55.0)) == QualityFlag.BAD_TIMESTAMP


def test_alarm_sentinel_hysteresis_and_chatter_suppression():
    t0 = datetime(2026, 11, 25, 2, 0, tzinfo=timezone.utc)
    sentinel = AlarmSentinel(_spec(), chatter_window_sec=120, chatter_limit=3)

    # First trip activates and escalates
    e1 = sentinel.process(TelemetrySample("TURBINE.TEMP", t0, 102.0))
    assert e1 is not None and e1.state == "ACTIVE"
    assert e1.escalation_group == "Process-Safety-Lead"

    # Inside deadband (96.0 > 95.0): does not clear yet
    assert sentinel.process(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=10), 96.0)) is None

    # Below 95.0 clears cleanly
    e_clear = sentinel.process(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=20), 92.0))
    assert e_clear is not None and e_clear.state == "CLEARED"

    # Rapid second trip + clear + third trip triggers chatter suppression
    assert sentinel.process(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=30), 101.0)).state == "ACTIVE"
    assert sentinel.process(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=40), 90.0)).state == "CLEARED"
    e_sup = sentinel.process(TelemetrySample("TURBINE.TEMP", t0 + timedelta(seconds=50), 103.0))
    assert e_sup is not None and e_sup.state == "SUPPRESSED_CHATTER"


def test_compute_oee_and_downtime_attribution():
    m = compute_oee(
        planned_production_mins=480.0,
        downtime_events_mins={"seal_fault": 30.0, "sensor_recal": 18.0},
        ideal_cycle_time_mins=0.5,
        total_units=800,
        good_units=784,
    )
    assert m.unplanned_downtime_mins == 48.0
    assert m.availability_pct == 90.0
    assert m.quality_pct == 98.0
    assert m.oee_pct > 80.0
