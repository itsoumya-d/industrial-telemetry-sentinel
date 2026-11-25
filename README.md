# Industrial Telemetry Sentinel

Operational technology (OT) telemetry quality validator, hysteresis/chatter-suppressed alarm engine, and Overall Equipment Effectiveness (OEE) downtime analytics library in Python.

## Problem

Industrial time-series tags from PLCs, SCADA historians, and fleet sensors suffer from frozen/flatlined readings, out-of-range spikes, and chattering alarms that overwhelm operators during process upsets.

## Solution

- **Telemetry Data Quality Validation**: Flags stale/frozen sensors, physical range violations, and timestamp regressions with deterministic tag quality scoring.
- **Alarm Hysteresis & Chatter Suppression**: Applies configurable deadbands and sliding-window transition limits to isolate genuine process safety escalations from sensor noise.
- **OEE & Downtime Analytics**: Computes Availability, Performance, Quality, and composite OEE percentages with categorized downtime attribution.

## Run Locally

```bash
python3 -m pip install -e ".[dev]"
pytest
```
