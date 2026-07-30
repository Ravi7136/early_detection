"""Real-time cold-room entry monitor (CUSUM SPC + BLE proximity confirmation).

Designed for streaming: the detector is a stateful class that consumes ONE
sensor event at a time and never looks ahead. It works on partial data:

    LEARNING  - builds a stable temperature baseline from the live stream.
                The detector only arms itself once the recent readings are
                stable (low spread), so it is safe to start monitoring at
                any point in the package's life, not just at room temp.
    ARMED     - runs a one-sided (lower) CUSUM on temperature plus
                proximity (txPower/RSSI path loss towards cold-room WAPs)
                and rate-of-change confirmation. Fires ONE alert on entry.
    IN_COLD_ROOM - suppresses further alerts until the temperature
                recovers towards baseline (exit), then re-arms.

Stream robustness:
    - out-of-order / stale events are dropped
    - rssi == 0 or blank treated as invalid and ignored
    - proximity / rate-of-change windows are TIME based (not count based)
      so data gaps do not distort them

Usage:
    py realtime_cold_room_monitor.py                 # replay CSV as a live stream
    py realtime_cold_room_monitor.py --realtime 60   # 60x speed simulated clock

Integration: instantiate ColdRoomDetector and call detector.update(event)
from your ingestion pipeline (MQTT / Kafka consumer, webhook, etc.).
"""

import argparse
import csv
import statistics
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

CSV_PATH = "sensor_sample_data.csv"

# ---------------- configuration ----------------
BASELINE_SAMPLES = 6            # readings needed to learn the baseline
BASELINE_STABLE_RANGE = 1.5     # deg C max spread for the baseline window
MIN_TEMP_DROP = 3.0             # deg C below baseline required to alert
SIGMA_FLOOR = 0.25              # deg C lower bound on noise estimate
K_FACTOR = 0.5                  # CUSUM allowance k = K_FACTOR * sigma
H_FACTOR = 4.0                  # CUSUM threshold h = H_FACTOR * sigma
PROX_WINDOW = timedelta(minutes=15)   # proximity evidence window
COLD_SHARE_MIN = 0.5            # >=50% recent events heard by cold-room WAPs
ROC_WINDOW = timedelta(minutes=10)    # rate-of-change confirmation window
ROC_MIN_DROP = 0.5              # deg C fall inside ROC_WINDOW
EXIT_MARGIN_SIGMA = 2.0         # recovery above mu0 - 2*sigma => exited
MAD_TO_SIGMA = 1.4826           # robust sigma = MAD * 1.4826


def is_cold_room_wap(name: str) -> bool:
    return "RM" not in name.upper()


@dataclass
class SensorEvent:
    ts: datetime
    hw: str
    temp: float
    path_loss: Optional[float]   # median txPOWER - rssi over valid pairs (dB)

    @classmethod
    def from_record(cls, rec: dict) -> "SensorEvent":
        pairs = []
        for i in range(1, 6):
            tx = str(rec.get(f"txPOWER{i}", "")).strip()
            rs = str(rec.get(f"rssi{i}", "")).strip()
            if tx and rs:
                tx_v, rs_v = int(tx), int(rs)
                if rs_v != 0:                       # rssi 0 = invalid reading
                    pairs.append(tx_v - rs_v)
        return cls(
            ts=datetime.strptime(rec["EVENTTIME"], "%d-%m-%Y %H:%M"),
            hw=rec["HARDWARENAME"],
            temp=float(rec["IDNODECHIPTEMPARATURE"]),
            path_loss=statistics.median(pairs) if pairs else None,
        )


class ColdRoomDetector:
    """Online detector: feed events chronologically via update(); an alert
    dict is returned at the moment a cold-room entry is detected."""

    def __init__(self):
        self.state = "LEARNING"
        self.last_ts: Optional[datetime] = None
        self.baseline_buf: deque = deque(maxlen=BASELINE_SAMPLES)
        self.mu0 = self.sigma = self.k = self.h = None
        self.s_lo = 0.0
        self.recent: deque = deque()        # (ts, temp, hw, path_loss)

    # ---------- internals ----------
    def _prune(self, now: datetime):
        horizon = max(PROX_WINDOW, ROC_WINDOW)
        while self.recent and now - self.recent[0][0] > horizon:
            self.recent.popleft()

    def _try_arm(self):
        temps = [t for _, t in self.baseline_buf]
        if len(temps) < BASELINE_SAMPLES:
            return
        if max(temps) - min(temps) > BASELINE_STABLE_RANGE:
            return                          # not stable yet - keep sliding
        self.mu0 = statistics.mean(temps)
        med = statistics.median(temps)
        mad = statistics.median(abs(t - med) for t in temps)
        self.sigma = max(mad * MAD_TO_SIGMA, SIGMA_FLOOR)
        self.k = K_FACTOR * self.sigma
        self.h = H_FACTOR * self.sigma
        self.s_lo = 0.0
        self.state = "ARMED"

    def _proximity_ok(self, now: datetime) -> tuple:
        win = [r for r in self.recent if now - r[0] <= PROX_WINDOW]
        if not win:
            return False, 0.0, None
        cold = [r for r in win if is_cold_room_wap(r[2])]
        share = len(cold) / len(win)
        pl = [r[3] for r in cold if r[3] is not None]
        pl_now = statistics.median(pl) if pl else None
        return share >= COLD_SHARE_MIN and pl_now is not None, share, pl_now

    def _roc_ok(self, now: datetime, temp: float) -> bool:
        past = [t for ts, t, _, _ in self.recent if now - ts <= ROC_WINDOW]
        return bool(past) and max(past) - temp >= ROC_MIN_DROP

    # ---------- public API ----------
    def update(self, ev: SensorEvent) -> Optional[dict]:
        # drop stale / out-of-order events
        if self.last_ts is not None and ev.ts < self.last_ts:
            return None
        self.last_ts = ev.ts

        self._prune(ev.ts)
        alert = None

        if self.state == "LEARNING":
            self.baseline_buf.append((ev.ts, ev.temp))
            self._try_arm()

        elif self.state == "ARMED":
            self.s_lo = min(0.0, self.s_lo + (ev.temp - self.mu0 + self.k))
            prox_ok, share, pl_now = self._proximity_ok(ev.ts)
            if (self.s_lo < -self.h and prox_ok
                    and ev.temp <= self.mu0 - MIN_TEMP_DROP
                    and self._roc_ok(ev.ts, ev.temp)):
                self.state = "IN_COLD_ROOM"
                alert = {
                    "ts": ev.ts, "temp": ev.temp, "baseline": self.mu0,
                    "cusum": self.s_lo, "threshold": -self.h,
                    "cold_share": share, "path_loss": pl_now, "hw": ev.hw,
                }

        elif self.state == "IN_COLD_ROOM":
            if ev.temp > self.mu0 - EXIT_MARGIN_SIGMA * self.sigma:
                self.state = "ARMED"        # package left the cold room
                self.s_lo = 0.0

        self.recent.append((ev.ts, ev.temp, ev.hw, ev.path_loss))
        return alert


def format_alert(a: dict) -> str:
    lines = [
        "=" * 68,
        "EARLY WARNING ALERT - potential cold-room entry",
        f"  Time            : {a['ts']:%d-%m-%Y %H:%M}",
        f"  Temperature     : {a['temp']:.1f} C  (baseline {a['baseline']:.1f} C)",
        f"  CUSUM statistic : {a['cusum']:.2f}  (threshold {a['threshold']:.2f})",
        f"  Cold-WAP share  : {a['cold_share']:.0%} of proximity window",
    ]
    if a["path_loss"] is not None:
        lines.append(f"  Path loss (cold): {a['path_loss']:.1f} dB (lower = closer)")
    lines += [
        f"  Receiving WAP   : {a['hw']}",
        "  ACTION: notify operations - verify package is intended for cold storage.",
        "=" * 68,
    ]
    return "\n".join(lines)


def replay_stream(csv_path: str, speedup: Optional[float]):
    """Simulates a live feed by pushing CSV rows one at a time in order.
    In production, replace this loop with your MQTT/Kafka consumer."""
    detector = ColdRoomDetector()
    prev_ts, n_alerts, armed_at = None, 0, None

    with open(csv_path, newline="") as f:
        for rec in csv.DictReader(f):
            ev = SensorEvent.from_record(rec)
            if speedup and prev_ts is not None:
                time.sleep(max((ev.ts - prev_ts).total_seconds(), 0) / speedup)
            prev_ts = ev.ts

            was_learning = detector.state == "LEARNING"
            alert = detector.update(ev)
            if was_learning and detector.state == "ARMED":
                armed_at = ev.ts
                print(f"[{ev.ts:%d-%m-%Y %H:%M}] detector ARMED  "
                      f"baseline={detector.mu0:.2f} C  sigma={detector.sigma:.2f}  "
                      f"h={detector.h:.2f}")
            if alert:
                n_alerts += 1
                print(format_alert(alert))
            if detector.state == "ARMED" and alert is None and armed_at \
                    and ev.temp > detector.mu0 - EXIT_MARGIN_SIGMA * detector.sigma:
                pass  # normal armed operation - stay quiet

    print(f"\nStream ended: {n_alerts} alert(s). Final state: {detector.state}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Real-time cold-room entry monitor")
    ap.add_argument("--csv", default=CSV_PATH, help="event source to replay")
    ap.add_argument("--realtime", type=float, default=None, metavar="SPEEDUP",
                    help="replay with simulated inter-event delays, e.g. 60 = 60x speed")
    args = ap.parse_args()
    replay_stream(args.csv, args.realtime)
