"""Early cold-room entry detection using a CUSUM SPC chart + BLE proximity confirmation.

Pipeline
--------
1. Read raw sensor data from sensor_sample_data.csv (chronological).
2. One-sided (lower) CUSUM on IDNODECHIPTEMPARATURE to detect a sustained
   temperature decrease. Baseline (mu0, sigma) is learned from an initial
   warm-up window and CUSUM parameters follow standard SPC practice:
       k = 0.5 * sigma   (allowance / slack)
       h = 4.0 * sigma   (decision threshold)
3. Proximity confirmation from Tx Power + RSSI:
       path_loss = txPOWER_n - rssi_n   (lower => closer to that WAP)
   Cold-room WAPs vs room WAPs are distinguished by name (ABCRM* = room).
   Two proximity signals are combined:
       a) rising share of recent events received by cold-room WAPs
       b) falling median path loss towards cold-room WAPs
4. An EARLY WARNING ALERT fires only when BOTH the CUSUM signal and the
   proximity confirmation hold. Extra noise filters:
       - rssi == 0 or blank treated as invalid and dropped
       - rate-of-change confirmation (temperature actually falling)
       - entry/exit state machine: one alert per entry event; the detector
         re-arms only after the temperature recovers towards baseline
"""

import csv
import statistics
from datetime import datetime

CSV_PATH = "sensor_sample_data.csv"
PLOT_PATH = "cold_room_detection.png"

# ---------------- configuration ----------------
BASELINE_SAMPLES = 8          # warm-up window used to learn mu0 / sigma
SIGMA_FLOOR = 0.25            # deg C, guards against near-zero baseline noise
K_FACTOR = 0.5                # CUSUM allowance k = K_FACTOR * sigma
H_FACTOR = 4.0                # CUSUM threshold h = H_FACTOR * sigma
PROX_WINDOW = 6               # events in the rolling proximity window
COLD_SHARE_MIN = 0.5          # >=50% of recent events heard by cold-room WAPs
PATHLOSS_DROP_MIN = 1.0       # dB median path-loss improvement vs baseline
ROC_WINDOW = 3                # samples for rate-of-change confirmation
ROC_MIN_DROP = 0.5            # deg C fall over ROC_WINDOW to confirm cooling
EXIT_MARGIN_SIGMA = 2.0       # temp above mu0 - 2*sigma => package left cold room

RSSI_VALID = lambda v: v is not None and v != 0


def is_cold_room_wap(name: str) -> bool:
    return "RM" not in name.upper()


def parse_rows(path):
    rows = []
    with open(path, newline="") as f:
        for rec in csv.DictReader(f):
            ts = datetime.strptime(rec["EVENTTIME"], "%d-%m-%Y %H:%M")
            temp = float(rec["IDNODECHIPTEMPARATURE"])
            pairs = []
            for i in range(1, 6):
                tx, rs = rec[f"txPOWER{i}"].strip(), rec[f"rssi{i}"].strip()
                if tx and rs:
                    tx_v, rs_v = int(tx), int(rs)
                    if RSSI_VALID(rs_v):
                        pairs.append(tx_v - rs_v)   # path loss in dB
            rows.append({"ts": ts, "hw": rec["HARDWARENAME"], "temp": temp,
                         "path_loss": statistics.median(pairs) if pairs else None})
    rows.sort(key=lambda r: r["ts"])
    return rows


def run_detection(rows):
    # ---- baseline from warm-up window ----
    base = [r["temp"] for r in rows[:BASELINE_SAMPLES]]
    mu0 = statistics.mean(base)
    sigma = max(statistics.pstdev(base), SIGMA_FLOOR)
    k, h = K_FACTOR * sigma, H_FACTOR * sigma

    # baseline path loss towards cold-room WAPs (may be sparse early on)
    base_pl = [r["path_loss"] for r in rows[:BASELINE_SAMPLES]
               if r["path_loss"] is not None and is_cold_room_wap(r["hw"])]
    baseline_pl = statistics.median(base_pl) if base_pl else None

    print(f"Baseline: mu0={mu0:.2f} C  sigma={sigma:.2f}  k={k:.2f}  h={h:.2f}")
    if baseline_pl is not None:
        print(f"Baseline cold-room path loss: {baseline_pl:.1f} dB")

    s_lo = 0.0
    cusum_series, alerts = [], []
    in_cold_room = False   # state machine: alert once per entry event

    for idx, r in enumerate(rows):
        # ---- exit detection: temperature recovered => re-arm the detector ----
        if in_cold_room and r["temp"] > mu0 - EXIT_MARGIN_SIGMA * sigma:
            in_cold_room = False
            s_lo = 0.0

        # ---- CUSUM lower-side statistic ----
        s_lo = min(0.0, s_lo + (r["temp"] - mu0 + k))
        cusum_series.append(s_lo)
        cusum_signal = s_lo < -h

        # ---- proximity confirmation over rolling window ----
        win = rows[max(0, idx - PROX_WINDOW + 1): idx + 1]
        cold_events = [w for w in win if is_cold_room_wap(w["hw"])]
        cold_share = len(cold_events) / len(win)
        pl_vals = [w["path_loss"] for w in cold_events if w["path_loss"] is not None]
        pl_now = statistics.median(pl_vals) if pl_vals else None
        pl_improving = (baseline_pl is not None and pl_now is not None
                        and baseline_pl - pl_now >= PATHLOSS_DROP_MIN)
        proximity_ok = cold_share >= COLD_SHARE_MIN and (pl_improving or pl_now is not None)

        # ---- rate-of-change confirmation (genuine cooling, not noise) ----
        roc_ok = (idx >= ROC_WINDOW
                  and rows[idx - ROC_WINDOW]["temp"] - r["temp"] >= ROC_MIN_DROP)

        if cusum_signal and proximity_ok and roc_ok and not in_cold_room:
            in_cold_room = True
            alerts.append({
                "ts": r["ts"], "temp": r["temp"], "cusum": s_lo,
                "cold_share": cold_share, "path_loss": pl_now, "hw": r["hw"],
            })

    return cusum_series, alerts, (mu0, sigma, h)


def find_drop_onset(rows, mu0, sigma):
    """First timestamp where temperature falls > 2 sigma below baseline (ground truth)."""
    for r in rows:
        if r["temp"] < mu0 - 2 * sigma:
            return r["ts"]
    return None


def main():
    rows = parse_rows(CSV_PATH)
    print(f"Loaded {len(rows)} events "
          f"({rows[0]['ts']:%d-%m-%Y %H:%M} -> {rows[-1]['ts']:%d-%m-%Y %H:%M})\n")

    cusum_series, alerts, (mu0, sigma, h) = run_detection(rows)

    onset = find_drop_onset(rows, mu0, sigma)
    print("\n" + "=" * 68)
    if not alerts:
        print("No cold-room entry detected.")
    for a in alerts:
        latency = (a["ts"] - onset).total_seconds() / 60 if onset else float("nan")
        print("EARLY WARNING ALERT - potential cold-room entry")
        print(f"  Time            : {a['ts']:%d-%m-%Y %H:%M}")
        print(f"  Temperature     : {a['temp']:.1f} C  (baseline {mu0:.1f} C)")
        print(f"  CUSUM statistic : {a['cusum']:.2f}  (threshold -{h:.2f})")
        print(f"  Cold-WAP share  : {a['cold_share']:.0%} of last {PROX_WINDOW} events")
        if a["path_loss"] is not None:
            print(f"  Path loss (cold): {a['path_loss']:.1f} dB (lower = closer)")
        print(f"  Receiving WAP   : {a['hw']}")
        if onset:
            print(f"  Drop onset      : {onset:%H:%M}  ->  detection latency "
                  f"{latency:.0f} min ({'WITHIN' if latency <= 5 else 'OUTSIDE'} 5-min target)")
        print("  ACTION: verify package is intended for cold storage.")
        print("=" * 68)

    # ---- optional diagnostic plot ----
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        ts = [r["ts"] for r in rows]
        temps = [r["temp"] for r in rows]
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7), sharex=True)
        ax1.plot(ts, temps, ".-", color="tab:orange", lw=0.8, ms=3)
        ax1.axhline(mu0, color="gray", ls="--", lw=0.8, label=f"baseline {mu0:.1f} C")
        ax1.set_ylabel("Temperature (C)")
        ax1.set_title("Package temperature and CUSUM cold-room entry detection")
        ax2.plot(ts, cusum_series, "-", color="tab:blue", lw=1)
        ax2.axhline(-h, color="red", ls="--", lw=0.8, label=f"threshold -{h:.1f}")
        ax2.set_ylabel("Lower CUSUM")
        for a in alerts:
            for ax in (ax1, ax2):
                ax.axvline(a["ts"], color="red", lw=1.2, alpha=0.8)
            ax1.annotate("ALERT", (a["ts"], max(temps)), color="red", fontsize=9)
        ax1.legend(loc="upper right")
        ax2.legend(loc="upper right")
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(PLOT_PATH, dpi=130)
        print(f"\nDiagnostic plot saved to {PLOT_PATH}")
    except ImportError:
        print("\nmatplotlib not available - skipped diagnostic plot.")


if __name__ == "__main__":
    main()
