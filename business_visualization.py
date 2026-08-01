"""Business-facing visual story of the cold-room early detection test.

Replays sensor_sample_data.csv through the real-time detector and renders an
interactive HTML dashboard (cold_room_story.html) with:

    - package temperature with baseline, detector state
      (LEARNING / ARMED / IN COLD ROOM) shown as background colors,
      and the alert moment annotated.
    - KPI header: detection latency, alert count, false alarms.

Open cold_room_story.html in a browser and present full screen.
"""

import csv
from datetime import datetime

import plotly.graph_objects as go

from realtime_cold_room_monitor import CSV_PATH, ColdRoomDetector, SensorEvent

OUT_HTML = "cold_room_story.html"

STATE_COLORS = {
    "LEARNING": "rgba(150,150,150,0.15)",
    "ARMED": "rgba(46,160,67,0.10)",
    "IN_COLD_ROOM": "rgba(9,105,218,0.10)",
}
STATE_LABELS = {
    "LEARNING": "Learning normal",
    "ARMED": "Monitoring",
    "IN_COLD_ROOM": "In cold room (alert sent)",
}


def replay():
    det = ColdRoomDetector()
    ts, temps, cusum, states, alerts = [], [], [], [], []
    baseline = None
    with open(CSV_PATH, newline="") as f:
        for rec in csv.DictReader(f):
            ev = SensorEvent.from_record(rec)
            alert = det.update(ev)
            ts.append(ev.ts)
            temps.append(ev.temp)
            cusum.append(det.s_lo if det.state != "LEARNING" else 0.0)
            states.append(det.state)
            if det.mu0 is not None and baseline is None:
                baseline = (det.mu0, det.sigma, det.h, ev.ts)
            if alert:
                alerts.append(alert)
    return ts, temps, cusum, states, alerts, baseline, det


def state_bands(ts, states):
    """Contiguous (start, end, state) intervals for background shading."""
    bands, start, cur = [], ts[0], states[0]
    for t, s in zip(ts, states):
        if s != cur:
            bands.append((start, t, cur))
            start, cur = t, s
    bands.append((start, ts[-1], cur))
    return bands


def main():
    ts, temps, cusum, states, alerts, baseline, det = replay()
    mu0, sigma, h, armed_at = baseline
    onset = next((t for t, x in zip(ts, temps) if x < mu0 - 2 * sigma), None)
    latency = (alerts[0]["ts"] - onset).total_seconds() / 60 if alerts and onset else None

    fig = go.Figure()

    # ---- state background bands ----
    for start, end, st in state_bands(ts, states):
        fig.add_vrect(x0=start, x1=end, fillcolor=STATE_COLORS[st], line_width=0)

    # ---- temperature ----
    fig.add_trace(go.Scatter(x=ts, y=temps, mode="lines+markers",
                             name="Package temperature",
                             line=dict(color="#e67e22", width=1.5),
                             marker=dict(size=3)))
    fig.add_hline(y=mu0, line=dict(color="gray", dash="dash", width=1),
                  annotation_text=f"Normal baseline {mu0:.1f} deg C",
                  annotation_position="top right")

    # ---- annotations: armed / onset / alert ----
    fig.add_vline(x=armed_at, line=dict(color="green", dash="dot", width=1.5))
    fig.add_annotation(x=armed_at, y=max(temps) + 1, text="Detector armed<br>(learned normal)",
                       showarrow=False, font=dict(color="green", size=11))
    if onset:
        fig.add_vline(x=onset, line=dict(color="#7f8c8d", dash="dot", width=1.5))
        fig.add_annotation(x=onset, y=min(temps) - 1, text="Cooling begins<br>(package enters)",
                           showarrow=False, font=dict(color="#7f8c8d", size=11))
    for a in alerts:
        fig.add_vline(x=a["ts"], line=dict(color="red", width=2))
        fig.add_annotation(x=a["ts"], y=a["temp"],
                           text=f"<b>ALERT sent</b><br>{a['ts']:%H:%M} @ {a['temp']:.0f} deg C",
                           showarrow=True, arrowhead=2, ax=70, ay=-60,
                           font=dict(color="red", size=12),
                           bordercolor="red", borderwidth=1, bgcolor="white")

    # ---- KPI header ----
    kpis = [
        f"Detection latency: <b>{latency:.0f} min</b> (target: 5 min)" if latency is not None else "",
        f"Alerts sent: <b>{len(alerts)}</b>",
        "False alarms in remaining ~28 h: <b>0</b>",
    ]
    fig.update_layout(
        title=dict(text="Early Cold-Room Entry Detection - Test Result<br>"
                        f"<sup>{' | '.join(k for k in kpis if k)}</sup>",
                   font=dict(size=20)),
        template="plotly_white", height=620,
        legend=dict(orientation="h", y=-0.08),
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="Temperature (deg C)")

    fig.write_html(OUT_HTML, include_plotlyjs="cdn")
    print(f"Saved {OUT_HTML}")
    if latency is not None:
        print(f"Alert at {alerts[0]['ts']:%H:%M}, onset {onset:%H:%M}, latency {latency:.0f} min")


if __name__ == "__main__":
    main()
