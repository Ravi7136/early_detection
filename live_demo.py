"""Live demo: streams BLE sensor events from the CSV as if they were arriving
in real time, runs the ColdRoomDetector on each event, and animates the
temperature + CUSUM evidence charts. An alert banner fires the moment the
package starts cooling.

Run:  py -m streamlit run live_demo.py
"""

import csv
import time

import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from realtime_cold_room_monitor import CSV_PATH, ColdRoomDetector, SensorEvent

st.set_page_config(page_title="Cold-Room Early Detection - Live Demo",
                   layout="wide", page_icon="\u2744")

STATE_BADGE = {
    "LEARNING": ("Learning normal", "gray"),
    "ARMED": ("Monitoring", "green"),
    "IN_COLD_ROOM": ("IN COLD ROOM - alert sent", "red"),
}


@st.cache_data
def load_events():
    with open(CSV_PATH, newline="") as f:
        return [SensorEvent.from_record(rec) for rec in csv.DictReader(f)]


def build_figure(ts, temps, cusum, mu0, h, alerts):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.08, row_heights=[0.55, 0.45],
                        subplot_titles=("Package temperature (live)",
                                        "CUSUM evidence meter (live)"))
    fig.add_trace(go.Scatter(x=ts, y=temps, mode="lines+markers",
                             name="Temperature",
                             line=dict(color="#e67e22", width=2),
                             marker=dict(size=4)), row=1, col=1)
    if mu0 is not None:
        fig.add_hline(y=mu0, line=dict(color="gray", dash="dash", width=1),
                      annotation_text=f"baseline {mu0:.1f} deg C", row=1, col=1)
    fig.add_trace(go.Scatter(x=ts, y=cusum, mode="lines",
                             name="Evidence (CUSUM)",
                             line=dict(color="#2c6fbb", width=2)), row=2, col=1)
    if h is not None:
        fig.add_hline(y=-h, line=dict(color="red", dash="dash", width=1.5),
                      annotation_text="alarm threshold", row=2, col=1)
    for a in alerts:
        for r in (1, 2):
            fig.add_vline(x=a["ts"], line=dict(color="red", width=2), row=r, col=1)
        fig.add_annotation(x=a["ts"], y=a["temp"], text="<b>ALERT</b>",
                           showarrow=True, arrowhead=2, ax=50, ay=-40,
                           font=dict(color="red", size=13), bgcolor="white",
                           bordercolor="red", row=1, col=1)
    fig.update_layout(template="plotly_white", height=620,
                      showlegend=False, margin=dict(t=60, b=30),
                      uirevision="keep")  # preserve zoom between frames
    fig.update_yaxes(title_text="deg C", row=1, col=1)
    fig.update_yaxes(title_text="evidence", row=2, col=1)
    return fig


# ---------------- UI ----------------
st.title("\u2744 Cold-Room Early Detection - Live Stream Demo")
st.caption("BLE events replayed from sensor_sample_data.csv through the "
           "real-time CUSUM detector - exactly as they would arrive in production.")

with st.sidebar:
    st.header("Demo controls")
    speedup = st.slider("Replay speed (x real time)", 60, 3600, 600, step=60,
                        help="600x: one real minute passes every 0.1 s")
    max_fps_sleep = 1.0 / 30
    start = st.button("Start live stream", type="primary", use_container_width=True)
    st.markdown("---")
    st.markdown("**Legend**\n- Orange: package temperature\n"
                "- Blue: accumulated evidence (CUSUM)\n"
                "- Red dashed: alarm threshold")

alert_box = st.empty()
k1, k2, k3, k4 = st.columns(4)
clock_ph, temp_ph, state_ph, alerts_ph = k1.empty(), k2.empty(), k3.empty(), k4.empty()
chart_ph = st.empty()

if start:
    events = load_events()
    det = ColdRoomDetector()
    ts, temps, cusum, alerts = [], [], [], []
    prev_ts = None

    for ev in events:
        # simulated real-time pacing
        if prev_ts is not None:
            gap = (ev.ts - prev_ts).total_seconds() / speedup
            time.sleep(min(max(gap, 0), 2.0))
        prev_ts = ev.ts

        alert = det.update(ev)
        ts.append(ev.ts)
        temps.append(ev.temp)
        cusum.append(det.s_lo if det.state != "LEARNING" else 0.0)
        if alert:
            alerts.append(alert)
            alert_box.error(
                f"**EARLY WARNING ALERT - potential cold-room entry**  \n"
                f"Time **{alert['ts']:%d-%m-%Y %H:%M}** | "
                f"Temperature **{alert['temp']:.1f} deg C** "
                f"(baseline {alert['baseline']:.1f}) | "
                f"CUSUM {alert['cusum']:.2f} (threshold {alert['threshold']:.2f}) | "
                f"WAP {alert['hw']}  \n"
                f"ACTION: verify package is intended for cold storage.",
                icon="\U0001F6A8")
            st.toast("Cold-room entry detected!", icon="\U0001F6A8")

        # KPI row
        label, color = STATE_BADGE[det.state]
        clock_ph.metric("Stream clock", f"{ev.ts:%d-%m %H:%M}")
        temp_ph.metric("Temperature", f"{ev.temp:.1f} deg C")
        state_ph.markdown(f"**Detector state**  \n:{color}[{label}]")
        alerts_ph.metric("Alerts sent", len(alerts))

        chart_ph.plotly_chart(
            build_figure(ts, temps, cusum, det.mu0, det.h, alerts),
            use_container_width=True, key=f"chart_{len(ts)}")

    st.success(f"Stream finished - {len(alerts)} alert(s), "
               f"final state: {det.state}")
else:
    st.info("Set the replay speed in the sidebar and press **Start live stream**.")
