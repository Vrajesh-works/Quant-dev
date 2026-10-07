"""Smart Order Router Lab - interactive Cont-Kukanov demo.

Runs the allocation engine from allocator.py directly on venue snapshots
(no Kafka broker needed), and compares against TWAP / VWAP / Best-Ask.
Deploy: Streamlit Community Cloud, main file = streamlit_app.py
"""

import copy
import json

import numpy as np
import pandas as pd
import streamlit as st

from allocator import ContKukanovAllocator, Venue
from benchmark_strategies import BenchmarkStrategies

st.set_page_config(page_title="SOR Lab - Smart Order Router", layout="wide")

# ---------------------------------------------------------------- helpers

DEFAULT_VENUES = [
    {"venue": "NYSE",   "ask": 50.00, "ask_size": 2000, "fee": 0.0030, "rebate": 0.0020},
    {"venue": "NASDAQ", "ask": 49.99, "ask_size": 1500, "fee": 0.0050, "rebate": 0.0010},
    {"venue": "BATS",   "ask": 50.03, "ask_size": 4000, "fee": 0.0010, "rebate": 0.0030},
    {"venue": "EDGX",   "ask": 50.01, "ask_size": 1200, "fee": 0.0020, "rebate": 0.0020},
]


def random_market(n=4):
    names = ["NYSE", "NASDAQ", "BATS", "EDGX", "IEX", "MEMX"]
    mid = 50.0
    rows = []
    for i in range(n):
        rows.append({
            "venue": names[i % len(names)],
            "ask": round(mid + np.random.uniform(-0.06, 0.06), 2),
            "ask_size": int(np.random.uniform(800, 4500)),
            "fee": round(float(np.random.uniform(0.001, 0.006)), 4),
            "rebate": round(float(np.random.uniform(0.001, 0.004)), 4),
        })
    return rows


def venues_from_df(df):
    venues = []
    for _, r in df.iterrows():
        try:
            name = str(r["venue"]).strip() or f"VENUE-{len(venues)+1}"
            venues.append(Venue(
                id=name,
                ask=float(r["ask"]),
                ask_size=int(r["ask_size"]),
                fee=float(r["fee"]),
                rebate=float(r["rebate"]),
            ))
        except (ValueError, TypeError):
            continue
    return [v for v in venues if v.ask_size > 0 and v.ask > 0]


def cost_breakdown(split, venues, order_size, alloc):
    """Recompute the Cont-Kukanov cost components for display."""
    cash_spent, executed = 0.0, 0
    for q, v in zip(split, venues):
        exe = min(q, v.ask_size)
        executed += exe
        cash_spent += exe * (v.ask + v.fee)
        cash_spent -= max(q - exe, 0) * v.rebate
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk = alloc.theta_queue * (underfill + overfill)
    penalty = alloc.lambda_under * underfill + alloc.lambda_over * overfill
    return cash_spent, risk, penalty, executed


@st.cache_data
def load_backtest_results():
    try:
        with open("backtest_results.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


# ---------------------------------------------------------------- sidebar

st.sidebar.header("Order & model parameters")
order_size = st.sidebar.number_input("Order size (shares)", min_value=100, max_value=20000,
                                     value=5000, step=100)
if order_size % 100:
    order_size -= order_size % 100
    st.sidebar.caption(f"Snapped to {order_size} (allocator works in 100-share lots)")

lambda_over = st.sidebar.slider("lambda_over (overfill penalty)", 0.0, 1.5, 0.4, 0.1)
lambda_under = st.sidebar.slider("lambda_under (underfill penalty)", 0.0, 1.5, 0.6, 0.1)
theta_queue = st.sidebar.slider("theta_queue (queue-risk weight)", 0.0, 1.0, 0.3, 0.1)

st.sidebar.header("Venue book")
if st.sidebar.button("Simulate new market"):
    st.session_state["venue_rows"] = random_market()
    st.rerun()

if "venue_rows" not in st.session_state:
    st.session_state["venue_rows"] = DEFAULT_VENUES

venue_df = st.sidebar.data_editor(
    pd.DataFrame(st.session_state["venue_rows"]),
    num_rows="dynamic",
    key="venue_editor",
    column_config={
        "venue": st.column_config.TextColumn("Venue"),
        "ask": st.column_config.NumberColumn("Ask $", format="%.2f"),
        "ask_size": st.column_config.NumberColumn("Ask size"),
        "fee": st.column_config.NumberColumn("Fee $/sh", format="%.4f"),
        "rebate": st.column_config.NumberColumn("Rebate $/sh", format="%.4f"),
    },
)

venues = venues_from_df(venue_df)

# ---------------------------------------------------------------- header

st.title("Smart Order Router Lab")
st.caption(
    "Optimal order splitting across trading venues using the Cont-Kukanov cost model: "
    "minimize cash spent + queue-risk penalties + fill-risk penalties. "
    "This demo runs the allocation engine directly; the Kafka streaming layer is bypassed in the hosted version."
)

if not venues:
    st.warning("Add at least one venue with a positive ask price and size.")
    st.stop()

total_liquidity = sum(v.ask_size for v in venues)
if total_liquidity < order_size:
    st.warning(f"Only {total_liquidity:,} shares available across venues for a {order_size:,}-share order. "
               "The optimizer will underfill and penalize it.")

# ---------------------------------------------------------------- allocation

allocator = ContKukanovAllocator(lambda_over, lambda_under, theta_queue)
split, total_cost = allocator.allocate(order_size, copy.deepcopy(venues))
cash, risk, penalty, executed = cost_breakdown(split, venues, order_size, allocator)
avg_px = cash / executed if executed else 0.0

st.subheader("Optimal allocation")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total cost", f"${total_cost:,.2f}")
col2.metric("Avg fill price", f"${avg_px:.4f}")
col3.metric("Shares filled", f"{executed:,}")
col4.metric("Venues used", f"{sum(1 for q in split if q > 0)}")

alloc_df = pd.DataFrame({"venue": [v.id for v in venues], "shares": split}).set_index("venue")
st.bar_chart(alloc_df)

with st.expander("Cost breakdown"):
    bdf = pd.DataFrame({
        "Component": ["Cash spent (price + fees - rebates)", "Queue-risk penalty", "Fill-risk penalty"],
        "Cost ($)": [round(cash, 2), round(risk, 2), round(penalty, 2)],
    })
    st.dataframe(bdf, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- benchmarks

st.subheader("Benchmark comparison")
bench = BenchmarkStrategies()
results = {"Cont-Kukanov": (total_cost, avg_px, executed)}
for name, fn in [("Best Ask", bench.naive_best_ask),
                 ("TWAP", bench.twap_strategy),
                 ("VWAP", bench.vwap_strategy)]:
    r = fn(order_size, copy.deepcopy(venues))
    results[name] = (r.total_cash, r.avg_fill_px, r.shares_filled)

base_cost = results["Cont-Kukanov"][0]
rows = []
for name, (cost, avg, filled) in results.items():
    saving_bps = (cost - base_cost) / cost * 10000 if cost else 0.0
    rows.append({"Strategy": name,
                 "Filled": f"{filled:,}",
                 "Total cost ($)": round(cost, 2),
                 "Avg fill ($)": round(avg, 4),
                 "Extra cost vs optimal (bps)": round(saving_bps, 1)})
st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- backtest reference

st.subheader("Full backtest reference")
bt = load_backtest_results()
if bt:
    bp = bt["best_parameters"]
    c1, c2, c3 = st.columns(3)
    c1.metric("Best lambda_over", bp["lambda_over"])
    c2.metric("Best lambda_under", bp["lambda_under"])
    c3.metric("Best theta_queue", bp["theta_queue"])
    st.caption("Savings of the optimized router vs baselines in the historical backtest (basis points):")
    sdf = pd.DataFrame([{"Baseline": k, "Saved (bps)": v}
                        for k, v in bt["savings_vs_baselines_bps"].items()])
    st.dataframe(sdf, hide_index=True, use_container_width=True)
else:
    st.info("backtest_results.json not found next to the app.")

# ---------------------------------------------------------------- sensitivity

st.subheader("Parameter sensitivity")
st.caption("Total cost across a grid of penalty parameters (theta fixed at sidebar value). "
           "Darker = cheaper.")
with st.spinner("Sweeping parameters..."):
    lo_vals = [0.2, 0.4, 0.6, 0.8, 1.0]
    lu_vals = [0.3, 0.5, 0.7, 0.9, 1.1]
    grid = np.zeros((len(lu_vals), len(lo_vals)))
    for i, lu in enumerate(lu_vals):
        for j, lo in enumerate(lo_vals):
            a = ContKukanovAllocator(lo, lu, theta_queue)
            _, c = a.allocate(order_size, copy.deepcopy(venues))
            grid[i, j] = c
    gdf = pd.DataFrame(grid, index=[f"lu={v}" for v in lu_vals],
                       columns=[f"lo={v}" for v in lo_vals])
    st.dataframe(gdf.style.background_gradient(cmap="Greens_r", axis=None).format("${:,.0f}"),
                 use_container_width=True)

st.divider()
st.caption("Reference: Cont & Kukanov, 'Optimal order placement in limit order markets' (2012). "
           "Demo for portfolio purposes; not investment advice.")
