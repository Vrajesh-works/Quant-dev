"""SOR Lab - interactive Smart Order Router demo.

Optimal order splitting across fragmented venues using the Cont-Kukanov
cost model, with live crypto L2 books, TCA decomposition, market impact,
and institutional benchmarks (TWAP / VWAP / POV / Almgren-Chriss).

Data sources: synthetic book (editable) or live Binance/Coinbase/Kraken
L2 books via free public REST endpoints (no API key).

Deploy: Streamlit Community Cloud, main file = streamlit_app.py
"""

import copy
import json

import numpy as np
import pandas as pd
import streamlit as st

from allocator import ContKukanovAllocator, Venue
from benchmark_strategies import BenchmarkStrategies
from market_impact import ImpactParams, default_adv, effective_price
from tca import Fill, compute_tca
import crypto_feed

st.set_page_config(page_title="SOR Lab - Smart Order Router", layout="wide")

# ---------------------------------------------------------------- helpers

DEFAULT_VENUES = [
    {"venue": "NYSE",   "ask": 50.00, "ask_size": 2000, "fee": 0.0030, "rebate": 0.0020},
    {"venue": "NASDAQ", "ask": 49.99, "ask_size": 1500, "fee": 0.0050, "rebate": 0.0010},
    {"venue": "BATS",   "ask": 50.03, "ask_size": 4000, "fee": 0.0010, "rebate": 0.0030},
    {"venue": "EDGX",   "ask": 50.01, "ask_size": 1200, "fee": 0.0020, "rebate": 0.0020},
]
UNIT_LABEL = {"SYNTH": "shares", "BTC": "0.001 BTC", "ETH": "0.01 ETH"}


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
            ask = float(r["ask"])
            venues.append(Venue(
                id=name, ask=ask, ask_size=int(r["ask_size"]),
                fee=float(r["fee"]), rebate=float(r["rebate"]),
                bid=ask * (1 - 0.0004),  # 4 bps synthetic spread
            ))
        except (ValueError, TypeError):
            continue
    return [v for v in venues if v.ask_size > 0 and v.ask > 0]


@st.cache_data(ttl=60)
def load_live_books(base):
    return crypto_feed.fetch_live_books(base)


def synthetic_ladder(venue, levels=8):
    """Fake depth ladder around the top of book for the synthetic market."""
    ladder = []
    cum = 0
    for i in range(levels):
        px = venue.ask * (1 + i * 0.0002)
        sz = max(int(venue.ask_size * 0.35 * (0.85 ** i)), 1)
        cum += sz
        ladder.append({"price": px, "size": sz, "cum_size": cum})
    return ladder


def live_ladder(book, levels=12):
    ladder, cum = [], 0
    for l in book.asks[:levels]:
        cum += l.size_units
        ladder.append({"price": l.price, "size": l.size_units, "cum_size": cum})
    return ladder


def apply_impact_to_fills(fills, venues, adv_map, params, enabled):
    """Adjust fill prices for temporary market impact when enabled."""
    if not enabled:
        return fills
    by_id = {v.id: v for v in venues}
    out = []
    for f in fills:
        v = by_id.get(f.venue_id)
        if v is None:
            out.append(f)
            continue
        adv = adv_map.get(v.id, default_adv(v))
        px = effective_price(v, f.shares, adv, params)
        out.append(Fill(v.id, f.shares, px, f.mid, v.fee))
    return out


@st.cache_data
def load_backtest_results():
    try:
        with open("backtest_results.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


# ---------------------------------------------------------------- sidebar

st.sidebar.header("Market data")
data_mode = st.sidebar.radio(
    "Book source",
    ["Synthetic book", "Live: BTC-USD", "Live: ETH-USD"],
    index=0,
)
live_base = {"Live: BTC-USD": "BTC", "Live: ETH-USD": "ETH"}.get(data_mode)
is_live = live_base is not None
unit = UNIT_LABEL["BTC" if live_base == "BTC" else "ETH" if live_base else "SYNTH"]

st.sidebar.header("Order & model")
order_size = st.sidebar.number_input(f"Order size ({unit})", min_value=100,
                                     max_value=20000, value=5000, step=100)
if order_size % 100:
    order_size -= order_size % 100
    st.sidebar.caption(f"Snapped to {order_size} (100-unit lots)")

lambda_over = st.sidebar.slider("lambda_over (overfill penalty)", 0.0, 1.5, 0.4, 0.1)
lambda_under = st.sidebar.slider("lambda_under (underfill penalty)", 0.0, 1.5, 0.6, 0.1)
theta_queue = st.sidebar.slider("theta_queue (queue-risk weight)", 0.0, 1.0, 0.3, 0.1)
use_impact = st.sidebar.checkbox("Model market impact (sqrt model)", value=True)
urgency = st.sidebar.slider("AC urgency (risk aversion)", 0.01, 100.0, 0.1, 0.01,
                            help="Almgren-Chriss risk aversion. Low = patient (TWAP-like), high = front-loaded.")

impact_params = ImpactParams()

# ---------------------------------------------------------------- market

books, book_errors = [], {}
if is_live:
    if st.sidebar.button("Refresh live books"):
        st.cache_data.clear()
        st.rerun()
    with st.spinner(f"Fetching live {live_base} books..."):
        books, book_errors = load_live_books(live_base)
    if len(books) < 2:
        st.error(f"Only {len(books)} live venue(s) reachable ({book_errors}). "
                 "Falling back to the synthetic book.")
        is_live = False
        books = []

if is_live:
    venues = crypto_feed.venues_from_books(books)
    adv_map = crypto_feed.adv_map_from_books(books)
    arrival = crypto_feed.arrival_mid(books)
    ladders = {b.venue_id: live_ladder(b) for b in books}
    st.sidebar.caption(f"Live books: {', '.join(b.venue_id for b in books)}")
    if book_errors:
        st.sidebar.caption(f"Unreachable: {', '.join(book_errors)}")
else:
    st.sidebar.header("Venue book")
    if st.sidebar.button("Simulate new market"):
        st.session_state["venue_rows"] = random_market()
        st.rerun()
    if "venue_rows" not in st.session_state:
        st.session_state["venue_rows"] = DEFAULT_VENUES
    venue_df = st.sidebar.data_editor(
        pd.DataFrame(st.session_state["venue_rows"]),
        num_rows="dynamic", key="venue_editor",
        column_config={
            "venue": st.column_config.TextColumn("Venue"),
            "ask": st.column_config.NumberColumn("Ask $", format="%.2f"),
            "ask_size": st.column_config.NumberColumn("Ask size"),
            "fee": st.column_config.NumberColumn("Fee $/sh", format="%.4f"),
            "rebate": st.column_config.NumberColumn("Rebate $/sh", format="%.4f"),
        },
    )
    venues = venues_from_df(venue_df)
    adv_map = {v.id: default_adv(v) for v in venues}
    arrival = (sum(v.mid * v.ask_size for v in venues) / sum(v.ask_size for v in venues)
               if venues else 0.0)
    ladders = {v.id: synthetic_ladder(v) for v in venues}

# ---------------------------------------------------------------- header

st.title("SOR Lab: Smart Order Router")
st.caption(
    "Optimal order splitting across fragmented venues (Cont-Kukanov) with live books, "
    "TCA decomposition, square-root market impact, and institutional benchmarks. "
    f"Arrival price: **{arrival:,.2f}**."
)

if not venues:
    st.warning("Add at least one venue with a positive ask price and size.")
    st.stop()

total_liquidity = sum(v.ask_size for v in venues)
if total_liquidity < order_size:
    st.warning(f"Only {total_liquidity:,} units available for a {order_size:,}-unit order. "
               "The optimizer will underfill and penalize it.")

# ---------------------------------------------------------------- depth

st.subheader("Order book depth")
depth_cols = st.columns(len(venues))
for col, v in zip(depth_cols, venues):
    with col:
        st.caption(f"**{v.id}** — ask {v.ask:,.2f}, size {v.ask_size:,}")
        df = pd.DataFrame(ladders[v.id]).set_index("price")["cum_size"]
        st.line_chart(df)

# ---------------------------------------------------------------- allocation

allocator = ContKukanovAllocator(lambda_over, lambda_under, theta_queue)
split, _noimpact_cost = allocator.allocate(order_size, copy.deepcopy(venues))

ck_fills = []
for v, q in zip(venues, split):
    if q <= 0:
        continue
    adv = adv_map.get(v.id, default_adv(v))
    px = effective_price(v, q, adv, impact_params) if use_impact else v.ask + v.fee
    ck_fills.append(Fill(v.id, q, px, v.mid, v.fee))
executed = sum(f.shares for f in ck_fills)
ck_cash = sum(f.shares * f.price for f in ck_fills)
ck_avg = ck_cash / executed if executed else 0.0

st.subheader("Optimal allocation (Cont-Kukanov)")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Expected cost", f"${ck_cash:,.2f}")
c2.metric("Avg fill price", f"${ck_avg:,.4f}")
c3.metric("Units filled", f"{executed:,}")
c4.metric("Venues used", f"{sum(1 for q in split if q > 0)}")

alloc_df = pd.DataFrame({"venue": [v.id for v in venues], "units": split}).set_index("venue")
st.bar_chart(alloc_df)

# ---------------------------------------------------------------- benchmarks + TCA

st.subheader("Benchmarks & transaction cost analysis")
bench = BenchmarkStrategies()
results = {"Cont-Kukanov": ck_fills}
for name, fn in [("Best Ask", bench.naive_best_ask),
                 ("TWAP", bench.twap_strategy),
                 ("VWAP", bench.vwap_strategy),
                 ("POV", bench.pov_strategy),
                 ("Almgren-Chriss", lambda s, v: bench.almgren_chriss_strategy(
                     s, v, risk_aversion=urgency))]:
    r = fn(order_size, copy.deepcopy(venues))
    fee_by_id = {v.id: v.fee for v in venues}
    fills = [Fill(f["venue_id"], f["shares"], f["price"], f["mid"],
                  fee_by_id.get(f["venue_id"], 0.0)) for f in r.fills]
    results[name] = apply_impact_to_fills(fills, venues, adv_map, impact_params, use_impact)

tca_reports = {}
for name, fills in results.items():
    tca_reports[name] = compute_tca(name, fills, arrival, arrival, order_size,
                                    adv_map, impact_params)

base_bps = tca_reports["Cont-Kukanov"].shortfall_bps
rows = []
for name, rep in tca_reports.items():
    rows.append({
        "Strategy": name,
        "Filled": f"{rep.filled_shares:,}",
        "Total cost ($)": round(rep.gross_cash, 2),
        "Avg fill": round(rep.gross_cash / rep.filled_shares, 4) if rep.filled_shares else 0,
        "IS (bps)": round(rep.shortfall_bps, 1),
        "vs optimal (bps)": round(rep.shortfall_bps - base_bps, 1),
    })
st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

with st.expander("TCA decomposition: where the shortfall comes from"):
    tdf = pd.DataFrame([{
        "Strategy": r.strategy,
        "Spread ($)": round(r.spread_cost, 2),
        "Fees ($)": round(r.fees, 2),
        "Delay ($)": round(r.delay_cost, 2),
        "Impact, modeled ($)": round(r.impact_cost, 2),
        "Opportunity ($)": round(r.opportunity_cost, 2),
        "Shortfall (bps)": round(r.shortfall_bps, 1),
    } for r in tca_reports.values()])
    st.dataframe(tdf, hide_index=True, use_container_width=True)
    st.caption("Perold / Wagner-Edwards taxonomy vs the arrival price. "
               "Impact is modeled with the square-root model; spread and delay come from fills.")

# ---------------------------------------------------------------- backtest reference

st.subheader("Historical backtest reference")
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

# ---------------------------------------------------------------- sensitivity

st.subheader("Parameter sensitivity")
st.caption("Total expected cost across a grid of penalty parameters (theta fixed at sidebar value).")
with st.spinner("Sweeping parameters..."):
    lo_vals = [0.2, 0.4, 0.6, 0.8, 1.0]
    lu_vals = [0.3, 0.5, 0.7, 0.9, 1.1]
    grid = np.zeros((len(lu_vals), len(lo_vals)))
    for i, lu in enumerate(lu_vals):
        for j, lo in enumerate(lo_vals):
            a = ContKukanovAllocator(lo, lu, theta_queue)
            s, _ = a.allocate(order_size, copy.deepcopy(venues))
            cost = sum(q * (effective_price(v, q, adv_map.get(v.id, default_adv(v)), impact_params)
                            if use_impact else v.ask + v.fee)
                       for v, q in zip(venues, s))
            grid[i, j] = cost
    gdf = pd.DataFrame(grid, index=[f"lu={v}" for v in lu_vals],
                       columns=[f"lo={v}" for v in lo_vals])
    st.dataframe(gdf.style.background_gradient(cmap="Greens_r", axis=None).format("${:,.0f}"),
                 use_container_width=True)

st.divider()
st.caption("Models: Cont & Kukanov (2012) optimal placement; Almgren & Chriss (2001) trajectory; "
           "square-root impact (Almgren et al.); TCA per Perold / Wagner-Edwards. "
           "Live books: Binance / Coinbase / Kraken public L2 endpoints. Demo for education, not investment advice.")
