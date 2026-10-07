# Smart Order Router (SOR)

[![CI](https://github.com/Vrajesh-works/Quant-dev/actions/workflows/ci.yml/badge.svg)](https://github.com/Vrajesh-works/Quant-dev/actions/workflows/ci.yml)

Optimal order execution across multiple trading venues using the Cont-Kukanov cost model,
with real-time market data streaming via Kafka and a backtesting engine.

**Live interactive demo:** [SOR Lab on Streamlit](https://quant-dev.streamlit.app/)
_(run the allocator on live crypto books, tune the risk parameters, and compare against TWAP / VWAP / POV / Almgren-Chriss, all in the browser)_

## What this is not

Most Cont-Kukanov implementations online are the same interview trial task:
a 5000-share brute-force allocator with Best-Ask/TWAP/VWAP baselines on
synthetic data. This project goes further:

- **Live fragmented markets** - real level-2 books from Binance, Coinbase
  and Kraken (free public endpoints, no API key) as the venues
- **TCA** - implementation shortfall decomposed into spread, fees, delay,
  modeled market impact and opportunity cost (Perold / Wagner-Edwards)
- **Square-root market impact** - temporary + permanent impact, so splitting
  across venues is genuinely optimal instead of decorative
- **Institutional benchmarks** - POV and the Almgren-Chriss optimal
  trajectory alongside TWAP/VWAP/Best-Ask, all with fill guarantees
- **Order-book depth visualization** - cumulative depth per venue
- **pytest suite + CI** - 48 tests covering the allocator, benchmarks, TCA
  and impact model

## Overview

When executing a large order, naive strategies either pay the spread everywhere or leave
size unfilled. This router treats execution as an optimization problem: split the order
across venues to minimize

```
Total Cost = Cash Spent + Queue-Risk Penalty + Fill-Risk Penalty
```

- **Cash Spent**: price + fees - maker rebates per venue
- **Queue-Risk Penalty** (`theta_queue`): cost of queue-position uncertainty
- **Fill-Risk Penalty** (`lambda_under`, `lambda_over`): cost of under/over-filling

A parameter search over historical snapshots found the best configuration at
`lambda_over=0.4, lambda_under=0.6, theta_queue=0.3`, beating the baselines by
**4-16 basis points** (see `backtest_results.json`).

## Architecture

```
Market Data Simulation --> Kafka Streaming --> Backtester & Allocator --> Benchmarks
                                                    |
                                          Streamlit demo (no Kafka needed)
```

### Components

- **allocator.py** - `ContKukanovAllocator`: exhaustive-search optimizer over venue splits (pure Python, no dependencies)
- **benchmark_strategies.py** - TWAP, VWAP, POV, Almgren-Chriss and Best-Ask baselines, all recording per-fill details
- **tca.py** - implementation-shortfall decomposition (spread, fees, delay, modeled impact, opportunity cost)
- **market_impact.py** - square-root temporary/permanent market impact model
- **crypto_feed.py** - live L2 books from Binance / Coinbase / Kraken public endpoints (no API key)
- **backtest.py** - backtesting engine: consumes Kafka snapshots, runs the parameter grid search
- **kafka_producer.py** / **docker_kafka.py** - market-data simulator and Kafka/Docker orchestration
- **streamlit_app.py** - interactive web demo: live or synthetic books, depth charts, optimizer, TCA panel, benchmarks
- **tests/** - pytest suite (48 tests)

## Try the demo locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Run the tests:

```bash
pip install pytest
pytest tests/ -q
```

## Full system (with Kafka streaming)

```bash
pip install -r requirements-full.txt

# Terminal 1: start Kafka and the market-data producer
python docker_kafka.py setup
python kafka_producer.py

# Terminal 2: run the backtest
python backtest.py
```

Requires Docker & Docker Compose. The demo above needs neither.

## Project structure

```
allocator.py             Cont-Kukanov allocation engine
benchmark_strategies.py  TWAP / VWAP / Best-Ask baselines
backtest.py              Kafka-backed backtesting engine + parameter search
kafka_producer.py        Simulated market-data feed
docker_kafka.py          Kafka/Docker lifecycle management
config/kafka_config.py   Kafka connection settings
streamlit_app.py         Interactive web demo
backtest_results.json    Results of the historical parameter search
```

## References

- [Cont & Kukanov (2012) - Optimal order placement in limit order markets](https://arxiv.org/pdf/1210.1625)
- [Kafka documentation](https://kafka.apache.org/documentation/)
