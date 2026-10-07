# Smart Order Router (SOR)

Optimal order execution across multiple trading venues using the Cont-Kukanov cost model,
with real-time market data streaming via Kafka and a backtesting engine.

**Live interactive demo:** [SOR Lab on Streamlit](https://quant-dev.streamlit.app/)
_(run the allocator, tune the risk parameters, and compare against TWAP / VWAP / Best-Ask, all in the browser)_

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
- **benchmark_strategies.py** - TWAP, VWAP, and Best-Ask baselines for comparison
- **backtest.py** - backtesting engine: consumes Kafka snapshots, runs the parameter grid search
- **kafka_producer.py** / **docker_kafka.py** - market-data simulator and Kafka/Docker orchestration
- **streamlit_app.py** - interactive web demo: edit the venue book, tune parameters, run the optimizer live

## Try the demo locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Full system (with Kafka streaming)

```bash
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
