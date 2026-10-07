"""Benchmark guarantees: every strategy must fill when liquidity exists,
never invent shares, and record fills for TCA."""
import copy

import pytest

from allocator import Venue
from benchmark_strategies import BenchmarkStrategies


@pytest.fixture
def venues():
    return [
        Venue("A", ask=50.00, ask_size=2000, fee=0.003, rebate=0.002, bid=49.99),
        Venue("B", ask=49.98, ask_size=1500, fee=0.005, rebate=0.001, bid=49.97),
        Venue("C", ask=50.05, ask_size=3000, fee=0.001, rebate=0.003, bid=50.04),
    ]


@pytest.fixture
def bench():
    return BenchmarkStrategies()


STRATEGIES = ["naive_best_ask", "twap_strategy", "vwap_strategy",
              "pov_strategy", "almgren_chriss_strategy"]


@pytest.mark.parametrize("name", STRATEGIES)
def test_full_fill_when_liquidity_suffices(bench, venues, name):
    r = getattr(bench, name)(3000, copy.deepcopy(venues))
    assert r.shares_filled == 3000, f"{name} underfilled"


@pytest.mark.parametrize("name", STRATEGIES)
def test_fills_sum_to_reported(bench, venues, name):
    r = getattr(bench, name)(3000, copy.deepcopy(venues))
    assert sum(f["shares"] for f in r.fills) == r.shares_filled
    cash = sum(f["shares"] * f["price"] for f in r.fills)
    assert cash == pytest.approx(r.total_cash, rel=1e-9)


@pytest.mark.parametrize("name", STRATEGIES)
def test_no_negative_or_zero_price_fills(bench, venues, name):
    r = getattr(bench, name)(3000, copy.deepcopy(venues))
    for f in r.fills:
        assert f["shares"] > 0
        assert f["price"] > 0
        assert f["mid"] > 0


@pytest.mark.parametrize("name", STRATEGIES)
def test_thin_liquidity_never_overfills(bench, name):
    thin = [Venue("A", ask=50.0, ask_size=400, fee=0.003, bid=49.99)]
    r = getattr(bench, name)(3000, copy.deepcopy(thin))
    assert r.shares_filled <= 400
    assert r.shares_filled == sum(f["shares"] for f in r.fills)


def test_calculate_savings_bps_direction(bench):
    # Cheaper optimized cost -> positive savings.
    assert bench.calculate_savings_bps(9900, 10000, 100) > 0
    assert bench.calculate_savings_bps(10100, 10000, 100) < 0
    assert bench.calculate_savings_bps(10000, 0, 100) == 0.0


def test_pov_respects_participation_cap(bench, venues):
    orig_size = {v.id: v.ask_size for v in venues}
    r = bench.pov_strategy(3000, copy.deepcopy(venues), participation_rate=0.1,
                           num_slices=5)
    # No single fill should exceed 10% of its own venue's displayed size.
    for f in r.fills:
        assert f["shares"] <= max(int(orig_size[f["venue_id"]] * 0.1), 1)


def test_ac_low_urgency_approaches_twap(bench, venues):
    chill = bench.almgren_chriss_strategy(3000, copy.deepcopy(venues),
                                         risk_aversion=0.01)
    twap = bench.twap_strategy(3000, copy.deepcopy(venues))
    # Both should fill fully and land in the same cost neighborhood.
    assert chill.shares_filled == twap.shares_filled == 3000
    assert abs(chill.total_cash - twap.total_cash) / twap.total_cash < 0.05
