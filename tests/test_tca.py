"""TCA decomposition: accounting identities must hold."""
import pytest

from tca import Fill, compute_tca
from market_impact import ImpactParams


def make_fills():
    return [
        Fill("A", 1000, 50.01, 50.00, fee=0.003),
        Fill("B", 1000, 50.03, 50.02, fee=0.005),
    ]


def test_shortfall_matches_definition():
    fills = make_fills()
    rep = compute_tca("X", fills, arrival_price=50.00, end_price=50.00,
                      target_shares=2000, adv_map={"A": 1e6, "B": 1e6})
    expected = sum(f.shares * (f.price - 50.00) for f in fills)
    assert rep.shortfall_dollars == pytest.approx(expected)
    assert rep.shortfall_bps == pytest.approx(expected / (2000 * 50.00) * 10000)
    assert rep.fill_rate == pytest.approx(1.0)


def test_opportunity_cost_on_underfill():
    fills = [Fill("A", 1000, 50.01, 50.00, fee=0.003)]
    rep = compute_tca("X", fills, arrival_price=50.00, end_price=50.10,
                      target_shares=2000, adv_map={"A": 1e6})
    assert rep.opportunity_cost == pytest.approx(1000 * 0.10)
    assert rep.fill_rate == pytest.approx(0.5)


def test_no_opportunity_when_price_falls():
    fills = [Fill("A", 1000, 50.01, 50.00, fee=0.003)]
    rep = compute_tca("X", fills, arrival_price=50.00, end_price=49.90,
                      target_shares=2000, adv_map={"A": 1e6})
    assert rep.opportunity_cost == 0.0


def test_spread_excludes_fees():
    fills = [Fill("A", 1000, 50.01, 50.00, fee=0.003)]
    rep = compute_tca("X", fills, arrival_price=50.00, end_price=50.00,
                      target_shares=1000, adv_map={"A": 1e6})
    # price - fee - mid = 50.01 - 0.003 - 50.00 = 0.007 per share
    assert rep.spread_cost == pytest.approx(1000 * 0.007)
    assert rep.fees == pytest.approx(1000 * 0.003)


def test_empty_fills_zero_shortfall():
    rep = compute_tca("X", [], arrival_price=50.0, end_price=50.0,
                      target_shares=1000, adv_map={})
    assert rep.shortfall_dollars == 0.0
    assert rep.filled_shares == 0


def test_impact_grows_with_size():
    small = [Fill("A", 100, 50.01, 50.00)]
    big = [Fill("A", 10000, 50.01, 50.00)]
    params = ImpactParams()
    r_small = compute_tca("X", small, 50.0, 50.0, 100, {"A": 1e6}, params)
    r_big = compute_tca("X", big, 50.0, 50.0, 10000, {"A": 1e6}, params)
    # per-share modeled impact must increase with size (sqrt model)
    assert (r_big.impact_cost / 10000) > (r_small.impact_cost / 100)
