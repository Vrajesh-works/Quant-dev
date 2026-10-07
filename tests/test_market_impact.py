"""Market impact model: monotonicity, edge cases, calibration sanity."""
import pytest

from allocator import Venue
from market_impact import (ImpactParams, apply_impact_to_split, default_adv,
                           effective_price, permanent_impact_per_share,
                           temporary_impact_per_share)


def test_temporary_impact_monotone_in_size():
    p = ImpactParams()
    small = temporary_impact_per_share(100, 1_000_000, p)
    big = temporary_impact_per_share(10_000, 1_000_000, p)
    assert 0 < small < big


def test_impact_falls_with_adv():
    p = ImpactParams()
    thin = temporary_impact_per_share(1000, 100_000, p)
    deep = temporary_impact_per_share(1000, 10_000_000, p)
    assert thin > deep > 0


def test_zero_inputs_zero_impact():
    p = ImpactParams()
    assert temporary_impact_per_share(0, 1e6, p) == 0.0
    assert temporary_impact_per_share(100, 0, p) == 0.0
    assert permanent_impact_per_share(0, 1e6, p) == 0.0


def test_effective_price_above_quoted():
    p = ImpactParams()
    v = Venue("A", ask=50.0, ask_size=10_000, fee=0.01, bid=49.99)
    px = effective_price(v, 5000, adv=1_000_000, params=p)
    assert px > 50.01  # quoted all-in
    # impact for 5000 @ ADV 1e6, sigma 2%: 0.5*0.02*sqrt(0.005) ~= 7.07 bps
    assert px == pytest.approx(50.01 + 50.0 * 0.5 * 0.02 * (5000 / 1e6) ** 0.5)


def test_apply_impact_split_consistency():
    p = ImpactParams()
    venues = [Venue("A", 50.0, 5000, 0.01, bid=49.99),
              Venue("B", 50.02, 5000, 0.01, bid=50.01)]
    out = apply_impact_to_split(venues, [2500, 2500],
                                {"A": 1e6, "B": 1e6}, p)
    assert out["total_cash"] > sum(q * (v.ask + v.fee)
                                   for q, v in zip([2500, 2500], venues))
    assert out["impact_cost"] == pytest.approx(
        out["total_cash"] - sum(q * (v.ask + v.fee)
                                for q, v in zip([2500, 2500], venues)))
    assert len(out["per_venue"]) == 2


def test_default_adv_scales_with_size():
    small = Venue("S", 10.0, 100, 0.01)
    big = Venue("B", 10.0, 10_000, 0.01)
    assert default_adv(big) > default_adv(small) > 0
