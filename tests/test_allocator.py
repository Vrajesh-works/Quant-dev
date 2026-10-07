"""Allocator correctness: optimal splits, edge cases, cost model."""
import copy

import pytest

from allocator import ContKukanovAllocator, Venue


@pytest.fixture
def venues():
    return [
        Venue("A", ask=50.00, ask_size=2000, fee=0.003, rebate=0.002, bid=49.99),
        Venue("B", ask=49.98, ask_size=1500, fee=0.005, rebate=0.001, bid=49.97),
        Venue("C", ask=50.05, ask_size=3000, fee=0.001, rebate=0.003, bid=50.04),
    ]


@pytest.fixture
def allocator():
    return ContKukanovAllocator(lambda_over=0.4, lambda_under=0.6, theta_queue=0.3)


def test_split_sums_to_order_size(allocator, venues):
    split, _ = allocator.allocate(3000, copy.deepcopy(venues))
    assert sum(split) == 3000
    assert len(split) == len(venues)


def test_respects_venue_capacity(allocator, venues):
    split, _ = allocator.allocate(3000, copy.deepcopy(venues))
    for q, v in zip(split, venues):
        assert q <= v.ask_size


def test_prefers_cheapest_all_in_venue():
    # No penalties: everything should go to the cheapest all-in venue.
    alloc = ContKukanovAllocator(0.0, 0.0, 0.0)
    venues = [
        Venue("cheap", ask=50.00, ask_size=5000, fee=0.001),
        Venue("pricey", ask=50.10, ask_size=5000, fee=0.001),
    ]
    split, _ = alloc.allocate(1000, venues)
    assert split == [1000, 0]


def test_underfill_penalized_not_ignored(allocator):
    venues = [Venue("A", ask=50.0, ask_size=500, fee=0.003)]
    split, cost = allocator.allocate(2000, copy.deepcopy(venues))
    assert sum(split) == 500  # can only take what exists
    # cost must include the underfill penalty, not just cash
    assert cost > 500 * 50.0


def test_empty_venues_returns_zero():
    alloc = ContKukanovAllocator(0.4, 0.6, 0.3)
    split, cost = alloc.allocate(1000, [])
    assert split == [] or sum(split) == 0


def test_cost_decomposition_matches_total(allocator, venues):
    # _compute_cost is the single source of truth for ranking splits.
    split, cost = allocator.allocate(2000, copy.deepcopy(venues))
    manual = allocator._compute_cost(split, venues, 2000)
    assert cost == pytest.approx(manual)


def test_mid_defaults_to_ask_without_bid():
    v = Venue("X", ask=10.0, ask_size=100)
    assert v.mid == 10.0
    v2 = Venue("Y", ask=10.0, ask_size=100, bid=9.9)
    assert v2.mid == pytest.approx(9.95)


def test_update_parameters(allocator):
    allocator.update_parameters(0.1, 0.2, 0.3)
    assert (allocator.lambda_over, allocator.lambda_under, allocator.theta_queue) == (0.1, 0.2, 0.3)
