from typing import List, Dict, Tuple
from dataclasses import dataclass, field
import math
import time
from allocator import Venue

@dataclass
class ExecutionResult:
    total_cash: float
    shares_filled: int
    avg_fill_px: float
    execution_time: float
    fills: List[Dict] = field(default_factory=list)
    # each fill: {"venue_id", "shares", "price" (all-in), "mid"}

class BenchmarkStrategies:
    #Implementation of benchmark strategies for comparison


    def __init__(self):
        pass

    def naive_best_ask(self, target_shares: int, venues: List[Venue]) -> ExecutionResult:

        start_time = time.time()

        total_cash = 0.0
        shares_filled = 0

        remaining_shares = target_shares
        fills = []

        while remaining_shares > 0 and venues:
            best_venue = min(venues, key=lambda v: v.ask)

            shares_to_buy = min(remaining_shares, best_venue.ask_size)

            if shares_to_buy > 0:
                cost = shares_to_buy * (best_venue.ask + best_venue.fee)
                total_cash += cost
                shares_filled += shares_to_buy
                remaining_shares -= shares_to_buy
                fills.append({"venue_id": best_venue.id, "shares": shares_to_buy,
                              "price": best_venue.ask + best_venue.fee,
                              "mid": best_venue.mid})

                best_venue.ask_size -= shares_to_buy
                if best_venue.ask_size <= 0:
                    venues.remove(best_venue)
            else:
                break

        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time

        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time, fills)

    def twap_strategy(self, target_shares: int, venues: List[Venue],
                     duration_seconds: int = 60) -> ExecutionResult:

        # TWAP: Time-Weighted Average Price over specified duration.
        # Splits the order into equal time slices; within each slice sweeps
        # venues best-ask-first so the full slice is filled.

        start_time = time.time()

        num_intervals = 10  # Split into 10 equal time intervals
        shares_per_interval = target_shares // num_intervals

        total_cash = 0.0
        shares_filled = 0
        fills = []

        for interval in range(num_intervals):
            if interval == num_intervals - 1:  # Last interval gets remainder
                need = target_shares - shares_filled
            else:
                need = shares_per_interval

            while need > 0:
                available_venues = [v for v in venues if v.ask_size > 0]
                if not available_venues:
                    break
                best_venue = min(available_venues, key=lambda v: v.ask)
                executable_shares = min(need, best_venue.ask_size)
                if executable_shares <= 0:
                    break
                total_cash += executable_shares * (best_venue.ask + best_venue.fee)
                shares_filled += executable_shares
                need -= executable_shares
                fills.append({"venue_id": best_venue.id, "shares": executable_shares,
                              "price": best_venue.ask + best_venue.fee,
                              "mid": best_venue.mid})
                best_venue.ask_size -= executable_shares

            time.sleep(0.01)  # Small delay to simulate time passage

        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time

        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time, fills)

    def vwap_strategy(self, target_shares: int, venues: List[Venue]) -> ExecutionResult:
        # VWAP: Volume-Weighted Average Price.
        # Allocates proportionally to displayed size, then sweeps any
        # remainder best-ask-first so the full order is filled.

        start_time = time.time()

        total_volume = sum(venue.ask_size for venue in venues)

        if total_volume == 0:
            return ExecutionResult(0.0, 0, 0.0, time.time() - start_time)

        total_cash = 0.0
        shares_filled = 0
        fills = []

        for venue in venues:
            if venue.ask_size <= 0:
                continue
            volume_proportion = venue.ask_size / total_volume
            executable_shares = min(
                int(target_shares * volume_proportion),
                venue.ask_size
            )

            if executable_shares > 0:
                total_cash += executable_shares * (venue.ask + venue.fee)
                shares_filled += executable_shares
                fills.append({"venue_id": venue.id, "shares": executable_shares,
                              "price": venue.ask + venue.fee, "mid": venue.mid})
                venue.ask_size -= executable_shares

        need = target_shares - shares_filled
        while need > 0:
            available_venues = [v for v in venues if v.ask_size > 0]
            if not available_venues:
                break
            best_venue = min(available_venues, key=lambda v: v.ask)
            executable_shares = min(need, best_venue.ask_size)
            if executable_shares <= 0:
                break
            total_cash += executable_shares * (best_venue.ask + best_venue.fee)
            shares_filled += executable_shares
            need -= executable_shares
            fills.append({"venue_id": best_venue.id, "shares": executable_shares,
                          "price": best_venue.ask + best_venue.fee,
                          "mid": best_venue.mid})
            best_venue.ask_size -= executable_shares

        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time

        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time, fills)

    def pov_strategy(self, target_shares: int, venues: List[Venue],
                     participation_rate: float = 0.2,
                     num_slices: int = 10) -> ExecutionResult:
        # POV: Percentage of Volume. Works the order in equal time slices,
        # taking at most participation_rate of each venue's displayed size
        # per slice, sweeping best-ask-first. Gentle on any single venue,
        # which keeps market impact low at the cost of slower execution.

        start_time = time.time()
        total_cash = 0.0
        shares_filled = 0
        fills = []

        for s in range(num_slices):
            need = (target_shares - shares_filled) if s == num_slices - 1 \
                else target_shares // num_slices
            while need > 0:
                avail = sorted((v for v in venues if v.ask_size > 0),
                               key=lambda v: v.ask)
                if not avail:
                    break
                progressed = False
                for v in avail:
                    if need <= 0:
                        break
                    cap = max(int(v.ask_size * participation_rate), 1)
                    take = min(need, cap, v.ask_size)
                    if take <= 0:
                        continue
                    total_cash += take * (v.ask + v.fee)
                    shares_filled += take
                    need -= take
                    fills.append({"venue_id": v.id, "shares": take,
                                  "price": v.ask + v.fee, "mid": v.mid})
                    v.ask_size -= take
                    progressed = True
                if not progressed:
                    break
            time.sleep(0.01)

        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        return ExecutionResult(total_cash, shares_filled, avg_fill_px,
                               time.time() - start_time, fills)

    def almgren_chriss_strategy(self, target_shares: int, venues: List[Venue],
                                risk_aversion: float = 1.0,
                                daily_volatility: float = 0.02,
                                num_slices: int = 10) -> ExecutionResult:
        # Almgren-Chriss optimal execution trajectory (closed form).
        #
        # Holdings follow x(t) = X * sinh(kappa*(T-t)) / sinh(kappa*T) with
        #   kappa = sqrt(lambda * sigma^2 / eta),
        # the mean-variance optimal tradeoff between market impact (eta)
        # and timing risk (lambda * sigma^2).  lambda -> 0 recovers TWAP;
        # large lambda front-loads execution to cut timing risk.
        #
        # eta is calibrated from the square-root impact model so the
        # trajectory responds to the same liquidity picture as the router.

        from market_impact import ImpactParams, temporary_impact_per_share

        start_time = time.time()
        total_cash = 0.0
        shares_filled = 0
        fills = []

        total_adv = sum(max(v.ask_size * 50, 1) for v in venues)
        avg_px = sum(v.ask * v.ask_size for v in venues) / max(
            sum(v.ask_size for v in venues), 1)
        sigma_slice = daily_volatility / math.sqrt(num_slices)
        # eta: $ cost per share^2, from linearizing temp impact at mean slice
        mean_slice = target_shares / num_slices
        eta = (avg_px * ImpactParams(daily_volatility=daily_volatility).k_temporary
               * daily_volatility * (mean_slice / total_adv) ** 0.5) / max(mean_slice, 1)
        eta = max(eta, 1e-12)

        kappa = math.sqrt(max(risk_aversion, 1e-9) * sigma_slice ** 2 / eta)
        # Holdings at each slice boundary; slice trades = differences.
        # Largest-remainder rounding so slices are non-negative and sum
        # exactly to the target.
        traj = []
        denom = math.sinh(kappa * num_slices)
        for j in range(num_slices + 1):
            t_left = num_slices - j
            x = (target_shares * math.sinh(kappa * t_left) / denom
                 if denom > 0 else target_shares * (num_slices - j) / num_slices)
            traj.append(x)
        exact = [traj[j] - traj[j + 1] for j in range(num_slices)]
        slice_trades = [int(math.floor(x)) for x in exact]
        remainder = target_shares - sum(slice_trades)
        for j in sorted(range(num_slices),
                        key=lambda k: exact[k] - slice_trades[k],
                        reverse=True)[:remainder]:
            slice_trades[j] += 1

        for need in slice_trades:
            while need > 0:
                avail = [v for v in venues if v.ask_size > 0]
                if not avail:
                    break
                best = min(avail, key=lambda v: v.ask)
                take = min(need, best.ask_size)
                if take <= 0:
                    break
                total_cash += take * (best.ask + best.fee)
                shares_filled += take
                need -= take
                fills.append({"venue_id": best.id, "shares": take,
                              "price": best.ask + best.fee, "mid": best.mid})
                best.ask_size -= take
            time.sleep(0.01)

        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        return ExecutionResult(total_cash, shares_filled, avg_fill_px,
                               time.time() - start_time, fills)

    def calculate_savings_bps(self, optimized_cost: float, baseline_cost: float,
                            shares: int) -> float:

        # Calculate savings in basis points

        if shares == 0 or baseline_cost == 0:
            return 0.0

        avg_optimized_px = optimized_cost / shares
        avg_baseline_px = baseline_cost / shares

        savings_per_share = avg_baseline_px - avg_optimized_px
        savings_bps = (savings_per_share / avg_baseline_px) * 10000

        return savings_bps

def test_benchmarks():
    # Create test venues
    test_venues = [
        Venue(id="1", ask=50.00, ask_size=1000, fee=0.003, rebate=0.002),
        Venue(id="2", ask=50.01, ask_size=800, fee=0.003, rebate=0.002),
        Venue(id="3", ask=49.99, ask_size=1200, fee=0.003, rebate=0.002),
    ]

    benchmarks = BenchmarkStrategies()
    target_shares = 2000

    # Test each strategy
    print("Testing Benchmark Strategies:")

    # Best Ask
    import copy
    result = benchmarks.naive_best_ask(target_shares, copy.deepcopy(test_venues))
    print(f"Best Ask: ${result.total_cash:.2f}, {result.shares_filled} shares, avg ${result.avg_fill_px:.4f}")

    # TWAP
    result = benchmarks.twap_strategy(target_shares, copy.deepcopy(test_venues))
    print(f"TWAP: ${result.total_cash:.2f}, {result.shares_filled} shares, avg ${result.avg_fill_px:.4f}")

    # VWAP
    result = benchmarks.vwap_strategy(target_shares, copy.deepcopy(test_venues))
    print(f"VWAP: ${result.total_cash:.2f}, {result.shares_filled} shares, avg ${result.avg_fill_px:.4f}")

if __name__ == "__main__":
    test_benchmarks()
