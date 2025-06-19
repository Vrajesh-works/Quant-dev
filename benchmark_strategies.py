from typing import List, Dict, Tuple
from dataclasses import dataclass
import time
from allocator import Venue

@dataclass
class ExecutionResult:
    total_cash: float
    shares_filled: int
    avg_fill_px: float
    execution_time: float

class BenchmarkStrategies:
    #Implementation of benchmark strategies for comparison
    
    
    def __init__(self):
        pass
    
    def naive_best_ask(self, target_shares: int, venues: List[Venue]) -> ExecutionResult:
      
        start_time = time.time()
        
        total_cash = 0.0
        shares_filled = 0
        
        remaining_shares = target_shares
        
        while remaining_shares > 0 and venues:
            best_venue = min(venues, key=lambda v: v.ask)
            
            shares_to_buy = min(remaining_shares, best_venue.ask_size)
            
            if shares_to_buy > 0:
                cost = shares_to_buy * (best_venue.ask + best_venue.fee)
                total_cash += cost
                shares_filled += shares_to_buy
                remaining_shares -= shares_to_buy
                
                best_venue.ask_size -= shares_to_buy
                if best_venue.ask_size <= 0:
                    venues.remove(best_venue)
            else:
                break
        
        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time
        
        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)
    
    def twap_strategy(self, target_shares: int, venues: List[Venue], 
                     duration_seconds: int = 60) -> ExecutionResult:

        # TWAP: Time-Weighted Average Price over specified duration

        start_time = time.time()
        
        num_intervals = 10  # Split into 10 equal time intervals
        shares_per_interval = target_shares // num_intervals
        interval_duration = duration_seconds / num_intervals
        
        total_cash = 0.0
        shares_filled = 0
        
        for interval in range(num_intervals):
            if not venues:
                break
                
            shares_to_buy = shares_per_interval
            if interval == num_intervals - 1:  # Last interval gets remainder
                shares_to_buy = target_shares - shares_filled
            
            available_venues = [v for v in venues if v.ask_size > 0]
            if available_venues:
                best_venue = min(available_venues, key=lambda v: v.ask)
                executable_shares = min(shares_to_buy, best_venue.ask_size)
                
                if executable_shares > 0:
                    cost = executable_shares * (best_venue.ask + best_venue.fee)
                    total_cash += cost
                    shares_filled += executable_shares
                    best_venue.ask_size -= executable_shares
            
            time.sleep(0.01)  # Small delay to simulate time passage
        
        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time
        
        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)
    
    def vwap_strategy(self, target_shares: int, venues: List[Venue]) -> ExecutionResult:
        # VWAP: Volume-Weighted Average Price

        start_time = time.time()
        
        total_volume = sum(venue.ask_size for venue in venues)
        
        if total_volume == 0:
            return ExecutionResult(0.0, 0, 0.0, time.time() - start_time)
        
        total_cash = 0.0
        shares_filled = 0
        
        for venue in venues:
            if venue.ask_size > 0:
                volume_proportion = venue.ask_size / total_volume
                shares_to_buy = min(
                    int(target_shares * volume_proportion),
                    venue.ask_size
                )
                
                if shares_to_buy > 0:
                    cost = shares_to_buy * (venue.ask + venue.fee)
                    total_cash += cost
                    shares_filled += shares_to_buy
        
        remaining_shares = target_shares - shares_filled
        if remaining_shares > 0:
            available_venues = [v for v in venues if v.ask_size > shares_filled]
            if available_venues:
                best_venue = min(available_venues, key=lambda v: v.ask)
                executable_shares = min(remaining_shares, best_venue.ask_size)
                
                if executable_shares > 0:
                    cost = executable_shares * (best_venue.ask + best_venue.fee)
                    total_cash += cost
                    shares_filled += executable_shares
        
        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time
        
        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)
    
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
    result = benchmarks.naive_best_ask(target_shares, test_venues.copy())
    print(f"Best Ask: ${result.total_cash:.2f}, {result.shares_filled} shares, avg ${result.avg_fill_px:.4f}")
    
    # TWAP
    result = benchmarks.twap_strategy(target_shares, test_venues.copy())
    print(f"TWAP: ${result.total_cash:.2f}, {result.shares_filled} shares, avg ${result.avg_fill_px:.4f}")
    
    # VWAP
    result = benchmarks.vwap_strategy(target_shares, test_venues.copy())
    print(f"VWAP: ${result.total_cash:.2f}, {result.shares_filled} shares, avg ${result.avg_fill_px:.4f}")

if __name__ == "__main__":
    test_benchmarks()