import json
import time
from typing import Dict, List, Tuple
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import threading
from dataclasses import asdict

from config.kafka_config import KAFKA_CONFIG
from allocator import ContKukanovAllocator, Venue, create_venues_from_snapshot
from benchmark_strategies import BenchmarkStrategies, ExecutionResult

class SORBacktester:
    #Smart Order Router Backtesting Engine

    
    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_CONFIG['topic_name'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='sor_backtest_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.target_shares = 5000
        self.snapshots_received = []
        self.running = False
        
        self.benchmarks = BenchmarkStrategies()
        
    def parameter_search(self) -> Tuple[Dict, float]:

 
        print("Starting parameter search...")
        
        lambda_over_values = [0.2, 0.4, 0.6, 0.8, 1.0]
        lambda_under_values = [0.3, 0.5, 0.7, 0.9, 1.1]
        theta_queue_values = [0.1, 0.3, 0.5, 0.7, 0.9]
        
        best_params = {}
        best_cost = float('inf')
        best_result = None
        
        total_combinations = len(lambda_over_values) * len(lambda_under_values) * len(theta_queue_values)
        current_combination = 0
        
        for lambda_over in lambda_over_values:
            for lambda_under in lambda_under_values:
                for theta_queue in theta_queue_values:
                    current_combination += 1
                    
                    allocator = ContKukanovAllocator(lambda_over, lambda_under, theta_queue)
                    result = self._test_strategy(allocator)
                    
                    if result and result.total_cash < best_cost:
                        best_cost = result.total_cash
                        best_params = {
                            'lambda_over': lambda_over,
                            'lambda_under': lambda_under,
                            'theta_queue': theta_queue
                        }
                        best_result = result
                    
                    if current_combination % 10 == 0:
                        print(f"Tested {current_combination}/{total_combinations} combinations...")
        
        print(f"Parameter search complete. Best cost: ${best_cost:.2f}")
        return best_params, best_result
    
    def _test_strategy(self, allocator: ContKukanovAllocator) -> ExecutionResult:

        
   
        if not self.snapshots_received:
            return None
        
        total_cash = 0.0
        shares_filled = 0
        start_time = time.time()
        
        remaining_shares = self.target_shares
        
        for snapshot in self.snapshots_received:
            if remaining_shares <= 0:
                break
            
            venues = []
            for venue_data in snapshot['venues']:
                if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                    venues.append(Venue(
                        id=str(venue_data['publisher_id']),
                        ask=venue_data['ask_px_00'],
                        ask_size=venue_data['ask_sz_00']
                    ))
            
            if not venues:
                continue
            
            try:
                allocation, expected_cost = allocator.allocate(
                    min(remaining_shares, self.target_shares), venues
                )
                
                for i, venue in enumerate(venues):
                    if i < len(allocation) and allocation[i] > 0:
                        shares_to_buy = min(allocation[i], venue.ask_size, remaining_shares)
                        if shares_to_buy > 0:
                            cost = shares_to_buy * (venue.ask + venue.fee)
                            total_cash += cost
                            shares_filled += shares_to_buy
                            remaining_shares -= shares_to_buy
            
            except Exception as e:
                continue  
        
        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time
        
        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)
    
    def run_benchmarks(self) -> Dict:
     
        print("Running benchmark strategies...")
        
        results = {}
        
        if not self.snapshots_received:
            print("No snapshots available for benchmarking")
            return results
        
        first_snapshot = self.snapshots_received[0]
        venues = []
        
        for venue_data in first_snapshot['venues']:
            if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                venues.append(Venue(
                    id=str(venue_data['publisher_id']),
                    ask=venue_data['ask_px_00'],
                    ask_size=venue_data['ask_sz_00']
                ))
        
        if not venues:
            print("No valid venues found for benchmarking")
            return results
        
        # Run each benchmark
        try:
            # Best Ask
            result = self.benchmarks.naive_best_ask(self.target_shares, venues.copy())
            results['best_ask'] = {
                'total_cash': result.total_cash,
                'avg_fill_px': result.avg_fill_px
            }
            
            # TWAP
            result = self.benchmarks.twap_strategy(self.target_shares, venues.copy())
            results['twap'] = {
                'total_cash': result.total_cash,
                'avg_fill_px': result.avg_fill_px
            }
            
            # VWAP
            result = self.benchmarks.vwap_strategy(self.target_shares, venues.copy())
            results['vwap'] = {
                'total_cash': result.total_cash,
                'avg_fill_px': result.avg_fill_px
            }
            
        except Exception as e:
            print(f"Error running benchmarks: {e}")
        
        return results
    
    def consume_market_data(self, timeout_seconds: int = 300):
        # Consume market data from Kafka topic
     
       
        print("Starting to consume market data...")
        self.running = True
        start_time = time.time()
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                if time.time() - start_time > timeout_seconds:
                    print("Timeout reached, stopping consumption")
                    break
                
                snapshot = message.value
                self.snapshots_received.append(snapshot)
                
                if len(self.snapshots_received) % 50 == 0:
                    print(f"Received {len(self.snapshots_received)} snapshots")
                
        except KeyboardInterrupt:
            print("Data consumption interrupted by user")
        except Exception as e:
            print(f"Error consuming data: {e}")
        finally:
            self.running = False
            self.consumer.close()
    
    def run_backtest(self) -> Dict:
        #Run complete backtesting process
    
        print("Starting Smart Order Router Backtest")
        print(f"Target shares: {self.target_shares}")
        
        consumer_thread = threading.Thread(
            target=self.consume_market_data,
            args=(60,)  # 1 minute timeout
        )
        consumer_thread.start()
        
        time.sleep(10)
        
        if not self.snapshots_received:
            print("No data received, cannot proceed with backtest")
            return {}
        
        print(f"Received {len(self.snapshots_received)} market snapshots")
        
        best_params, optimized_result = self.parameter_search()
        
        benchmark_results = self.run_benchmarks()
        
        savings = {}
        if optimized_result and benchmark_results:
            benchmarks_obj = BenchmarkStrategies()
            for strategy_name, bench_result in benchmark_results.items():
                savings[strategy_name] = benchmarks_obj.calculate_savings_bps(
                    optimized_result.total_cash,
                    bench_result['total_cash'],
                    optimized_result.shares_filled
                )
        
        final_results = {
            "best_parameters": best_params,
            "optimized": {
                "total_cash": optimized_result.total_cash if optimized_result else 0,
                "avg_fill_px": optimized_result.avg_fill_px if optimized_result else 0
            },
            "baselines": benchmark_results,
            "savings_vs_baselines_bps": savings
        }
        
        return final_results

def main():
    backtester = SORBacktester()
    
    try:
        results = backtester.run_backtest()
        
        if results:
            print("\n" + "="*50)
            print("FINAL RESULTS:")
            print("="*50)
            print(json.dumps(results, indent=2))
            
            with open('backtest_results.json', 'w') as f:
                json.dump(results, f, indent=2)
            
            print("\nResults saved to backtest_results.json")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()