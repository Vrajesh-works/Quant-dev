
import sys
import subprocess
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json

def test_kafka_connection():
    print("Testing Kafka connection...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        test_msg = {"test": "connection", "timestamp": time.time()}
        producer.send('test_topic', value=test_msg)
        producer.flush()
        producer.close()
        
        print("✓ Kafka producer test passed")
        
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        consumer.close()
        
        print("✓ Kafka consumer test passed")
        return True
        
    except NoBrokersAvailable:
        print("✗ Kafka is not running or not accessible")
        print("Please start Kafka using: ./setup_kafka.sh")
        return False
    except Exception as e:
        print(f"✗ Kafka test failed: {e}")
        return False

def test_data_file():
    print("Testing market data file...")
    
    try:
        import pandas as pd
        df = pd.read_csv('data/l1_day.csv')
        print(f"✓ Data file loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        
        required_cols = ['ts_event', 'publisher_id', 'ask_px_00', 'ask_sz_00']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"✗ Missing required columns: {missing_cols}")
            return False
        else:
            print("✓ All required columns present")
            return True
            
    except FileNotFoundError:
        print("✗ Data file 'data/l1_day.csv' not found")
        print("Please download l1_day.csv and place it in the data/ directory")
        return False
    except Exception as e:
        print(f"✗ Data file test failed: {e}")
        return False

def test_allocator():
    print("Testing Cont-Kukanov allocator...")
    
    try:
        from allocator import ContKukanovAllocator, Venue
        
        allocator = ContKukanovAllocator(
            lambda_over=0.5,
            lambda_under=0.7,
            theta_queue=0.3
        )
        
        venues = [
            Venue(id="1", ask=50.00, ask_size=1000),
            Venue(id="2", ask=50.01, ask_size=800),
            Venue(id="3", ask=49.99, ask_size=1200),
        ]
        
        allocation, cost = allocator.allocate(2000, venues)
        
        print(f"✓ Allocator test passed")
        print(f"  Allocation: {allocation}")
        print(f"  Expected cost: ${cost:.2f}")
        
        return True
        
    except Exception as e:
        print(f"✗ Allocator test failed: {e}")
        return False

def test_benchmarks():
    print("Testing benchmark strategies...")
    
    try:
        from benchmark_strategies import BenchmarkStrategies
        from allocator import Venue
        
        benchmarks = BenchmarkStrategies()
        
        venues = [
            Venue(id="1", ask=50.00, ask_size=1000),
            Venue(id="2", ask=50.01, ask_size=800),
            Venue(id="3", ask=49.99, ask_size=1200),
        ]
        
        target_shares = 1500
        
        result = benchmarks.naive_best_ask(target_shares, venues.copy())
        print(f"✓ Best Ask: {result.shares_filled} shares, ${result.avg_fill_px:.4f} avg price")
        
        result = benchmarks.vwap_strategy(target_shares, venues.copy())
        print(f"✓ VWAP: {result.shares_filled} shares, ${result.avg_fill_px:.4f} avg price")
        
        result = benchmarks.twap_strategy(target_shares, venues.copy())
        print(f"✓ TWAP: {result.shares_filled} shares, ${result.avg_fill_px:.4f} avg price")
        
        return True
        
    except Exception as e:
        print(f"✗ Benchmark test failed: {e}")
        return False

def run_integration_test():
    print("Running integration test...")
    
    try:
        def producer_thread():
            from kafka_producer import MarketDataStreamer
            streamer = MarketDataStreamer('data/l1_day.csv')
            
            df = streamer.load_and_filter_data()
            snapshots = streamer.create_venue_snapshots(df)
            
            for i in range(min(5, len(snapshots))):
                streamer.producer.send(streamer.topic, value=snapshots[i])
                time.sleep(0.5)
            
            streamer.producer.flush()
            streamer.close()
        
        producer = threading.Thread(target=producer_thread)
        producer.start()
        
        consumer = KafkaConsumer(
            'mock_l1_stream',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000
        )
        
        messages_received = 0
        for message in consumer:
            messages_received += 1
            print(f"  Received snapshot at {message.value['timestamp']}")
            if messages_received >= 3:  # Just test a few messages
                break
        
        consumer.close()
        producer.join()
        
        if messages_received > 0:
            print(f"✓ Integration test passed: {messages_received} messages")
            return True
        else:
            print("✗ Integration test failed: no messages received")
            return False
            
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        return False

def main():
    print("Smart Order Router System Tests")
    print("=" * 40)
    
    tests = [
        ("Kafka Connection", test_kafka_connection),
        ("Market Data File", test_data_file),
        ("Allocator Logic", test_allocator),
        ("Benchmark Strategies", test_benchmarks),
        ("Integration Test", run_integration_test),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 20)
        
        if test_func():
            passed += 1
        else:
            print(f"Test '{test_name}' failed. Please fix before proceeding.")
    
    print("\n" + "=" * 40)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("✓ All tests passed! System ready for backtesting.")
        print("\nTo run the full backtest:")
        print("1. Start producer: python kafka_producer.py")
        print("2. Start backtest: python backtest.py")
    else:
        print("✗ Some tests failed. Please fix issues before running backtest.")
        sys.exit(1)

if __name__ == "__main__":
    main()