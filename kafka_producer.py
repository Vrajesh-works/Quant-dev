import pandas as pd
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from config.kafka_config import KAFKA_CONFIG

class MarketDataStreamer:
# Kafka producer to stream market data from l1_day.csv
    
    def __init__(self, data_file: str):
        self.data_file = data_file
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=KAFKA_CONFIG['value_serializer']
        )
        self.topic = KAFKA_CONFIG['topic_name']
        
    def load_and_filter_data(self):
        print("Loading market data...")
        df = pd.read_csv(self.data_file)
        
        df['ts_event'] = pd.to_datetime(df['ts_event'])
        df['ts_recv'] = pd.to_datetime(df['ts_recv'])
        
        start_time = pd.Timestamp('13:36:32').time()
        end_time = pd.Timestamp('13:45:14').time()
        
        mask = (df['ts_event'].dt.time >= start_time) & (df['ts_event'].dt.time <= end_time)
        filtered_df = df[mask].copy()
        
        filtered_df = filtered_df.sort_values('ts_event')
        
        print(f"Filtered data: {len(filtered_df)} records")
        print(f"Time range: {filtered_df['ts_event'].min()} to {filtered_df['ts_event'].max()}")
        
        return filtered_df
    
    def create_venue_snapshots(self, df):
        snapshots = []
        
        for timestamp, group in df.groupby('ts_event'):
            snapshot = {
                'timestamp': timestamp.isoformat(),
                'venues': []
            }
            
            for _, row in group.iterrows():
                venue_data = {
                    'publisher_id': int(row['publisher_id']),
                    'ask_px_00': float(row['ask_px_00']) if pd.notna(row['ask_px_00']) else 0.0,
                    'ask_sz_00': int(row['ask_sz_00']) if pd.notna(row['ask_sz_00']) else 0,
                    'bid_px_00': float(row['bid_px_00']) if pd.notna(row['bid_px_00']) else 0.0,
                    'bid_sz_00': int(row['bid_sz_00']) if pd.notna(row['bid_sz_00']) else 0
                }
                snapshot['venues'].append(venue_data)
            
            snapshots.append(snapshot)
        
        return snapshots
    
    def stream_data(self, simulation_speed: float = 1.0):
        # Stream market data to Kafka topic

        df = self.load_and_filter_data()
        snapshots = self.create_venue_snapshots(df)
        
        print(f"Starting to stream {len(snapshots)} snapshots...")
        print(f"Simulation speed: {simulation_speed}x")
        
        prev_timestamp = None
        
        for i, snapshot in enumerate(snapshots):
            current_timestamp = pd.to_datetime(snapshot['timestamp'])
            
            if prev_timestamp is not None:
                time_diff = (current_timestamp - prev_timestamp).total_seconds()
                sleep_time = time_diff / simulation_speed
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            try:
                self.producer.send(self.topic, value=snapshot)
                if i % 100 == 0:
                    print(f"Sent snapshot {i+1}/{len(snapshots)} - {snapshot['timestamp']}")
                    
            except Exception as e:
                print(f"Error sending snapshot: {e}")
            
            prev_timestamp = current_timestamp
        
        # Ensure all messages are sent
        self.producer.flush()
        print("Finished streaming all data")
    
    def close(self):
        self.producer.close()

def main():
    streamer = MarketDataStreamer('data/l1_day.csv')
    
    try:
        streamer.stream_data(simulation_speed=10.0)
    except KeyboardInterrupt:
        print("Streaming interrupted by user")
    finally:
        streamer.close()

if __name__ == "__main__":
    main()