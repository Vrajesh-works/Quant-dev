# Complete Docker Kafka Helper


import subprocess
import sys
import time
import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

class DockerKafkaManager:
    def __init__(self):
        self.topic_name = "mock_l1_stream"
        
    def run_command(self, command):
        try:
            result = subprocess.run(command.split(), capture_output=True, text=True)
            return result.returncode == 0
        except:
            return False
    
    def is_docker_running(self):
        return self.run_command("docker info")
    
    def start_services(self):
        print("Starting Kafka services...")
        
        if not self.is_docker_running():
            print("Docker is not running. Please start Docker Desktop first.")
            return False
        
        if not self.run_command("docker-compose up -d"):
            print("Failed to start services")
            return False
        
        print("Services started, waiting for Kafka...")
        
        # Wait for Kafka to be ready
        for i in range(30):
            if self.test_kafka_connection():
                print("Kafka is ready!")
                return True
            time.sleep(2)
        
        print("Kafka failed to start")
        return False
    
    def stop_services(self):
        print("Stopping services...")
        if self.run_command("docker-compose down"):
            print("✅ Services stopped")
            return True
        return False
    
    def show_status(self):
        print("Docker Kafka Status:")
        print("-" * 30)
        
        if not self.is_docker_running():
            print("Docker is not running")
            return False
        
        # Check containers
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=kafka", "--filter", "name=zookeeper", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"],
                capture_output=True, text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                print("Container Status:")
                print(result.stdout)
            else:
                print("No Kafka containers running")
                return False
        except:
            print("Failed to check container status")
            return False
        
        # Test Kafka connection
        if self.test_kafka_connection():
            print("Kafka connection: OK")
            print(f"Topic '{self.topic_name}': Available")
        else:
            print("Kafka connection: Failed")
            return False
        
        return True
    
    def test_kafka_connection(self):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers="localhost:9092",
                request_timeout_ms=5000
            )
            admin_client.close()
            return True
        except:
            return False
    
    def create_topic(self):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers="localhost:9092",
                request_timeout_ms=10000
            )
            
            topic = NewTopic(
                name=self.topic_name,
                num_partitions=3,
                replication_factor=1
            )
            
            admin_client.create_topics([topic])
            print(f"Topic '{self.topic_name}' created")
            
        except TopicAlreadyExistsError:
            print(f"Topic '{self.topic_name}' already exists")
        except Exception as e:
            print(f"Failed to create topic: {e}")
            return False
        finally:
            admin_client.close()
        
        return True
    
    def setup(self):
        print("Setting up Kafka for Smart Order Router")
        print("=" * 50)
        
        if not self.start_services():
            return False
        
        if not self.create_topic():
            return False
        
        # Quick test
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            producer.send(self.topic_name, {"test": "setup"})
            producer.flush()
            producer.close()
            print("Setup test passed")
        except Exception as e:
            print(f"Setup test failed: {e}")
            return False
        
        print("\n" + "=" * 50)
        print("Kafka setup complete!")
        print("🔌 Kafka broker: localhost:9092")
        print(f"Topic ready: {self.topic_name}")
        print("\nYou can now run:")
        print("  python kafka_producer.py")
        print("  python backtest.py")
        
        return True

def main():
    manager = DockerKafkaManager()
    
    if len(sys.argv) < 2:
        print("Usage: python docker_kafka.py [command]")
        print("Commands:")
        print("  setup   - Complete setup (start + create topic + test)")
        print("  start   - Start Kafka services")
        print("  stop    - Stop Kafka services") 
        print("  status  - Show service status")
        print("  test    - Test Kafka connection")
        return
    
    command = sys.argv[1].lower()
    
    if command == "setup":
        success = manager.setup()
        sys.exit(0 if success else 1)
    
    elif command == "start":
        success = manager.start_services()
        if success:
            manager.create_topic()
        sys.exit(0 if success else 1)
    
    elif command == "stop":
        success = manager.stop_services()
        sys.exit(0 if success else 1)
    
    elif command == "status":
        success = manager.show_status()
        sys.exit(0 if success else 1)
    
    elif command == "test":
        if manager.test_kafka_connection():
            print("Kafka connection successful")
        else:
            print("Kafka connection failed")
            sys.exit(1)
    
    else:
        print(f"Unknown command: {command}")
        print("Available commands: setup, start, stop, status, test")
        sys.exit(1)

if __name__ == "__main__":
    main()