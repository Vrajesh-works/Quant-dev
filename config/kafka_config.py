# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic_name': 'mock_l1_stream',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'sor_consumer_group',
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
}

import json