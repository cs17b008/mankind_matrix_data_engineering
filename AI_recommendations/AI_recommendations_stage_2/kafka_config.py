# kafka_config.py
import json



KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'ai-recommendation-group',
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
}

TOPICS = {
    'user_events': 'user-events',
    'recommendations': 'recommendations',
    'metrics': 'metrics'
}