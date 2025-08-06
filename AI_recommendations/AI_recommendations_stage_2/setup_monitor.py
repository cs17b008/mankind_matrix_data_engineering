"""
Setup Topics and Monitor Scripts for Kafka AI Recommendation System
"""

import json
import time
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
}

TOPICS = {
    'user_events': 'user-events',
    'recommendations': 'recommendations',
    'metrics': 'metrics'
}

def setup_kafka_topics():
    """
    Create necessary Kafka topics for the recommendation system
    """
    print("Setting up Kafka topics...")
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers']
    )
    
    # Define topics with their configurations
    topics_to_create = [
        NewTopic(
            name=TOPICS['user_events'], 
            num_partitions=3, 
            replication_factor=1,
            topic_configs={'retention.ms': '86400000'}  # 24 hours retention
        ),
        NewTopic(
            name=TOPICS['recommendations'], 
            num_partitions=3, 
            replication_factor=1,
            topic_configs={'retention.ms': '86400000'}
        ),
        NewTopic(
            name=TOPICS['metrics'], 
            num_partitions=1, 
            replication_factor=1,
            topic_configs={'retention.ms': '604800000'}  # 7 days retention
        )
    ]
    
    created_topics = []
    existing_topics = []
    
    for topic in topics_to_create:
        try:
            admin_client.create_topics([topic])
            created_topics.append(topic.name)
            print(f"✅ Created topic: {topic.name}")
        except TopicAlreadyExistsError:
            existing_topics.append(topic.name)
            print(f"ℹ️ Topic already exists: {topic.name}")
        except Exception as e:
            print(f"❌ Error creating topic {topic.name}: {e}")
    
    print(f"\nTopic Setup Complete!")
    print(f"Created: {len(created_topics)} topics")
    print(f"Already existed: {len(existing_topics)} topics")
    
    # List all topics
    try:
        metadata = admin_client.describe_topics(list(TOPICS.values()))
        print(f"\nTopic Details:")
        for topic_name, topic_metadata in metadata.items():
            print(f"  {topic_name}: {len(topic_metadata.partitions)} partitions")
    except Exception as e:
        print(f"Could not describe topics: {e}")
    
    admin_client.close()


def monitor_recommendations():
    """
    Monitor recommendation and metrics topics
    """
    print("🔍 Starting Recommendation Monitor...")
    print("Monitoring topics: recommendations, metrics")
    print("Press Ctrl+C to stop\n")
    
    consumer = KafkaConsumer(
        TOPICS['recommendations'],
        TOPICS['metrics'],
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        auto_offset_reset='latest',  # Only show new messages
        value_deserializer=KAFKA_CONFIG['value_deserializer'],
        group_id='monitor-group'
    )
    
    message_count = 0
    start_time = time.time()
    
    try:
        for message in consumer:
            data = message.value
            event_type = data.get('event_type', 'unknown')
            timestamp = data.get('timestamp', time.time())
            message_count += 1
            
            # Format timestamp
            time_str = time.strftime('%H:%M:%S', time.localtime(timestamp))
            
            if event_type == 'recommendations_generated':
                print(f"\n🎯 [{time_str}] Recommendations for User {data.get('user_id', 'Unknown')}")
                print(f"   Viewed: {data.get('viewed_product_name', 'Unknown Product')}")
                print(f"   Processing Time: {data.get('processing_time_ms', 0):.2f}ms")
                print(f"   Has Profile: {'Yes' if data.get('has_user_profile') else 'No'}")
                print(f"   Recommendations ({data.get('recommendation_count', 0)}):")
                
                for i, rec in enumerate(data.get('recommendations', []), 1):
                    price = rec.get('price', 0)
                    category = rec.get('category', 'Unknown')
                    print(f"     {i}. {rec.get('name', 'Unknown')} - ${price} ({category})")
            
            elif event_type == 'purchase_confirmed':
                print(f"\n💰 [{time_str}] Purchase Confirmed")
                print(f"   User {data.get('user_id', 'Unknown')} bought: {data.get('product_name', 'Unknown')}")
                print(f"   Price: ${data.get('price', 0)}")
            
            elif event_type == 'metrics_update':
                metrics = data.get('metrics', {})
                print(f"\n📊 [{time_str}] Metrics Update")
                print(f"   Precision: {metrics.get('precision', 0):.4f}")
                print(f"   Recall: {metrics.get('recall', 0):.4f}")
                print(f"   F1 Score: {metrics.get('f1', 0):.4f}")
                print(f"   Events Processed: {metrics.get('events_processed', 0)}")
                print(f"   Events/Second: {metrics.get('events_per_second', 0):.2f}")
                print(f"   Active Sessions: {metrics.get('active_user_sessions', 0)}")
            
            # Show progress every 20 messages
            if message_count % 20 == 0:
                runtime = time.time() - start_time
                print(f"\n📈 Monitor Stats: {message_count} messages in {runtime:.1f}s")
            
            print("-" * 50)
                
    except KeyboardInterrupt:
        print("\n⚠️ Monitor stopped by user")
    finally:
        consumer.close()
        runtime = time.time() - start_time
        print(f"\n📊 Final Monitor Stats:")
        print(f"Total Messages: {message_count}")
        print(f"Runtime: {runtime:.2f} seconds")
        print(f"Messages/Second: {message_count/runtime:.2f}")
        print("Monitor shutdown complete! 👋")


def monitor_user_events():
    """
    Monitor user events topic to see what events are being produced
    """
    print("👥 Starting User Events Monitor...")
    print("Monitoring topic: user-events")
    print("Press Ctrl+C to stop\n")
    
    consumer = KafkaConsumer(
        TOPICS['user_events'],
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        auto_offset_reset='latest',
        value_deserializer=KAFKA_CONFIG['value_deserializer'],
        group_id='events-monitor-group'
    )
    
    event_count = 0
    start_time = time.time()
    
    try:
        for message in consumer:
            data = message.value
            event_type = data.get('event_type', 'unknown')
            timestamp = data.get('timestamp', time.time())
            event_count += 1
            
            time_str = time.strftime('%H:%M:%S', time.localtime(timestamp))
            
            if event_type == 'product_view':
                user_id = data.get('user_id', 'Anonymous')
                product = data.get('product', {})
                product_name = product.get('name', 'Unknown Product')
                category = product.get('category', 'Unknown')
                price = product.get('price', 0)
                
                print(f"👁️ [{time_str}] View Event - User {user_id}")
                print(f"   Product: {product_name} (${price})")
                print(f"   Category: {category}")
                
            elif event_type == 'product_purchase':
                user_id = data.get('user_id', 'Anonymous')
                product = data.get('product', {})
                product_name = product.get('name', 'Unknown Product')
                price = product.get('price', 0)
                
                print(f"🛒 [{time_str}] Purchase Event - User {user_id}")
                print(f"   Product: {product_name} (${price})")
            
            if event_count % 10 == 0:
                runtime = time.time() - start_time
                print(f"\n📈 Events Monitor: {event_count} events in {runtime:.1f}s")
            
            print("-" * 40)
                
    except KeyboardInterrupt:
        print("\n⚠️ Events monitor stopped by user")
    finally:
        consumer.close()
        runtime = time.time() - start_time
        print(f"\n📊 Final Events Monitor Stats:")
        print(f"Total Events: {event_count}")
        print(f"Runtime: {runtime:.2f} seconds")
        print(f"Events/Second: {event_count/runtime:.2f}")
        print("Events monitor shutdown complete! 👋")


def check_kafka_connection():
    """
    Check if Kafka is running and accessible
    """
    print("🔌 Checking Kafka connection...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers']
        )
        
        # Try to list topics
        topics = admin_client.list_topics()
        print(f"✅ Connected to Kafka successfully!")
        print(f"Available topics: {list(topics)}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print("Make sure Kafka is running on localhost:9092")
        return False


def main():
    """
    Main function with menu options
    """
    print("Kafka AI Recommendation System - Setup & Monitor")
    print("=" * 50)
    
    # Check Kafka connection first
    if not check_kafka_connection():
        print("\nPlease start Kafka and try again.")
        return
    
    while True:
        print("\nChoose an option:")
        print("1. Setup Kafka topics")
        print("2. Monitor recommendations and metrics")
        print("3. Monitor user events")
        print("4. Check Kafka connection")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            setup_kafka_topics()
        elif choice == "2":
            monitor_recommendations()
        elif choice == "3":
            monitor_user_events()
        elif choice == "4":
            check_kafka_connection()
        elif choice == "5":
            print("Goodbye! 👋")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()