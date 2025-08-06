"""
Kafka Producer - Event Simulator
Sends user events to Kafka for the AI recommendation system to consume
"""

import json
import time
import random
import os
from kafka import KafkaProducer
from typing import Dict, List, Any

# Import from your existing code
from AiRecommendation_1 import (
    UserProfile, create_simulated_users, load_product_catalog, 
    create_dummy_product_catalog, AIEngine, MetricsTracker
)

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'value_serializer': lambda x: json.dumps(x).encode('utf-8')
}

TOPICS = {
    'user_events': 'user-events',
    'recommendations': 'recommendations',
    'metrics': 'metrics'
}

class KafkaEventProducer:
    """
    Kafka producer that simulates user events and sends them to Kafka
    """
    
    def __init__(self, product_catalog: List[Dict[str, Any]], users: List[UserProfile]):
        """
        Initialize the Kafka event producer
        
        Args:
            product_catalog: List of product dictionaries
            users: List of UserProfile objects
        """
        self.products = product_catalog
        self.users = users or []
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=KAFKA_CONFIG['value_serializer']
        )
        
        print(f"Kafka Producer initialized with {len(self.products)} products and {len(self.users)} users")
    
    def create_user_event(self, event_type: str, user: UserProfile, product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a user event dictionary
        
        Args:
            event_type: Type of event ('product_view' or 'product_purchase')
            user: UserProfile object
            product: Product dictionary
            
        Returns:
            Event data dictionary
        """
        return {
            'event_type': event_type,
            'timestamp': time.time(),
            'user_id': user.user_id if user else None,
            'product_id': product['id'],
            'product': product,
            'user_profile': {
                'preferred_categories': user.preferred_categories,
                'preferred_features': user.preferred_features,
                'price_sensitivity': user.price_sensitivity,
                'viewing_history': user.get_viewing_history(),
                'purchase_history': user.get_purchase_history()
            } if user else None
        }
    
    def send_event(self, event_data: Dict[str, Any]):
        """
        Send an event to Kafka
        
        Args:
            event_data: Event dictionary to send
        """
        try:
            future = self.producer.send(TOPICS['user_events'], event_data)
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Event sent: {event_data['event_type']} - "
                  f"User {event_data['user_id']} - "
                  f"Product: {event_data['product']['name']}")
                  
        except Exception as e:
            print(f"❌ Error sending event: {e}")
    
    def simulate_user_events(self, duration_seconds: int = 120, interval_seconds: float = 3.0):
        """
        Simulate user events for a specified duration
        
        Args:
            duration_seconds: Total simulation time in seconds
            interval_seconds: Time between events in seconds
        """
        start_time = time.time()
        end_time = start_time + duration_seconds
        event_count = 0
        
        print(f"\n{'=' * 60}")
        print(f"Starting Kafka Event Simulation")
        print(f"Duration: {duration_seconds} seconds")
        print(f"Interval: {interval_seconds} seconds between events")
        print(f"{'=' * 60}\n")
        
        try:
            while time.time() < end_time:
                # Select a random user and product
                user = random.choice(self.users) if self.users else None
                viewed_product = random.choice(self.products)
                
                # Create and send product view event
                view_event = self.create_user_event('product_view', user, viewed_product)
                self.send_event(view_event)
                
                # Record viewing in user profile
                if user:
                    user.view_product(viewed_product['id'])
                
                event_count += 1
                
                # Simulate purchase events occasionally (30% chance)
                if user and random.random() < 0.3:
                    # User might purchase the viewed product or a related one
                    purchase_product = viewed_product
                    if random.random() < 0.4:  # 40% chance to buy a different product
                        # Find products in same category
                        same_category_products = [
                            p for p in self.products 
                            if p.get('category') == viewed_product.get('category') 
                            and p['id'] != viewed_product['id']
                        ]
                        if same_category_products:
                            purchase_product = random.choice(same_category_products)
                    
                    purchase_event = self.create_user_event('product_purchase', user, purchase_product)
                    self.send_event(purchase_event)
                    
                    # Record purchase in user profile
                    user.purchase_product(purchase_product['id'])
                    event_count += 1
                
                # Show progress every 10 events
                if event_count % 10 == 0:
                    elapsed = time.time() - start_time
                    remaining = duration_seconds - elapsed
                    print(f"📊 Progress: {event_count} events sent, {remaining:.1f}s remaining")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n⚠️ Simulation interrupted by user")
        
        finally:
            # Ensure all messages are sent before closing
            self.producer.flush()
            self.producer.close()
            
            elapsed_time = time.time() - start_time
            print(f"\n{'=' * 60}")
            print(f"Simulation Completed!")
            print(f"Total Events Sent: {event_count}")
            print(f"Total Time: {elapsed_time:.2f} seconds")
            print(f"Events per Second: {event_count/elapsed_time:.2f}")
            print(f"{'=' * 60}\n")
    
    def send_batch_events(self, num_events: int = 50):
        """
        Send a batch of events quickly for testing
        
        Args:
            num_events: Number of events to send
        """
        print(f"Sending batch of {num_events} events...")
        
        for i in range(num_events):
            user = random.choice(self.users) if self.users else None
            product = random.choice(self.products)
            
            event = self.create_user_event('product_view', user, product)
            self.send_event(event)
            
            if user:
                user.view_product(product['id'])
            
            # Add small delay to avoid overwhelming
            time.sleep(0.1)
        
        self.producer.flush()
        self.producer.close()
        print(f"Batch of {num_events} events sent successfully!")


def main():
    """
    Main function to run the Kafka event producer
    """
    # Setup paths
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    product_catalog_path = os.path.join(data_dir, "dummy_products.json")
    
    # Create dummy product catalog if it doesn't exist
    if not os.path.exists(product_catalog_path):
        create_dummy_product_catalog(product_catalog_path)
    
    # Load product catalog and create users
    products = load_product_catalog(product_catalog_path)
    users = create_simulated_users()
    
    # Display setup info
    print("Kafka Producer Setup:")
    print(f"Products loaded: {len(products)}")
    print(f"Users created: {len(users)}")
    print(f"Kafka broker: {KAFKA_CONFIG['bootstrap_servers'][0]}")
    
    # Create and run producer
    producer = KafkaEventProducer(products, users)
    
    # Choose simulation mode
    print("\nChoose simulation mode:")
    print("1. Continuous simulation (default)")
    print("2. Batch events")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "2":
        num_events = input("Number of events to send (default 50): ").strip()
        num_events = int(num_events) if num_events.isdigit() else 50
        producer.send_batch_events(num_events)
    else:
        duration = input("Simulation duration in seconds (default 120): ").strip()
        duration = int(duration) if duration.isdigit() else 120
        
        interval = input("Interval between events in seconds (default 3.0): ").strip()
        interval = float(interval) if interval.replace('.', '').isdigit() else 3.0
        
        producer.simulate_user_events(duration_seconds=duration, interval_seconds=interval)


if __name__ == "__main__":
    main()