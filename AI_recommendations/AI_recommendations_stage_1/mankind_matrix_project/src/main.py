"""
Mankind Matrix - Real-Time AI Recommendation Prototype
"""

import json
import time
import random
import os
from typing import Dict, List, Any

class AIEngine:
    """
    Simple AI recommendation engine that provides content-based product recommendations
    """
    
    def __init__(self, product_catalog: List[Dict[str, Any]]):
        """
        Initialize the AI Engine with a product catalog
        
        Args:
            product_catalog: List of product dictionaries
        """
        self.products = product_catalog
        
    def get_recommendations(self, viewed_product: Dict[str, Any], count: int = 3) -> List[Dict[str, Any]]:
        """
        Generate product recommendations based on the currently viewed product
        
        Args:
            viewed_product: The product currently being viewed by the user
            count: Number of recommendations to return
            
        Returns:
            List of recommended product dictionaries
        """
        # Get the category of the viewed product
        current_category = viewed_product.get('category', '')
        
        # Find other products in the same category
        same_category_products = [
            p for p in self.products 
            if p.get('category') == current_category and p.get('id') != viewed_product.get('id')
        ]
        
        # If we don't have enough products in the same category, add some random products
        if len(same_category_products) < count:
            other_products = [
                p for p in self.products 
                if p.get('id') != viewed_product.get('id') and p not in same_category_products
            ]
            random.shuffle(other_products)
            same_category_products.extend(other_products)
        
        # Return the specified number of recommendations
        return same_category_products[:count]


class StreamSimulator:
    """
    Simulates a stream of user events for testing the recommendation engine
    """
    
    def __init__(self, product_catalog: List[Dict[str, Any]], ai_engine: AIEngine):
        """
        Initialize the simulator with a product catalog and AI engine
        
        Args:
            product_catalog: List of product dictionaries
            ai_engine: The AI recommendation engine to use
        """
        self.products = product_catalog
        self.ai_engine = ai_engine
        
    def simulate_user_behavior(self, duration_seconds: int = 60, interval_seconds: float = 5.0):
        """
        Simulate user behavior for a specified duration
        
        Args:
            duration_seconds: Total simulation time in seconds
            interval_seconds: Time between events in seconds
        """
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        print(f"\n{'=' * 50}")
        print(f"Starting simulation for {duration_seconds} seconds")
        print(f"{'=' * 50}\n")
        
        while time.time() < end_time:
            # Randomly select a product to simulate user viewing it
            viewed_product = random.choice(self.products)
            
            # Display user event
            print(f"\n[User Event] Viewed: {viewed_product['name']}")
            
            # Get recommendations based on the viewed product
            recommendations = self.ai_engine.get_recommendations(viewed_product)
            
            # Display recommendations
            print("[AI Recommendations] You might like:")
            for rec in recommendations:
                print(f"- {rec['name']}")
                
            print("-" * 40)
            
            # Wait for the specified interval
            time.sleep(interval_seconds)
            
        print(f"\n{'=' * 50}")
        print(f"Simulation ended after {duration_seconds} seconds")
        print(f"{'=' * 50}\n")


def load_product_catalog(file_path: str) -> List[Dict[str, Any]]:
    """
    Load the product catalog from a JSON file
    
    Args:
        file_path: Path to the JSON file containing product data
        
    Returns:
        List of product dictionaries
    """
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading product catalog: {e}")
        # Return a dummy catalog if the file can't be loaded
        return [
            {"id": 1, "name": "Smart AI Assistant", "category": "AI Consumer Products", "price": 299.99},
            {"id": 2, "name": "Voice Recognition Module", "category": "AI Consumer Products", "price": 149.99},
            {"id": 3, "name": "AI Edge Processor", "category": "AI Consumer Products", "price": 499.99},
            {"id": 4, "name": "TPU Accelerator Card", "category": "Hardware", "price": 799.99},
            {"id": 5, "name": "Neural Network Server", "category": "Hardware", "price": 1299.99}
        ]


def create_dummy_product_catalog(file_path: str):
    """
    Create a dummy product catalog JSON file if it doesn't exist
    
    Args:
        file_path: Path to save the JSON file
    """
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Define dummy products
    dummy_products = [
        {"id": 1, "name": "Smart AI Assistant", "category": "AI Consumer Products", "price": 299.99, 
         "description": "An intelligent virtual assistant for everyday tasks"},
        {"id": 2, "name": "Voice Recognition Module", "category": "AI Consumer Products", "price": 149.99,
         "description": "Advanced voice recognition technology for smart devices"},
        {"id": 3, "name": "AI Edge Processor", "category": "AI Consumer Products", "price": 499.99,
         "description": "Process AI workloads locally on your device"},
        {"id": 4, "name": "TPU Accelerator Card", "category": "Hardware", "price": 799.99,
         "description": "Tensor Processing Unit for accelerated machine learning"},
        {"id": 5, "name": "Neural Network Server", "category": "Hardware", "price": 1299.99,
         "description": "Dedicated server for running neural networks"},
        {"id": 6, "name": "Quantum Computing Module", "category": "Advanced Computing", "price": 4999.99,
         "description": "Experimental quantum computing capabilities"},
        {"id": 7, "name": "AI Development Kit", "category": "Developer Tools", "price": 399.99,
         "description": "Complete toolkit for AI application development"},
        {"id": 8, "name": "Semiconductor Design Suite", "category": "Developer Tools", "price": 899.99,
         "description": "Professional tools for semiconductor design"},
        {"id": 9, "name": "Natural Language Processor", "category": "AI Components", "price": 249.99,
         "description": "Process and understand human language in your applications"},
        {"id": 10, "name": "Computer Vision System", "category": "AI Components", "price": 349.99,
         "description": "Add visual recognition capabilities to your projects"}
    ]
    
    # Write to file
    with open(file_path, 'w') as file:
        json.dump(dummy_products, file, indent=2)
    
    print(f"Created dummy product catalog at {file_path}")


def main():
    """
    Main function to run the recommendation prototype
    """
    # Define paths
    data_dir = "data"
    product_catalog_path = os.path.join(data_dir, "dummy_products.json")
    
    # Create dummy product catalog if it doesn't exist
    if not os.path.exists(product_catalog_path):
        create_dummy_product_catalog(product_catalog_path)
    
    # Load product catalog
    products = load_product_catalog(product_catalog_path)
    
    # Initialize AI recommendation engine
    ai_engine = AIEngine(products)
    
    # Initialize stream simulator
    simulator = StreamSimulator(products, ai_engine)
    
    # Run simulation
    simulator.simulate_user_behavior(duration_seconds=30, interval_seconds=3.0)


if __name__ == "__main__":
    main()