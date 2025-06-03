"""
Mankind Matrix - Real-Time AI Recommendation Prototype with Evaluation Metrics
"""

import json
import time
import random
import os
from typing import Dict, List, Any, Set, Tuple
from collections import defaultdict

class UserProfile:
    """
    Represents a simulated user with preferences for certain product categories and features
    """
    
    def __init__(self, user_id: int, preferred_categories: List[str], 
                 preferred_features: List[str] = None,
                 price_sensitivity: float = 0.5):
        """
        Initialize the user profile
        
        Args:
            user_id: Unique identifier for the user
            preferred_categories: List of product categories the user prefers
            preferred_features: List of product features the user prefers
            price_sensitivity: Float between 0-1 indicating price sensitivity (1 = very sensitive)
        """
        self.user_id = user_id
        self.preferred_categories = preferred_categories
        self.preferred_features = preferred_features or []
        self.price_sensitivity = price_sensitivity
        self.viewed_products: List[int] = []
        self.purchased_products: List[int] = []
        
    def would_like_product(self, product: Dict[str, Any]) -> bool:
        """
        Determine if this user would likely be interested in a product
        
        Args:
            product: Product dictionary
            
        Returns:
            Boolean indicating if the user would like this product
        """
        # Category match is the strongest signal
        if product.get('category') in self.preferred_categories:
            return True
            
        # Check for feature matches if category doesn't match
        description = product.get('description', '').lower()
        for feature in self.preferred_features:
            if feature.lower() in description:
                return True
                
        return False
        
    def view_product(self, product_id: int):
        """Record that the user viewed a product"""
        self.viewed_products.append(product_id)
        
    def purchase_product(self, product_id: int):
        """Record that the user purchased a product"""
        self.purchased_products.append(product_id)
        
    def get_viewing_history(self) -> List[int]:
        """Get list of products the user has viewed"""
        return self.viewed_products
        
    def get_purchase_history(self) -> List[int]:
        """Get list of products the user has purchased"""
        return self.purchased_products


class MetricsTracker:
    """
    Tracks and calculates precision and recall metrics for recommendation quality
    """
    
    def __init__(self):
        """Initialize the metrics tracker"""
        self.total_recommendations = 0
        self.relevant_recommendations = 0
        self.total_relevant_products = 0
        self.relevant_products_recommended = 0
        
        # For tracking detailed metrics by user
        self.user_metrics = defaultdict(lambda: {
            "total_recommendations": 0,
            "relevant_recommendations": 0,
            "total_relevant_products": 0,
            "relevant_products_recommended": 0
        })
        
    def record_recommendations(self, user: UserProfile, recommendations: List[Dict[str, Any]], 
                              all_products: List[Dict[str, Any]]):
        """
        Record metrics for a set of recommendations
        
        Args:
            user: The user profile
            recommendations: List of recommended products
            all_products: Complete list of available products
        """
        # Count recommendations made
        rec_count = len(recommendations)
        self.total_recommendations += rec_count
        self.user_metrics[user.user_id]["total_recommendations"] += rec_count
        
        # Count how many recommendations would be relevant to this user
        relevant_recs = sum(1 for p in recommendations if user.would_like_product(p))
        self.relevant_recommendations += relevant_recs
        self.user_metrics[user.user_id]["relevant_recommendations"] += relevant_recs
        
        # Count all products that would be relevant to this user
        relevant_product_count = sum(1 for p in all_products if user.would_like_product(p))
        
        # Only count this once per user
        if self.user_metrics[user.user_id]["total_relevant_products"] == 0:
            self.total_relevant_products += relevant_product_count
            self.user_metrics[user.user_id]["total_relevant_products"] = relevant_product_count
        
        # Count how many of the relevant products were recommended
        recommended_product_ids = set(r['id'] for r in recommendations)
        relevant_products_recommended = sum(1 for p in all_products 
                                           if user.would_like_product(p) and p['id'] in recommended_product_ids)
        
        self.relevant_products_recommended += relevant_products_recommended
        self.user_metrics[user.user_id]["relevant_products_recommended"] += relevant_products_recommended
        
    def get_precision(self) -> float:
        """
        Calculate precision: What percentage of our recommendations were relevant?
        
        Returns:
            Precision as a float between 0-1
        """
        if self.total_recommendations == 0:
            return 0.0
        return self.relevant_recommendations / self.total_recommendations
    
    def get_recall(self) -> float:
        """
        Calculate recall: What percentage of all relevant items did we recommend?
        
        Returns:
            Recall as a float between 0-1
        """
        if self.total_relevant_products == 0:
            return 0.0
        return self.relevant_products_recommended / self.total_relevant_products
    
    def get_f1_score(self) -> float:
        """
        Calculate F1 score: Harmonic mean of precision and recall
        
        Returns:
            F1 score as a float between 0-1
        """
        precision = self.get_precision()
        recall = self.get_recall()
        
        if precision + recall == 0:
            return 0.0
        return 2 * (precision * recall) / (precision + recall)
    
    def get_user_metrics(self, user_id: int) -> Dict[str, float]:
        """
        Get metrics for a specific user
        
        Args:
            user_id: The user ID to get metrics for
            
        Returns:
            Dictionary containing precision, recall, and F1 for this user
        """
        metrics = self.user_metrics[user_id]
        
        # Calculate precision
        precision = 0.0
        if metrics["total_recommendations"] > 0:
            precision = metrics["relevant_recommendations"] / metrics["total_recommendations"]
            
        # Calculate recall
        recall = 0.0
        if metrics["total_relevant_products"] > 0:
            recall = metrics["relevant_products_recommended"] / metrics["total_relevant_products"]
            
        # Calculate F1
        f1 = 0.0
        if precision + recall > 0:
            f1 = 2 * (precision * recall) / (precision + recall)
            
        return {
            "precision": precision,
            "recall": recall,
            "f1": f1
        }
    
    def get_summary(self) -> Dict[str, float]:
        """
        Get a summary of all metrics
        
        Returns:
            Dictionary containing overall precision, recall, and F1
        """
        return {
            "precision": self.get_precision(),
            "recall": self.get_recall(),
            "f1": self.get_f1_score(),
            "total_recommendations": self.total_recommendations,
            "relevant_recommendations": self.relevant_recommendations,
            "total_relevant_products": self.total_relevant_products
        }


class AIEngine:
    """
    Enhanced AI recommendation engine that provides content-based product recommendations
    """
    
    def __init__(self, product_catalog: List[Dict[str, Any]]):
        """
        Initialize the AI Engine with a product catalog
        
        Args:
            product_catalog: List of product dictionaries
        """
        self.products = product_catalog
        
    def get_recommendations(self, viewed_product: Dict[str, Any], 
                          user_profile: UserProfile = None,
                          count: int = 3) -> List[Dict[str, Any]]:
        """
        Generate product recommendations based on the currently viewed product and user profile
        
        Args:
            viewed_product: The product currently being viewed by the user
            user_profile: Optional user profile for personalized recommendations
            count: Number of recommendations to return
            
        Returns:
            List of recommended product dictionaries
        """
        recommendations = []
        
        # If we have user profile data, use it for personalized recommendations
        if user_profile:
            # Find products that match user's preferred categories
            category_matches = [
                p for p in self.products 
                if p.get('category') in user_profile.preferred_categories
                and p.get('id') != viewed_product.get('id')
            ]
            
            # Find products with descriptions containing preferred features
            feature_matches = []
            for product in self.products:
                if product.get('id') == viewed_product.get('id'):
                    continue
                    
                description = product.get('description', '').lower()
                if any(feature.lower() in description for feature in user_profile.preferred_features):
                    feature_matches.append(product)
            
            # Combine and prioritize recommendations (can't use set() with dicts)
            # Use a dict with product ID as key to eliminate duplicates
            rec_dict = {}
            for product in category_matches + feature_matches:
                rec_dict[product['id']] = product
            recommendations = list(rec_dict.values())
            
            # If we still need more recommendations, add some from the same category as viewed product
            if len(recommendations) < count:
                current_category = viewed_product.get('category', '')
                same_category = [
                    p for p in self.products 
                    if p.get('category') == current_category 
                    and p.get('id') != viewed_product.get('id')
                    and p not in recommendations
                ]
                recommendations.extend(same_category)
        else:
            # Fallback to simple category-based recommendations
            current_category = viewed_product.get('category', '')
            recommendations = [
                p for p in self.products 
                if p.get('category') == current_category and p.get('id') != viewed_product.get('id')
            ]
        
        # If we don't have enough recommendations, add some random products
        if len(recommendations) < count:
            other_products = [
                p for p in self.products 
                if p.get('id') != viewed_product.get('id') and p not in recommendations
            ]
            random.shuffle(other_products)
            recommendations.extend(other_products)
        
        # Return the specified number of recommendations
        return recommendations[:count]


class StreamSimulator:
    """
    Simulates a stream of user events for testing the recommendation engine
    """
    
    def __init__(self, product_catalog: List[Dict[str, Any]], ai_engine: AIEngine, 
                 users: List[UserProfile] = None):
        """
        Initialize the simulator with a product catalog and AI engine
        
        Args:
            product_catalog: List of product dictionaries
            ai_engine: The AI recommendation engine to use
            users: Optional list of user profiles for simulation
        """
        self.products = product_catalog
        self.ai_engine = ai_engine
        self.users = users or []
        self.metrics_tracker = MetricsTracker()
        
    def simulate_user_behavior(self, duration_seconds: int = 60, interval_seconds: float = 5.0,
                              with_metrics: bool = True):
        """
        Simulate user behavior for a specified duration
        
        Args:
            duration_seconds: Total simulation time in seconds
            interval_seconds: Time between events in seconds
            with_metrics: Whether to calculate and display metrics
        """
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        print(f"\n{'=' * 50}")
        print(f"Starting simulation for {duration_seconds} seconds")
        print(f"{'=' * 50}\n")
        
        while time.time() < end_time:
            # Select a random user
            user = random.choice(self.users) if self.users else None
            
            # Randomly select a product to simulate user viewing it
            viewed_product = random.choice(self.products)
            
            # Record that the user viewed this product
            if user:
                user.view_product(viewed_product['id'])
            
            # Display user event
            user_info = f"User {user.user_id}" if user else "Anonymous user"
            print(f"\n[User Event] {user_info} viewed: {viewed_product['name']}")
            
            # Get recommendations based on the viewed product and user profile
            recommendations = self.ai_engine.get_recommendations(
                viewed_product, 
                user_profile=user
            )
            
            # Record metrics if we have a user profile
            if with_metrics and user:
                self.metrics_tracker.record_recommendations(user, recommendations, self.products)
            
            # Display recommendations
            print("[AI Recommendations] You might like:")
            for rec in recommendations:
                print(f"- {rec['name']}")
                
            # Simulate user sometimes purchasing a recommended product
            if user and random.random() < 0.3:  # 30% chance to purchase
                purchased = random.choice(recommendations)
                user.purchase_product(purchased['id'])
                print(f"[User Event] {user_info} purchased: {purchased['name']}")
                
            print("-" * 40)
            
            # Wait for the specified interval
            time.sleep(interval_seconds)
            
        print(f"\n{'=' * 50}")
        print(f"Simulation ended after {duration_seconds} seconds")
        
        # Display metrics if enabled
        if with_metrics:
            self._display_metrics()
            
        print(f"{'=' * 50}\n")
    
    def _display_metrics(self):
        """Display the calculated metrics"""
        summary = self.metrics_tracker.get_summary()
        
        print("\nRecommendation System Metrics:")
        print(f"Precision: {summary['precision']:.4f} - What percentage of our recommendations were relevant")
        print(f"Recall: {summary['recall']:.4f} - What percentage of all relevant items did we recommend")
        print(f"F1 Score: {self.metrics_tracker.get_f1_score():.4f} - Harmonic mean of precision and recall")
        print(f"Total Recommendations: {summary['total_recommendations']}")
        print(f"Relevant Recommendations: {summary['relevant_recommendations']}")
        
        # Print per-user metrics
        if self.users:
            print("\nPer-User Metrics:")
            for user in self.users:
                user_metrics = self.metrics_tracker.get_user_metrics(user.user_id)
                print(f"User {user.user_id} - "
                      f"Precision: {user_metrics['precision']:.4f}, "
                      f"Recall: {user_metrics['recall']:.4f}, "
                      f"F1: {user_metrics['f1']:.4f}")


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
            {"id": 1, "name": "Smart AI Assistant", "category": "AI Consumer Products", "price": 299.99,
             "description": "An intelligent virtual assistant for everyday tasks"},
            {"id": 2, "name": "Voice Recognition Module", "category": "AI Consumer Products", "price": 149.99,
             "description": "Advanced voice recognition technology for smart devices"},
            {"id": 3, "name": "AI Edge Processor", "category": "AI Consumer Products", "price": 499.99,
             "description": "Process AI workloads locally on your device"},
            {"id": 4, "name": "TPU Accelerator Card", "category": "Hardware", "price": 799.99,
             "description": "Tensor Processing Unit for accelerated machine learning"},
            {"id": 5, "name": "Neural Network Server", "category": "Hardware", "price": 1299.99,
             "description": "Dedicated server for running neural networks"}
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
         "description": "Add visual recognition capabilities to your projects"},
        {"id": 11, "name": "Augmented Reality Headset", "category": "Consumer Electronics", "price": 699.99,
         "description": "Immersive AR experience with advanced tracking"},
        {"id": 12, "name": "Smart Home Hub", "category": "Consumer Electronics", "price": 199.99,
         "description": "Central control for all your smart home devices"},
        {"id": 13, "name": "Neural Interface", "category": "Advanced Computing", "price": 3499.99,
         "description": "Direct brain-computer interface for advanced applications"},
        {"id": 14, "name": "Quantum Encryption Module", "category": "Security Products", "price": 899.99,
         "description": "Next-generation encryption using quantum principles"},
        {"id": 15, "name": "Drone AI Controller", "category": "AI Components", "price": 399.99,
         "description": "Advanced autonomous control system for drones"}
    ]
    
    # Write to file
    with open(file_path, 'w') as file:
        json.dump(dummy_products, file, indent=2)
    
    print(f"Created dummy product catalog at {file_path}")


def create_simulated_users() -> List[UserProfile]:
    """
    Create a list of simulated users with different preferences
    
    Returns:
        List of UserProfile objects
    """
    users = [
        UserProfile(
            user_id=1,
            preferred_categories=["AI Consumer Products", "Consumer Electronics"],
            preferred_features=["assistant", "smart", "recognition"],
            price_sensitivity=0.7
        ),
        UserProfile(
            user_id=2,
            preferred_categories=["Hardware", "Advanced Computing"],
            preferred_features=["processor", "server", "computing"],
            price_sensitivity=0.3
        ),
        UserProfile(
            user_id=3,
            preferred_categories=["Developer Tools", "AI Components"],
            preferred_features=["development", "toolkit", "design"],
            price_sensitivity=0.5
        ),
        UserProfile(
            user_id=4,
            preferred_categories=["Security Products", "Hardware"],
            preferred_features=["encryption", "secure", "protection"],
            price_sensitivity=0.4
        ),
        UserProfile(
            user_id=5,
            preferred_categories=["Consumer Electronics", "AI Components"],
            preferred_features=["voice", "vision", "recognition"],
            price_sensitivity=0.6
        )
    ]
    
    return users


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
    
    # Create simulated users
    users = create_simulated_users()
    
    # Initialize AI recommendation engine
    ai_engine = AIEngine(products)
    
    # Initialize stream simulator
    simulator = StreamSimulator(products, ai_engine, users)
    
    # Run simulation with metrics
    simulator.simulate_user_behavior(duration_seconds=30, interval_seconds=3.0, with_metrics=True)


if __name__ == "__main__":
    main()