# Mankind Matrix - AI Recommendation System

A real-time recommendation engine with evaluation metrics for product recommendations.


## Overview

Mankind Matrix is an AI recommendation prototype that simulates real-time product recommendations and evaluates their quality using precision and recall metrics. The system models user preferences and behaviors to generate personalized product suggestions.

## Features

- **Content-based filtering** with category and feature matching
- **User preference modeling** to personalize recommendations
- **Real-time simulation** of user viewing and purchasing behaviors
- **Evaluation metrics** (precision, recall, F1) to measure recommendation quality
- **Per-user performance analysis** to identify user segment effectiveness

## System Components

### UserProfile

Models user preferences and behavior:
- Preferred product categories
- Preferred product features
- Price sensitivity
- Viewing and purchase history

```python
user = UserProfile(
    user_id=1,
    preferred_categories=["AI Consumer Products", "Consumer Electronics"],
    preferred_features=["assistant", "smart", "recognition"],
    price_sensitivity=0.7
)
```

### AIEngine

Generates personalized recommendations based on:
- Currently viewed product
- User's preferred categories
- User's preferred features

```python
recommendations = ai_engine.get_recommendations(
    viewed_product, 
    user_profile=user,
    count=3
)
```

### MetricsTracker

Evaluates recommendation quality using:
- Precision: What percentage of recommendations were relevant
- Recall: What percentage of all relevant items were recommended
- F1 Score: Harmonic mean balancing precision and recall

```python
metrics = metrics_tracker.get_summary()
print(f"Precision: {metrics['precision']:.4f}")
print(f"Recall: {metrics['recall']:.4f}")
print(f"F1 Score: {metrics['f1']:.4f}")
```

### StreamSimulator

Orchestrates the simulation:
- Simulates users viewing products
- Generates recommendations in real-time
- Records user behavior and purchases
- Calculates and displays evaluation metrics

```python
simulator = StreamSimulator(products, ai_engine, users)
simulator.simulate_user_behavior(duration_seconds=30, interval_seconds=3.0)
```

## AI Classification

This represents a **narrow AI system** (weak AI) with:
- Domain-specific intelligence for product recommendations
- Rule-based logic with simple personalization
- Deterministic behavior without learning capabilities
- Real-time simulation of user interactions

## Future Development

To evolve this system into more advanced AI, consider these enhancements:

### 1. Machine Learning Integration

```python
# Example implementation of collaborative filtering
class MLBasedRecommender:
    def __init__(self, user_interaction_data):
        self.model = self._train_model(user_interaction_data)
        
    def get_recommendations(self, user_id, product_id, count=5):
        # Use the model to predict items the user might like
        return predicted_items
```

### 2. Natural Language Processing

```python
# Example implementation of semantic similarity
class SemanticRecommender:
    def __init__(self, product_catalog):
        self.product_embeddings = self._generate_embeddings(product_catalog)
        
    def get_similar_products(self, product_id, count=5):
        # Find products with similar semantic meaning
        return similar_products
```

### 3. Reinforcement Learning

```python
# Example implementation of reinforcement learning
class RLRecommender:
    def __init__(self):
        self.model = self._initialize_model()
        
    def recommend(self, user_state):
        # Select action (recommendation) based on policy
        return recommendations
        
    def update_model(self, user_state, action, reward, new_state):
        # Update policy based on feedback (clicks, purchases)
        self.model.update(user_state, action, reward, new_state)
```

## Technical Prerequisites for Advanced Implementation

### 1. Database Requirements

| Table | Purpose | Key Fields |
|-------|---------|------------|
| users | Store user information | user_id, preferences, demographics |
| products | Store product catalog | product_id, name, category, description, price |
| user_events | Track user interactions | user_id, product_id, event_type, timestamp |
| recommendations | Cache generated recommendations | user_id, product_id, score, timestamp |

### 2. Data Collection Pipeline

Required events to track:
- Product views
- Recommendation clicks
- Purchases
- Search queries
- Time spent viewing products

### 3. ML-Ready Dataset Format

For Collaborative Filtering:
```
user_id,product_id,interaction_type,value
1,101,view,1
1,102,purchase,5
2,101,view,1
```

For Content-Based Filtering:
```
product_id,name,category,tags,description,price
101,Smart AI Assistant,AI Consumer Products,"assistant,smart,home",An intelligent...,299.99
102,Voice Recognition Module,AI Components,"voice,recognition",Advanced voice...,149.99
```

### 4. ML Model Prerequisites

| Model Type | Libraries | Input Data | Output |
|------------|-----------|------------|--------|
| Collaborative Filtering | scikit-learn, surprise, lightfm | User-item interaction matrix | User-item affinity scores |
| Content-Based | scikit-learn, transformers, spaCy | Product features, descriptions | Item similarity matrix |
| Hybrid Models | TensorFlow, PyTorch | Combined interaction and content data | Personalized recommendations |

## Implementation Roadmap

1. **Phase 1**: Current implementation with basic metrics (precision/recall) ✅
2. **Phase 2**: Add data collection and storage infrastructure
3. **Phase 3**: Implement offline model training pipeline
4. **Phase 4**: Deploy real-time recommendation API
5. **Phase 5**: Implement A/B testing framework to compare algorithms

## Getting Started

```bash
# Clone the repository
git clone https://github.com/yourusername/mankind-matrix.git
cd mankind-matrix

# Install dependencies
pip install -r requirements.txt

# Run the simulation
python main.py
```

