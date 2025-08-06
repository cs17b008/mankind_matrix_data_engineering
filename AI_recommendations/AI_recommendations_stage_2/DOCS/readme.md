# Complete Execution Guide - Kafka AI Recommendation System

## Windows & VS Code Setup Guide

### Prerequisites

#### 1. Install Required Software

```bash
# Install Python packages
pip install kafka-python confluent-kafka

# If you don't have them already:
pip install json time random os typing collections
```

#### 2. Verify Kafka Installation

Make sure our Kafka setup is running:

* **Zookeeper**: Usually on port 2181
* **Kafka Broker**: Usually on port 9092

## Project Structure Setup

### 1. Create Project Directory

```
kafka-ai-recommendation/
├── data/
│   └── dummy_products.json (auto-generated)
├── AiRecommendation_1.py                 (our existing code)
├── kafka_producer.py        (new)
├── kafka_consumer.py        (new)
├── setup_monitor.py         (new)
└── requirements.txt
```

### 2. Create requirements.txt

```
kafka-python==2.0.2
confluent-kafka==2.3.0
```

## Step-by-Step Execution

### Step 1: Prepare our Existing Code

1. Save our existing code as `AiRecommendation_1.py` in the project directory
2. Make sure all imports work correctly

### Step 2: Create New Files

1. Create `kafka_producer.py` (from the artifact above)
2. Create `kafka_consumer.py` (from the artifact above)
3. Create `setup_monitor.py` (from the artifact above)

### Step 3: Update Import Statements

In both `kafka_producer.py` and `kafka_consumer.py`, change:

```python
# From:
from AiRecommendation_1 import UserProfile, create_simulated_users, load_product_catalog

# To:
from AiRecommendation_1 import (
    UserProfile, create_simulated_users, load_product_catalog, 
    create_dummy_product_catalog, AIEngine, MetricsTracker
)
```

### Step 4: Start Kafka Services

#### Option A: If using Kafka installation

```bash
# Terminal 1 - Start Zookeeper
cd /path/to/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 - Start Kafka
cd /path/to/kafka  
bin/kafka-server-start.sh config/server.properties
```

#### Option B: If using Docker

```bash
# Create docker-compose.yml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

# Run:
docker-compose up -d
```

### Step 5: Setup Topics and Test Connection

#### In VS Code Terminal 1:

```bash
python setup_monitor.py
# Choose option 1 to setup topics
# Choose option 4 to test connection
```

Expected output:

```
✅ Connected to Kafka successfully!
✅ Created topic: user-events
✅ Created topic: recommendations  
✅ Created topic: metrics
```

### Step 6: Start AI Consumer (Recommendation Engine)

#### In VS Code Terminal 2:

```bash
python kafka_consumer.py
```

Expected output:

```
AI Consumer initialized with 15 products
Connected to Kafka broker: localhost:9092
🚀 AI Recommendation Consumer Started!
Listening on topic: user-events
Waiting for events...
```

### Step 7: Start Monitor (Optional)

#### In VS Code Terminal 3:

```bash
python setup_monitor.py
# Choose option 2 for recommendations monitor
# OR option 3 for user events monitor
```

### Step 8: Start Producer (Event Simulator)

#### In VS Code Terminal 4:

```bash
python kafka_producer.py
```

You'll see a menu:

```
Choose simulation mode:
1. Continuous simulation (default)
2. Batch events
Enter choice (1 or 2): 1
Simulation duration in seconds (default 120): 60
Interval between events in seconds (default 3.0): 2
```

## Expected Flow

### 1. Producer Output:

```
✅ Event sent: product_view - User 1 - Product: Smart AI Assistant
✅ Event sent: product_purchase - User 1 - Product: Smart AI Assistant
📊 Progress: 10 events sent, 45.2s remaining
```

### 2. Consumer Output:

```
🔍 Processing View Event:
   User: 1
   Product: Smart AI Assistant
   Category: AI Consumer Products
   Price: $299.99
   ✅ Generated 5 recommendations in 2.34ms
      1. Voice Recognition Module ($149.99) - AI Consumer Products
      2. AI Edge Processor ($499.99) - AI Consumer Products
      3. Natural Language Processor ($249.99) - AI Components
      4. Computer Vision System ($349.99) - AI Components
      5. Smart Home Hub ($199.99) - Consumer Electronics
```

### 3. Monitor Output:

```
🎯 [14:23:45] Recommendations for User 1
   Viewed: Smart AI Assistant
   Processing Time: 2.34ms
   Has Profile: Yes
   Recommendations (5):
     1. Voice Recognition Module - $149.99 (AI Consumer Products)
     2. AI Edge Processor - $499.99 (AI Consumer Products)
     ...
```

## Troubleshooting

### Common Issues:

#### 1. Kafka Connection Error

```
❌ Failed to connect to Kafka: NoBrokersAvailable
```

**Solution**: Make sure Kafka is running on localhost:9092

#### 2. Import Error

```
ModuleNotFoundError: No module named 'AiRecommendation_1'
```

**Solution**: Make sure `AiRecommendation_1.py` is in the same directory

#### 3. Topic Already Exists

```
ℹ️ Topic already exists: user-events
```

**Solution**: This is normal, topics are reused

#### 4. Consumer Not Receiving Messages

**Solution**:

* Restart consumer with Ctrl+C and restart
* Check if producer is sending to correct topic
* Verify topics exist: `python setup_monitor.py` → option 4

### Performance Tips:

1. **Adjust intervals**: Use smaller intervals (0.5s) for more activity
2. **Batch mode**: Use batch events for testing (option 2 in producer)
3. **Multiple consumers**: Run multiple consumer instances for load testing

## VS Code Debugging

### 1. Set Breakpoints

* In `kafka_consumer.py` at `process_view_event()`
* In `kafka_producer.py` at `send_event()`

### 2. Debug Configuration

Create `.vscode/launch.json`:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug Consumer",
            "type": "python",
            "request": "launch",
            "program": "kafka_consumer.py",
            "console": "integratedTerminal"
        },
        {
            "name": "Debug Producer", 
            "type": "python",
            "request": "launch",
            "program": "kafka_producer.py",
            "console": "integratedTerminal"
        }
    ]
}
```

## Metrics & Evaluation

The system tracks:

* **Precision**: % of recommendations that were relevant
* **Recall**: % of relevant items that were recommended
* **F1 Score**: Harmonic mean of precision and recall
* **Processing Time**: How fast recommendations are generated
* **Events/Second**: System throughput
