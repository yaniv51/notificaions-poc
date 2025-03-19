# Notification System POC

This is a proof of concept for a notification system that supports email and web push notifications with priority handling and rate limiting.

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- pip (Python package manager)

## Project Structure

```
.
├── docker-compose.yml    # Kafka and Zookeeper setup
├── requirements.txt     # Python dependencies
├── producer/           # Notification generator service
│   └── app.py
└── consumer/          # Notification worker service
    └── app.py
```

## Setup

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Start Kafka and Zookeeper:
```bash
docker-compose up -d
```

## Running the Services

1. Start the notification worker (consumer):
```bash
python consumer/app.py
```

2. Start the notification generator (producer):
```bash
python producer/app.py
```

## Usage

The notification generator exposes an HTTP endpoint to generate notifications:

```
GET http://localhost:5000/generate-notifications?type={EMAIL/PUSH}&amount={VALUE}
```

Example:
```bash
curl "http://localhost:5000/generate-notifications?type=EMAIL&amount=10"
```

## Features

- Supports two types of notifications: Email and Web Push
- Priority handling (High/Low) using Kafka partitions
- Rate limiting:
  - Email: 5 messages per second
  - Push: 20 messages per second
- Random notification generation with 80% low priority and 20% high priority
- High priority messages are sent to partitions 4+5, low priority to 0-3

## Kafka Topics

- `email`: 6 partitions
- `push`: 6 partitions

## Monitoring

The worker service logs all processed notifications to the console. 