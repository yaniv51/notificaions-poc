from kafka import KafkaConsumer
import json
import time
import logging
from datetime import datetime
import threading
from kafka.structs import TopicPartition

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class LeakyBucketRateLimiter:
    def __init__(self, rate_per_second):
        self.rate_per_second = rate_per_second
        self.last_update = time.time()
        self.tokens = 0
        self.lock = threading.Lock()
        
    def acquire(self):
        with self.lock:
            current_time = time.time()
            time_passed = current_time - self.last_update
            self.tokens = min(self.rate_per_second, self.tokens + time_passed * self.rate_per_second)
            self.last_update = current_time
            
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

def process_notification(notification, priority):
    logging.info(f"Processing {priority} priority notification: {notification.get('type')}")

def create_consumer(topic, partitions, group_id_suffix, rate_limit):
    logging.info(f"Creating consumer for topic {topic}, partitions {partitions}, rate limit {rate_limit}")
    
    # Create consumer without subscribing to any topic
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        group_id=f'notification_worker_{group_id_suffix}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,  # Enable auto commit
    )
    
    # Assign specific partitions to the consumer
    topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
    consumer.assign(topic_partitions)
    
    # Log assigned partitions
    logging.info(f"Consumer assigned to partitions: {[tp.partition for tp in consumer.assignment()]}")
    
    return consumer

def consume_messages(consumer, topic, priority, rate_limiter):
    logging.info(f"Starting {priority} priority consumer for {topic}")
    message_count = 0
    start_time = time.time()
    
    try:
        while True:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=1000)
            
            if not messages:
                logging.debug(f"No messages received for {priority} priority {topic}")
                continue
                
            # Process messages from each partition
            for topic_partition, partition_messages in messages.items():
                for message in partition_messages:
                    # Wait for rate limiter to allow processing
                    while not rate_limiter.acquire():
                        time.sleep(0.1)  # Small sleep to prevent busy waiting
                    
                    message_count += 1
                    notification = message.value
                    process_notification(notification, priority)
                    
                    # Log progress every 10 messages
                    if message_count % 10 == 0:
                        elapsed_time = time.time() - start_time
                        rate = message_count / elapsed_time
                        logging.info(f"Processed {message_count} messages for {priority} priority {topic}. Current rate: {rate:.2f} msgs/sec")
            
    except KeyboardInterrupt:
        logging.info(f"Shutting down {priority} priority consumer for {topic}...")
    except Exception as e:
        logging.error(f"Error in consumer for {priority} priority {topic}: {str(e)}")
    finally:
        consumer.close()
        total_time = time.time() - start_time
        logging.info(f"Total messages processed for {priority} priority {topic}: {message_count} in {total_time:.2f} seconds. Average rate: {message_count/total_time:.2f} msgs/sec")

def main():
    # Rate limits (messages per second)
    # Push topic: 10 messages/sec total (500 messages in 50 seconds)
    PUSH_RATE_LIMIT_HIGH = 10 
    PUSH_RATE_LIMIT = 6      

    # Email topic: 2 messages/sec total (assuming 1/5 of push rate)
    EMAIL_RATE_LIMIT_HIGH = 1  # 40% for high priority
    EMAIL_RATE_LIMIT = 1      # 60% for low priority

    logging.info("Initializing consumers...")

    # Create rate limiters
    high_priority_email_limiter = LeakyBucketRateLimiter(EMAIL_RATE_LIMIT_HIGH)
    low_priority_email_limiter = LeakyBucketRateLimiter(EMAIL_RATE_LIMIT)
    high_priority_push_limiter = LeakyBucketRateLimiter(PUSH_RATE_LIMIT_HIGH)
    low_priority_push_limiter = LeakyBucketRateLimiter(PUSH_RATE_LIMIT)

    # Create consumers for high and low priority messages
    high_priority_email_consumer = create_consumer('email', [4, 5], 'high_priority_email', EMAIL_RATE_LIMIT_HIGH)
    low_priority_email_consumer = create_consumer('email', [0, 1, 2, 3], 'low_priority_email', EMAIL_RATE_LIMIT)
    high_priority_push_consumer = create_consumer('push', [4, 5], 'high_priority_push', PUSH_RATE_LIMIT_HIGH)
    low_priority_push_consumer = create_consumer('push', [0, 1, 2, 3], 'low_priority_push', PUSH_RATE_LIMIT)

    logging.info("Starting notification workers...")

    # Create threads for each consumer
    threads = [
        threading.Thread(target=consume_messages, args=(high_priority_email_consumer, 'email', 'high', high_priority_email_limiter)),
        threading.Thread(target=consume_messages, args=(low_priority_email_consumer, 'email', 'low', low_priority_email_limiter)),
        threading.Thread(target=consume_messages, args=(high_priority_push_consumer, 'push', 'high', high_priority_push_limiter)),
        threading.Thread(target=consume_messages, args=(low_priority_push_consumer, 'push', 'low', low_priority_push_limiter))
    ]

    try:
        # Start all consumer threads
        for thread in threads:
            thread.start()
            logging.info(f"Started thread: {thread.name}")
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
            
    except KeyboardInterrupt:
        logging.info("Shutting down all consumers...")
    except Exception as e:
        logging.error(f"Error in main thread: {str(e)}")

if __name__ == '__main__':
    main()