"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: kafka_starter.py
Pillar: Big Data Processing — Task 3.2
=============================================================

SCENARIO
--------
The project management platform emits real-time events whenever key actions
occur (escalations, status changes, budget updates). Instead of batch files,
these now arrive as a live Kafka stream.

You will build a producer that simulates the event stream and a consumer
that reads, processes, and writes events to a local sink.

ARCHITECTURE
------------
  Simulator → [Kafka Topic: presight.project.events] → Consumer → outputs/kafka/

TASKS
-----
  Task 3.2a → Configure Kafka producer and consumer (see config below)
  Task 3.2b → Implement the producer to stream events from the JSONL file
  Task 3.2c → Implement the consumer to read and process events
  Task 3.2d → Consumer must: filter high-severity escalations and write them
               to a separate topic: presight.escalations.critical
  Task 3.2e → Write a summary of consumed events to outputs/kafka/summary.json

HOW TO RUN
----------
  Ensure Kafka is running (docker-compose up -d)

  Terminal 1 — Producer:
    python starter_files/kafka_starter.py --mode producer

  Terminal 2 — Consumer:
    python starter_files/kafka_starter.py --mode consumer

  Or run both in the same process (for testing):
    python starter_files/kafka_starter.py --mode both
"""

import json
import os
import time
import argparse
import logging
from datetime import datetime

# TODO: Install kafka-python → pip install kafka-python
# from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
# from kafka.admin import NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS_FILE     = os.path.join(BASE_DIR, "datasets", "events_stream", "events_2025_01.jsonl")
OUTPUT_DIR      = os.path.join(BASE_DIR, "outputs", "kafka")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Kafka config ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = "localhost:9092"
TOPIC_EVENTS     = "presight.project.events"
TOPIC_ESCALATIONS = "presight.escalations.critical"


# ==============================================================================
# SETUP — Create Kafka topics
# ==============================================================================

def create_topics():
    """
    Create the required Kafka topics if they don't already exist.

    Topics to create:
      - presight.project.events        (partitions=3, replication_factor=1)
      - presight.escalations.critical  (partitions=1, replication_factor=1)

    TODO:
      - Use KafkaAdminClient
      - Handle the case where topics already exist gracefully
    """
    # YOUR CODE HERE
    pass


# ==============================================================================
# PRODUCER
# ==============================================================================

def build_producer():
    """
    Build and return a KafkaProducer.

    TODO:
      - Bootstrap to KAFKA_BOOTSTRAP
      - Serialise message values as JSON (utf-8 encoded)
      - Set a sensible request_timeout_ms
    """
    # YOUR CODE HERE
    return None


def run_producer(producer, events_file: str, delay_seconds: float = 0.5):
    """
    Read events from the JSONL file and produce them to Kafka one by one,
    simulating a real-time stream.

    TODO:
      - Read each line from events_file
      - Parse it as JSON
      - Add a 'produced_at' timestamp to the message
      - Send to TOPIC_EVENTS using event_type as the message key
      - Log each message sent (event_id and event_type)
      - Sleep delay_seconds between messages to simulate stream rate
      - Flush and close the producer when done
    """
    logger.info("Starting producer — streaming events from: %s", events_file)

    # YOUR CODE HERE
    pass


# ==============================================================================
# CONSUMER
# ==============================================================================

def build_consumer(topic: str):
    """
    Build and return a KafkaConsumer subscribed to a given topic.

    TODO:
      - Bootstrap to KAFKA_BOOTSTRAP
      - Set group_id to 'presight-assessment-consumer'
      - Deserialise message values from JSON
      - Set auto_offset_reset to 'earliest' (so we read from the beginning)
      - Set consumer_timeout_ms to 10000 (stop after 10s of no messages)
    """
    # YOUR CODE HERE
    return None


def run_consumer(consumer):
    """
    Consume events from TOPIC_EVENTS and process them.

    TODO:
      - Consume all messages from the topic
      - For each message:
          a) Log the event_id, event_type, and project_id
          b) If event_type == 'escalation_raised' AND severity == 'Critical':
               → Forward to TOPIC_ESCALATIONS (produce a new message)
          c) Accumulate a summary dict:
               { event_type: count } for all consumed messages
      - After consuming, write the summary to outputs/kafka/summary.json
      - Return the summary dict
    """
    logger.info("Starting consumer — listening on: %s", TOPIC_EVENTS)

    summary = {}

    # YOUR CODE HERE

    # Write summary
    summary_path = os.path.join(OUTPUT_DIR, "summary.json")
    with open(summary_path, "w") as f:
        json.dump({
            "run_at": datetime.utcnow().isoformat(),
            "topic": TOPIC_EVENTS,
            "event_counts": summary
        }, f, indent=2)
    logger.info("Summary written to: %s", summary_path)

    return summary


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Kafka assessment — producer/consumer")
    parser.add_argument(
        "--mode",
        choices=["producer", "consumer", "both"],
        default="both",
        help="Run as producer, consumer, or both"
    )
    args = parser.parse_args()

    create_topics()

    if args.mode == "producer":
        producer = build_producer()
        run_producer(producer, EVENTS_FILE)

    elif args.mode == "consumer":
        consumer = build_consumer(TOPIC_EVENTS)
        run_consumer(consumer)

    elif args.mode == "both":
        # For testing: produce first, then consume
        producer = build_producer()
        run_producer(producer, EVENTS_FILE)

        consumer = build_consumer(TOPIC_EVENTS)
        summary  = run_consumer(consumer)
        print("\nEvent summary:", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
