"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: kafka_starter.py
Pillar: Big Data Processing — Task 3.2
=============================================================
"""

import json
import os
import time
import argparse
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS_FILE       = os.path.join(BASE_DIR, "datasets", "events_stream", "events_2025_01.jsonl")
OUTPUT_DIR        = os.path.join(BASE_DIR, "outputs", "kafka")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Kafka config ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_EVENTS      = "presight.project.events"
TOPIC_ESCALATIONS = "presight.escalations.critical"


# ==============================================================================
# SETUP — Create Kafka topics
# ==============================================================================

def create_topics():
    """
    Create the required Kafka topics if they don't already exist.
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="presight_admin"
        )
        # PRODUCTION NOTE: replication_factor is hardcoded to 1 for local single-node development.
        # For UAE cloud production clusters, scale replication_factor to 3 to ensure High Availability (HA).
        topics = [
            NewTopic(name=TOPIC_EVENTS, num_partitions=3, replication_factor=1),
            NewTopic(name=TOPIC_ESCALATIONS, num_partitions=1, replication_factor=1)
        ]
        admin_client.create_topics(new_topics=topics, validate_only=False)
        logger.info("Created Kafka topics: %s and %s", TOPIC_EVENTS, TOPIC_ESCALATIONS)
    except TopicAlreadyExistsError:
        logger.info("Kafka topics already exist. Skipping creation.")
    except NoBrokersAvailable:
        logger.error("No Kafka brokers available at %s. Ensure Kafka is running.", KAFKA_BOOTSTRAP)
    except Exception as e:
        # CRITICAL ALERT: Log the exact structural traceback if topic generation fails 
        # due to authorization (ACL) or network authentication barriers.
        logger.warning("Topic setup notice: %s", e)


# ==============================================================================
# PRODUCER
# ==============================================================================

def build_producer():
    """
    Build and return a KafkaProducer.
    """
    # ARCHITECTURAL DESIGN NOTE: 
    # 'acks=1' guarantees high throughput by confirming writes as soon as the leader logs it.
    # If absolute financial audit consistency is required later, change 'acks=1' to 'acks=all' 
    # to guarantee no message loss across multiple broker nodes.
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if k else b"",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=10000,
        acks=1
    )


def run_producer(producer, events_file: str, delay_seconds: float = 0.05):
    """
    Read events from JSONL and produce them to Kafka, simulating a real-time stream.
    """
    logger.info("Starting producer — streaming events from: %s", events_file)
    sent_count = 0

    if not os.path.exists(events_file):
        logger.error("Events file not found: %s", events_file)
        return 0

    with open(events_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            try:
                event = json.loads(line)
                
                # METRIC ENRICHMENT: Inject an ISO-8601 UTC timestamp at the point of ingestion 
                # to calculate downstream consumer processing latency later on.
                event["produced_at"] = datetime.utcnow().isoformat()
                event_type = event.get("event_type", "unknown")

                producer.send(TOPIC_EVENTS, key=event_type, value=event)
                sent_count += 1
            except json.JSONDecodeError:
                # DATA GOVERNANCE RESILIENCY: Skip corrupted log records instead of crashing the pipeline.
                logger.warning("Data Quality Skip: Malformed JSON packet encountered on line %d. Skipping row.", line_num)
                continue

            if sent_count % 100 == 0:
                logger.info("Producer: Sent %d messages (event_id=%s, type=%s)", 
                            sent_count, event.get("event_id"), event_type)

            time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    logger.info("Finished producing: %d total messages sent.", sent_count)
    return sent_count


# ==============================================================================
# CONSUMER
# ==============================================================================

def build_consumer(topic: str):
    """
    Build and return a KafkaConsumer subscribed to a given topic.
    """
    # COORDINATION NOTE: 
    # 'consumer_timeout_ms=10000' converts this script into a bounded batch execution task.
    # The consumer loop will automatically terminate and dump its final metric reports 
    # if the streaming channel becomes quiet for more than 10 seconds.
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="presight-assessment-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )


def run_consumer(consumer):
    """
    Consume events from TOPIC_EVENTS, forward critical escalations, and aggregate metrics.
    """
    logger.info("Starting consumer — listening on: %s", TOPIC_EVENTS)

    # Forwarding producer for critical escalations
    forwarder = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    summary = {}
    consumed_count = 0
    forwarded_critical = 0
    start_time = time.time()

    for message in consumer:
        event = message.value
        consumed_count += 1

        event_id = event.get("event_id", "N/A")
        event_type = event.get("event_type", "unknown")
        project_id = event.get("project_id", "N/A")
        payload = event.get("payload", {}) or {}

        summary[event_type] = summary.get(event_type, 0) + 1

        if consumed_count % 100 == 0:
            logger.info("Consumer: Processed %d messages (event_id=%s, type=%s, project_id=%s)", 
                        consumed_count, event_id, event_type, project_id)

        # Forward critical escalations
        if event_type == "escalation_raised" and payload.get("severity") == "Critical":
            forwarder.send(TOPIC_ESCALATIONS, value=event)
            forwarded_critical += 1
            logger.info("FORWARDED critical escalation: event_id=%s project_id=%s", event_id, project_id)

    forwarder.flush()
    forwarder.close()
    consumer.close()

    elapsed = time.time() - start_time
    throughput = consumed_count / elapsed if elapsed > 0 else 0.0

    # Write summary
    summary_path = os.path.join(OUTPUT_DIR, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({
            "run_at": datetime.utcnow().isoformat(),
            "topic": TOPIC_EVENTS,
            "total_messages_consumed": consumed_count,
            "event_counts": summary,
            "critical_escalations_forwarded": forwarded_critical,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_messages_per_second": round(throughput, 2)
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
        run_producer(producer, EVENTS_FILE, delay_seconds=0.001)

    elif args.mode == "consumer":
        consumer = build_consumer(TOPIC_EVENTS)
        run_consumer(consumer)

    elif args.mode == "both":
        producer = build_producer()
        run_producer(producer, EVENTS_FILE, delay_seconds=0.0005)

        consumer = build_consumer(TOPIC_EVENTS)
        summary = run_consumer(consumer)
        print("\nEvent summary:", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()