from kafka import KafkaProducer
import json
import logging

logger = logging.getLogger(__name__)

def get_producer():
    return KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        retries=3,
        linger_ms=50,
        request_timeout_ms=20000,
        max_block_ms=20000
    )

def send_posts(posts):
    producer = get_producer()
    for post in posts:
        producer.send(
            "reddit_posts",
            key=post["post_id"].encode("utf-8"),
            value=post
        )
    producer.flush(timeout=20)
    producer.close()
    logger.info("✅ Posts sent to Kafka")

def send_comments(comments):
    producer = get_producer()
    for comment in comments:
        producer.send(
            "reddit_comments",
            key=comment["comment_id"].encode("utf-8"),
            value=comment
        )
    producer.flush(timeout=20)
    producer.close()
    logger.info("✅ Comments sent to Kafka")
