import json
import time
from typing import Callable, Dict
from kafka import KafkaConsumer, KafkaProducer
from .models import JobMessage, ResultMessage

class Worker:
    def __init__(self):
        self.handlers: Dict[str, Callable[[str], str]] = {}
        self.consumer = None
        self.producer = None

    def register(self, action: str, handler: Callable[[str], str]):
        self.handlers[action] = handler

    def start(self, bootstrap_servers: str = 'kafka:9092', group_id: str = 'workflow-workers-python'):
        # Retry connection to Kafka
        for attempt in range(30):
            try:
                # We do NOT use value_deserializer here, to prevent the consumer loop from crashing on bad data
                self.consumer = KafkaConsumer(
                    'workflow-jobs',
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    auto_offset_reset='earliest'
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                # Test connection by requesting partitions
                self.consumer.partitions_for_topic('workflow-jobs')
                break
            except Exception as e:
                print(f"Kafka not ready, retrying ({attempt + 1}/30)...")
                time.sleep(2)
        else:
            raise Exception("Could not connect to Kafka after 30 attempts")

        print(f"Worker started, listening for jobs in group: {group_id}...")

        for message in self.consumer:
            try:
                # Safely decode and parse the JSON inside the loop
                raw_value = message.value.decode('utf-8')
                job_dict = json.loads(raw_value)
                job = JobMessage.from_dict(job_dict)
            except Exception as e:
                print(f"Failed to decode or parse message, skipping. Error: {e}")
                continue

            handler = self.handlers.get(job.action)
            if not handler:
                # Ignore silently, meant for another worker type
                continue

            print(f"Processing job: {job.action}")

            try:
                result = handler(job.payload)
            except Exception as e:
                # Safely construct JSON error
                result = json.dumps({"error": str(e)})

            response = ResultMessage(job.workflow_run_id, job.action, result)

            self.producer.send('workflow-results', value=response.to_dict())

        self.producer.flush()