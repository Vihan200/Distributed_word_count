import json
import os
import datetime

class Sidecar:
    def __init__(self, node_id):
        self.node_id = node_id
        self.log_file = f"logs/node_{node_id}.log"
        os.makedirs("logs", exist_ok=True)
        self.log(f"[BOOT] Node {node_id} initialized.")

    def log(self, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_msg = f"[{timestamp}] Node {self.node_id}: {message}"
        print(full_msg)  # Console log for debugging
        with open(self.log_file, "a") as f:
            f.write(full_msg + "\n")

    def proxy_send(self, kafka_producer, topic, data):
        """
        Acts as a middleware between the main node and Kafka.
        Logs and sends data.
        """
        log_msg = f"[SEND] To topic `{topic}`: {data}"
        # self.log(log_msg)

        kafka_producer.produce(topic, json.dumps(data).encode('utf-8'))
        kafka_producer.flush()

    def proxy_receive(self, msg):
        """
        Middleware to receive and log Kafka messages.
        """
        try:
            data = json.loads(msg.value().decode('utf-8'))
            # self.log(f"[RECEIVE] From topic `{msg.topic()}`: {data}")
            return data
        except Exception as e:
            self.log(f"[ERROR] Failed to decode message: {e}")
            return None
