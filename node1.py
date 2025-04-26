import threading
import time
import json
from confluent_kafka import Consumer, Producer, KafkaException
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - Node %(node_id)s - %(message)s',
)
logger = logging.getLogger(__name__)

class Node1:
    def __init__(self, node_id):
        self.node_id = node_id
        self.alive = True
        self.leader_id = None
        self.nodes = set()
        self.election_in_progress = False
        self.known_higher_nodes = set()
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 3
        self.heartbeat_timeout = 10
        
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': f'node-group-{node_id}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'heartbeat.interval.ms': 2000
        }
        
        # Topics
        self.election_topic = 'leader-election'
        self.heartbeat_topic = 'heartbeat'
        self.coordination_topic = 'node-coordination'
        
        # Initialize Kafka producer and consumer
        try:
            self.producer = Producer({
                'bootstrap.servers': self.kafka_config['bootstrap.servers'],
                'message.timeout.ms': 5000
            })
            
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.election_topic, self.heartbeat_topic, self.coordination_topic])
            
            logger.info(f"Kafka client initialized successfully", extra={'node_id': self.node_id})
        except Exception as e:
            logger.error(f"Failed to initialize Kafka client: {e}", extra={'node_id': self.node_id})
            raise
        
        # Register node
        self.register_node()
        
        # Start threads
        self.consume_thread = threading.Thread(target=self.consume_messages)
        self.consume_thread.daemon = True
        self.consume_thread.start()
        
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        self.monitor_thread = threading.Thread(target=self.monitor_leader)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        logger.info(f"Node initialized and ready", extra={'node_id': self.node_id})

    def _heartbeat_loop(self):
        """Continuously send heartbeats if this node is the leader"""
        while self.alive:
            self.send_heartbeat()
            time.sleep(self.heartbeat_interval)

    def send_heartbeat(self):
        """Send heartbeat if this node is the leader"""
        if self.leader_id == self.node_id:
            try:
                message = {
                    'type': 'heartbeat',
                    'node_id': self.node_id,
                    'timestamp': time.time()
                }
                self.producer.produce(
                    self.heartbeat_topic, 
                    json.dumps(message),
                    callback=self._delivery_report
                )
                logger.debug(f"Sent heartbeat", extra={'node_id': self.node_id})
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}", extra={'node_id': self.node_id})

    def _delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}", extra={'node_id': self.node_id})
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]", 
                        extra={'node_id': self.node_id})

    def register_node(self):
        """Register this node with the cluster"""
        try:
            message = {
                'type': 'node_join',
                'node_id': self.node_id,
                'timestamp': time.time()
            }
            self.producer.produce(
                self.coordination_topic, 
                json.dumps(message),
                callback=self._delivery_report
            )
            self.producer.flush()
            logger.info(f"Successfully registered with cluster", extra={'node_id': self.node_id})
        except Exception as e:
            logger.error(f"Failed to register node: {e}", extra={'node_id': self.node_id})

    def unregister_node(self):
        """Unregister this node from the cluster"""
        message = {
            'type': 'node_leave',
            'node_id': self.node_id,
            'timestamp': time.time()
        }
        self.send_message(self.coordination_topic, message)

    def send_message(self, topic, message):
        """Send a message to a Kafka topic"""
        try:
            self.producer.produce(
                topic, 
                json.dumps(message),
                callback=self._delivery_report
            )
            logger.debug(f"Sent message to {topic}: {message}", extra={'node_id': self.node_id})
        except BufferError:
            logger.warning(f"Producer buffer full, waiting to send message", extra={'node_id': self.node_id})
            self.producer.flush()
            self.producer.produce(topic, json.dumps(message))
        except KafkaException as e:
            logger.error(f"Failed to send message: {e}", extra={'node_id': self.node_id})

    def consume_messages(self):
        """Consume messages from Kafka topics"""
        logger.info("Starting message consumer", extra={'node_id': self.node_id})
        
        while self.alive:
            try:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}", extra={'node_id': self.node_id})
                        continue
                
                try:
                    message = json.loads(msg.value())
                    topic = msg.topic()
                    
                    logger.debug(f"Received message from {topic}: {message}", extra={'node_id': self.node_id})
                    
                    if topic == self.election_topic:
                        self.handle_election_message(message)
                    elif topic == self.heartbeat_topic:
                        self.handle_heartbeat_message(message)
                    elif topic == self.coordination_topic:
                        self.handle_coordination_message(message)
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}", extra={'node_id': self.node_id})
                except Exception as e:
                    logger.error(f"Error processing message: {e}", extra={'node_id': self.node_id})
                    
            except KeyboardInterrupt:
                self.alive = False
                break
            except Exception as e:
                logger.error(f"Unexpected error in consumer: {e}", extra={'node_id': self.node_id})
                time.sleep(1)

    def handle_election_message(self, message):
        """Handle leader election messages"""
        msg_type = message['type']
        
        if msg_type == 'election':
            sender_id = message['node_id']
            
            if sender_id < self.node_id:
                logger.info(f"Received election from lower ID node {sender_id}, starting own election", 
                           extra={'node_id': self.node_id})
                self.send_message(self.election_topic, {
                    'type': 'answer',
                    'node_id': self.node_id,
                    'to_node_id': sender_id
                })
                
                if not self.election_in_progress:
                    self.start_election()
            else:
                self.known_higher_nodes.add(sender_id)
        
        elif msg_type == 'answer':
            if message['to_node_id'] == self.node_id:
                self.known_higher_nodes.add(message['node_id'])
        
        elif msg_type == 'coordinator':
            new_leader = message['node_id']
            self.leader_id = new_leader
            self.election_in_progress = False
            self.known_higher_nodes.clear()
            
            if new_leader == self.node_id:
                logger.info(f"Node {self.node_id} is now the leader", extra={'node_id': self.node_id})
            else:
                logger.info(f"Node {new_leader} is the new leader", extra={'node_id': self.node_id})

    def handle_heartbeat_message(self, message):
        """Handle heartbeat messages from the leader"""
        if message['node_id'] == self.leader_id:
            self.last_heartbeat = time.time()

    def handle_coordination_message(self, message):
        """Handle node join/leave messages"""
        msg_type = message['type']
        node_id = message['node_id']
        
        if msg_type == 'node_join':
            if node_id not in self.nodes:
                self.nodes.add(node_id)
                logger.info(f"Node {node_id} joined the cluster", extra={'node_id': self.node_id})
                
                if self.leader_id == self.node_id:
                    self.send_heartbeat()
        
        elif msg_type == 'node_leave':
            if node_id in self.nodes:
                self.nodes.remove(node_id)
                logger.info(f"Node {node_id} left the cluster", extra={'node_id': self.node_id})
                
                if node_id == self.leader_id:
                    logger.info(f"Leader {node_id} left, starting election", extra={'node_id': self.node_id})
                    self.start_election()

    def start_election(self):
        """Start a leader election"""
        if self.election_in_progress:
            return
            
        self.election_in_progress = True
        self.known_higher_nodes.clear()
        
        higher_nodes = [n for n in self.nodes if n > self.node_id]
        
        if not higher_nodes:
            self.declare_leader()
            return
        
        logger.info(f"Starting election, challenging nodes: {higher_nodes}", extra={'node_id': self.node_id})
        
        for node_id in higher_nodes:
            self.send_message(self.election_topic, {
                'type': 'election',
                'node_id': self.node_id,
                'to_node_id': node_id
            })
        
        threading.Timer(5.0, self.check_election).start()

    def check_election(self):
        """Check election status after waiting period"""
        if not self.election_in_progress:
            return
            
        if not self.known_higher_nodes:
            self.declare_leader()
        else:
            logger.info(f"Waiting for higher nodes {self.known_higher_nodes} to declare leadership", 
                       extra={'node_id': self.node_id})

    def declare_leader(self):
        """Declare this node as the leader"""
        self.leader_id = self.node_id
        self.election_in_progress = False
        self.known_higher_nodes.clear()
        
        self.send_message(self.election_topic, {
            'type': 'coordinator',
            'node_id': self.node_id
        })
        
        logger.info(f"Node {self.node_id} declared itself as leader", extra={'node_id': self.node_id})

    def monitor_leader(self):
        """Monitor leader heartbeats and start election if leader is down"""
        while self.alive:
            if (self.leader_id is not None and 
                self.leader_id != self.node_id and 
                time.time() - self.last_heartbeat > self.heartbeat_timeout):
                
                logger.info(f"Leader {self.leader_id} heartbeat timeout, starting election", 
                           extra={'node_id': self.node_id})
                self.start_election()
            
            time.sleep(1)

    def shutdown(self):
        """Clean shutdown of the node"""
        logger.info("Starting shutdown procedure", extra={'node_id': self.node_id})
        self.alive = False
        
        try:
            self.unregister_node()
            self.consumer.close()
            self.producer.flush()
            logger.info("Kafka clients closed", extra={'node_id': self.node_id})
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", extra={'node_id': self.node_id})
        
        logger.info(f"Node shutdown complete", extra={'node_id': self.node_id})

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)
    
    node_id = int(sys.argv[1])
    node = Node1(node_id)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.shutdown()