import threading
import time
import random
import json
from confluent_kafka import Producer, Consumer
from sidecar import Sidecar

class Node4:
    def __init__(self, node_id):
        self.node_id = node_id
        self.role = "idle"
        self.leader_id = None
        self.alive = True
        self.sidecar = Sidecar(node_id)

        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': f'node-{node_id}',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['cluster-broadcast', f'node-{node_id}'])

        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

        self.nodes = set(range(100, 107))  # includes 100 to 106
        self.range = None  # for proposers only

        self.start_election()

        threading.Thread(target=self.listen_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_check, daemon=True).start()

    def send(self, topic, data):
        self.sidecar.proxy_send(self.producer, topic, data)

    def listen_loop(self):
        while self.alive:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            data = self.sidecar.proxy_receive(msg)
            if data:
                self.handle_message(data)

    def handle_message(self, msg):
        msg_type = msg.get("type")
        if msg_type == 'election':
            self.handle_election(msg)
        elif msg_type == 'coordinator':
            self.handle_coordinator(msg)
        elif msg_type == 'assign_role':
            self.assign_role(msg)
        elif msg_type == 'line':
            if self.role == 'proposer':
                self.process_line(msg['data'])
        elif msg_type == 'result':
            if self.role == 'acceptor':
                self.validate_result(msg)
        elif msg_type == 'validated':
            if self.role == 'learner':
                self.aggregate_result(msg)

    def assign_role(self, msg):
        self.role = msg['role']
        if self.role == 'proposer':
            self.range = msg.get('range')
        self.sidecar.log(f"Role assigned: {self.role}, Range: {self.range}")

    def start_election(self):
        self.sidecar.log("Initiating Bully election...")
        higher_nodes = [n for n in self.nodes if n > self.node_id]
        if not higher_nodes:
            self.declare_leader()
        else:
            for node in higher_nodes:
                self.send(f'node-{node}', {'type': 'election', 'from': self.node_id})
            threading.Timer(5, self.check_leader_response).start()

    def handle_election(self, msg):
        from_id = msg['from']
        if from_id < self.node_id:
            self.send(f'node-{from_id}', {'type': 'election_response', 'from': self.node_id})
            self.start_election()

    def check_leader_response(self):
        if not self.leader_id:
            self.declare_leader()

    def declare_leader(self):
        self.leader_id = self.node_id
        self.role = 'coordinator'
        self.sidecar.log("I am the new leader (Coordinator).")
        self.send('cluster-broadcast', {'type': 'coordinator', 'id': self.node_id})
        # self.assign_roles()

    def handle_coordinator(self, msg):
        self.leader_id = msg['id']
        self.sidecar.log(f"New leader recognized: Node {self.leader_id}")

    def assign_roles(self):
        all_nodes = sorted(list(self.nodes | {self.node_id}))
        roles = ['proposer'] * 3 + ['acceptor'] * 2 + ['learner']
        roles += ['idle'] * (len(all_nodes) - len(roles))

        for i, node in enumerate(all_nodes):
            payload = {'type': 'assign_role', 'role': roles[i]}
            if roles[i] == 'proposer':
                payload['range'] = self.get_range(i)
            self.send(f'node-{node}', payload)

    def get_range(self, i):
        ranges = [('a', 'c'), ('d', 'g'), ('h', 'z')]
        return ranges[i % len(ranges)]

    def process_line(self, line):
        count = {}
        words = line.strip().split()
        for word in words:
            word = word.lower()
            if not word.isalpha():
                continue
            if self.range[0] <= word[0] <= self.range[1]:
                count.setdefault(word[0].upper(), []).append(word)
        self.send('cluster-broadcast', {
            'type': 'result',
            'from': self.node_id,
            'data': count
        })

    def validate_result(self, msg):
        self.sidecar.log(f"Validating result from Node {msg['from']}")
        self.send('cluster-broadcast', {
            'type': 'validated',
            'data': msg['data']
        })

    def aggregate_result(self, msg):
        self.sidecar.log(f"Aggregated result received: {msg['data']}")

    def heartbeat_check(self):
        while self.alive:
            time.sleep(5)
            if self.role == "coordinator":
                self.sidecar.log("Heartbeat: Coordinator alive check")
    