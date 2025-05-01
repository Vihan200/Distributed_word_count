import threading
import time
import random
import json
from confluent_kafka import Producer, Consumer
from sidecar import Sidecar

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.role = "idle"
        self.last_heartbeat = None
        self.leader_id = None
        self.alive = True
        self.sidecar = Sidecar(node_id)
        self.node_data = {} 
        self.word_count = {}
        self.range = None
        self.from_leader_finalized = 1
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': f'node-{node_id}',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['cluster-broadcast', f'node-{node_id}'])
        self.accepted_counts = [] 
        self.final_results = {} 
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})
        
        self.nodes = set() 
        threading.Thread(target=self.listen_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_check, daemon=True).start()

        self.add_node()
        if self.node_id > 105:
            self.start_election()

    def set_node_data(self, node_id, key, value):
        """Store data for a specific node"""
        if node_id not in self.node_data:
            self.node_data[node_id] = {}
        self.node_data[node_id][key] = value
    

    def get_node_data(self, node_id, key):
        """Retrieve data for a specific node"""
        return self.node_data.get(node_id, {}).get(key)


    def listen_loop(self):
        while self.alive:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            data = self.sidecar.proxy_receive(msg)
            if data:
                self.handle_message(data)

    def send(self, topic, data):
        self.sidecar.proxy_send(self.producer, topic, data)

    def handle_message(self, msg):
        msg_type = msg.get("type")
        # if msg_type == 'election':
            # self.handle_election(msg)
        if msg_type == 'hello':
            self.handle_new_node(msg)
        elif msg_type == 'election':
            self.handle_election(msg)
        elif msg_type == 'heartbeat_act':
            self.heartbeat_act(msg)    
        elif msg_type == 'coordinator':
            self.handle_coordinator(msg)
        elif msg_type == 'heartbeat':
            self.monitor_heartbeat(msg)
        elif msg_type == 'assign_role':
            self.assign_role(msg)
        elif msg_type == 'remove_node':
            self.remove_node(msg)
        elif msg_type == 'word_count_line':
            self.process_word_count_line(msg)
        elif msg_type == 'word_count_result':
            self.process_word_count_result(msg)
        elif msg_type == 'validated_result':
            self.process_validated_result(msg)
            

    def add_node(self):
        self.send('cluster-broadcast', {'type': 'hello', 'from': self.node_id})

    def handle_new_node(self, msg):
        self.nodes.add(msg['from'])

        # self.sidecar.log(f"Current nodes: {self.nodes}")
        # self.sidecar.log(f"Leader: {self.leader_id}")

    def remove_node(self, msg):
        node_id = msg['id']
        if node_id in self.nodes:
            self.nodes.remove(node_id)
            self.sidecar.log(f"Removed node {node_id} from cluster")
            if node_id == self.leader_id:
                self.node_data = {}
                self.leader_id = None
                self.start_election()
            else:
                self.assign_roles() 


    def handle_election(self, msg):
        from_id = msg['from']
        if from_id < self.node_id and self.leader_id is not None:
            self.send(f'node-{from_id}', {'type': 'election_response', 'from': self.node_id})
            self.start_election()

    # def check_leader_response(self):
    #     if not self.leader_id:
            # self.declare_leader()  

    def start_election(self):
        # if self.leader_id is not None:
            # self.nodes.remove(self.leader_id)
        if self.leader_id == self.node_id:
            return;    
        self.leader_id = None
        self.sidecar.log("Initiating Bully election...")
        # self.sidecar.log(f"{self.nodes}")

        higher_nodes = [n for n in self.nodes if n > self.node_id]
        if not higher_nodes:
            self.declare_leader()
        else:
            for node in higher_nodes:
                # self.sidecar.log(f"{node}")
                self.send(f'node-{node}', {'type': 'election', 'from': self.node_id})
            # threading.Timer(5, self.check_leader_response).start()

    def declare_leader(self):
        self.leader_id = self.node_id
        self.last_heartbeat = None
        self.role = 'coordinator'
        self.sidecar.log("I am the new leader (Coordinator).")
        self.send('cluster-broadcast', {'type': 'coordinator', 'id': self.node_id})
        self.assign_roles()

    def handle_coordinator(self, msg):
        self.leader_id = msg['id']
        self.sidecar.log(f"New leader recognized: Node {self.leader_id}")
    def start_word_count(self):
        self.sidecar.log(f"############## Word Count Start ##############")
        
        # proposers = [node for node in self.nodes 
        #             if self.get_node_data(node, "role") == "proposer"]
        
        # if not proposers:
        #     self.sidecar.log("No proposers available for word count")
        #     return
        
        try:
            with open("document.txt", "r") as file:
                # First count all lines to get total
                lines = [line.strip() for line in file if line.strip()]
                total_lines = len(lines)
                
                # Then send each line with the total count
                for line in lines:
                    self.send('cluster-broadcast', {
                        'type': 'word_count_line',
                        'line': line,
                        'total_lines': total_lines,  # Include total count
                        'current_line': lines.index(line) + 1,  # Current line number
                        'from': self.node_id
                    })
                    time.sleep(0.1) 
                        
            self.sidecar.log("Finished sending document lines to proposers")
            
        except FileNotFoundError:
            self.sidecar.log("Error: document.txt not found")
        except Exception as e:
            self.sidecar.log(f"Error reading document: {str(e)}")
    def process_word_count_line(self, msg):
        if self.role == 'proposer':
            line = msg['line']
            if msg['current_line'] == 1:
                self.word_count = {}  # Reset for new counting session
                
            for char_code in range(ord(self.range[0]), ord(self.range[1]) + 1):
                letter = chr(char_code).lower()
                words_for_letter = [word.lower() for word in line.split() 
                                if word and word[0].lower() == letter]
                
                if letter not in self.word_count:
                    self.word_count[letter] = {
                        "count": 0,
                        "words": set()  # Using set to avoid duplicates
                    }
                    
                self.word_count[letter]["count"] += len(words_for_letter)
                self.word_count[letter]["words"].update(words_for_letter)
                
            if msg['current_line'] == msg['total_lines']:
                # Convert sets to sorted lists for consistent output
                for letter in self.word_count:
                    self.word_count[letter]["words"] = sorted(self.word_count[letter]["words"])
                    
                self.sidecar.log("===== Word Count (Proposer) =====")
                self.sidecar.log(f"{'Letter':<15} {'Count':<10} {'Words'}")
                self.sidecar.log("-" * 50)
                for letter in sorted(self.word_count.keys()):
                    count = self.word_count[letter]["count"]
                    words = ', '.join(self.word_count[letter]["words"])
                    self.sidecar.log(f"{letter.upper():<15} {count:<10} {words}")
                self.sidecar.log("=================================")
                
                self.send('cluster-broadcast', {
                    'type': 'word_count_result',
                    'from': self.node_id,
                    'data': self.word_count
                })


    def process_word_count_result(self, msg):
        if self.role == 'acceptor':
            proposed_data = msg['data']
            #todo Simulate validation 
            self.sidecar.log(f"Result validate from proposer {msg['from']}")

            is_valid = True  # Extend this with real validation logic if needed
            if is_valid:
                self.accepted_counts.append(proposed_data)
                self.send('cluster-broadcast', {
                    'type': 'validated_result',
                    'from': self.node_id,
                    'data': proposed_data
                })

    def process_validated_result(self, msg):
        if self.role == 'learner':
            incoming = msg['data']
            for letter, info in incoming.items():
                if letter not in self.final_results:
                    self.final_results[letter] = {
                        "count": info['count'],
                        "words": set(info.get('words', []))
                    }
                else:
                    self.final_results[letter]['count'] = info['count']
                    self.final_results[letter]['words'].update(info.get('words', []))
                    
            # Prepare table format output
            self.sidecar.log("===== Final Word Count Summary =====")
            self.sidecar.log(f"{'Letter':<15} {'Count':<10} {'Words'}")
            self.sidecar.log("-" * 50)
            
            # Sort letters and words for consistent output
            for char in "abcdefghijklmnopqrstuvwxyz":
                char = char.lower()
                if char in self.final_results:
                    count = self.final_results[char]['count']
                    words = ', '.join(sorted(self.final_results[char]['words']))
                    self.sidecar.log(f"{char.upper():<15} {count:<10} {words}")
                else:
                    self.sidecar.log(f"{char.upper():<15} {0:<10}")
                    
            self.sidecar.log("====================================")


    def heartbeat_check(self):
        while self.alive:
            time.sleep(5)
            if self.leader_id == self.node_id:
                self.send('cluster-broadcast', {'type': 'heartbeat', 'from': self.node_id})
                self.sidecar.log(f"{self.role} Heartbeat: Sent")
                self.role = 'coordinator'
                self.from_leader_finalized = self.from_leader_finalized + 1
                if self.from_leader_finalized > 3:
                    self.from_leader_finalized = 1
                    self.start_word_count()
            else:
                self.from_leader_finalized = 1
                if self.last_heartbeat is None:
                    self.sidecar.log("Waiting for first heartbeat...")
                else:
                    delay = time.time() - self.last_heartbeat
                    if delay > 15:
                        self.sidecar.log(f"Leader {self.leader_id} heartbeat timeout, starting election")
                        if self.leader_id is not None and self.leader_id != self.node_id:
                            self.send('cluster-broadcast', {'type': 'remove_node', 'from': self.node_id, 'id': self.leader_id})
                            self.nodes.discard(self.leader_id) 
                            self.sidecar.log(f"Removed leader {self.leader_id} from nodes list")
                        self.last_heartbeat = None
                        self.leader_id = None
                        self.start_election()

    def heartbeat_act(self,msg):
        self.set_node_data(msg['from'],"heartbeat",time.time())
        if msg['from'] not in self.nodes:
            self.nodes.add(msg['from']) 
            self.assign_roles()      
             # self.sidecar.log(f"{self.node_data}")
 
    def monitor_heartbeat(self, msg):
        if self.leader_id == self.node_id:
            current_time = time.time()
            for node in list(self.nodes): 
                heartbeat_time = self.get_node_data(node, "heartbeat")
                if heartbeat_time and (current_time - float(heartbeat_time)) > 10:
                    self.nodes.discard(node)
                    self.sidecar.log(f"Removed dead node: {node}")
                    self.send('cluster-broadcast', {'type': 'remove_node','from': self.node_id,'id': node})

        if self.leader_id != self.node_id:
            self.last_heartbeat = time.time()
            if self.role == 'proposer':
                self.sidecar.log(f"{self.role} : Range: {self.range} Leader Heartbeat Received from:  {msg['from']}")
            else:
                self.sidecar.log(f"{self.role} Leader Heartbeat Received from:  {msg['from']}")
            # self.sidecar.log(f"{self.nodes}")

            self.send('cluster-broadcast', {'type': 'heartbeat_act', 'from': self.node_id})

         
    def assign_roles(self):
        self.sidecar.log("################ Started Role Assignning ##################")
        all_nodes = sorted(list(self.nodes | {self.node_id}))
        other_nodes = [n for n in all_nodes if n != self.leader_id]
        proposer_count = len(other_nodes) // 2
        acceptor_count = len(other_nodes) - proposer_count - 1
        role_pattern =   ['learner'] + ['proposer'] * proposer_count + ['acceptor'] * acceptor_count
        for i, node in enumerate(other_nodes):
            payload = {'type': 'assign_role', 'role': role_pattern[i]}
            if role_pattern[i] == 'proposer':
                payload['range'] = self.get_range(i-1,proposer_count)
            self.send(f'node-{node}', payload) 
        self.send(f'node-{self.leader_id}', {'type': 'assign_role', 'role': 'coordinator'})

    def get_range(self, i, proposer_count):
        letters = [char for char in "abcdefghijklmnopqrstuvwxyz"]
        total_letters = len(letters)
        letters_per_proposer = max(1, total_letters // proposer_count)
        start_idx = i * letters_per_proposer
        end_idx = start_idx + letters_per_proposer
        if i == proposer_count - 1:
            end_idx = total_letters
        start_idx = min(start_idx, total_letters - 1)
        end_idx = min(end_idx, total_letters)
        start_char = letters[start_idx]
        end_char = letters[end_idx - 1] if end_idx > 0 else letters[-1]
        
        return (start_char, end_char)
         
    def assign_role(self, msg):
        self.role = msg['role']
        if self.role == 'proposer':
            self.range = msg.get('range')
        self.sidecar.log(f"Role assigned: {self.role}, Range: {self.range}")
