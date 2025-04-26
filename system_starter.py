import threading
import time
from node import Node
import random

def start_node(node_id):
    node = Node(node_id)
    
    # Keep the node running
    while node.alive:
        time.sleep(1)

if __name__ == "__main__":
    node_count = 15
    start_id = 100
    threads = []
    
    for i in range(node_count):
        node_id = start_id + i
        t = threading.Thread(target=start_node, args=(node_id,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()