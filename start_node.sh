#!/bin/bash
NODE_ID=$1

if [ -z "$NODE_ID" ]; then
  echo "Usage: ./start_node.sh <node_id>"
  exit 1
fi

gnome-terminal --title="Node $NODE_ID" -- bash -c "python3 -c 'from node import Node; n = Node($NODE_ID); import time; time.sleep(3600)'; read"
