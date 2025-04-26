#!/bin/bash

echo "Starting Kafka containers..."
docker compose -f kafka-docker-compose.yml up -d

echo "Waiting 10 seconds for Kafka to initialize..."
sleep 5

TERMINAL_EMULATOR=""

# Check which terminal emulators are available
if command -v xterm &> /dev/null; then
    TERMINAL_EMULATOR="xterm -fa 'Monospace' -fs 10 -title"
elif command -v gnome-terminal &> /dev/null; then
    TERMINAL_EMULATOR="gnome-terminal --tab --title"
elif command -v konsole &> /dev/null; then
    TERMINAL_EMULATOR="konsole --separate --new-tab -p tabtitle="
elif command -v terminator &> /dev/null; then
    TERMINAL_EMULATOR="terminator --new-tab --title="
else
    echo "ERROR: No supported terminal emulator found!"
    echo "Please install one of: xterm, gnome-terminal, konsole, terminator"
    exit 1
fi

# Start nodes
echo "Starting nodes..."
for i in {100..106}; do
    if [[ "$TERMINAL_EMULATOR" == *"xterm"* ]]; then
        $TERMINAL_EMULATOR "Node $i" -e "bash -c \"python3 -c 'from node import Node; n = Node($i); import time; time.sleep(3600)'; read -p 'Press Enter to close...'\"" &
    else
        $TERMINAL_EMULATOR"Node $i" -- bash -c "python3 -c 'from node import Node; n = Node($i); import time; time.sleep(3600)'; read -p 'Press Enter to close...'" &
    fi
    sleep 0.5  # Stagger window creation
done

echo "All nodes started in separate terminals"