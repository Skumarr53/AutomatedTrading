#!/bin/bash
# Get the process ID of the running Python script
PID=$(ps aux | grep 'python main.py' | grep -v grep | awk '{print $2}')

# Kill the process if it's running
if [ ! -z "$PID" ]; then
  kill $PID
fi
