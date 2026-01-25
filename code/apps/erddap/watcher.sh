#!/bin/bash
set -e

# 1. Start the Background Watcher
echo "[Wrapper] Starting background watcher for dataset triggers..."
(while true; do
  if [ -f /shared-state/trigger_datasets ]; then
    echo "[Watcher] Trigger detected. Attempting to run datasets.d.sh..."
    
    # Check if the target script exists (standard location in axiom image)
    if [ -f /usr/local/bin/datasets.d.sh ]; then
        # Run the script
        bash /usr/local/bin/datasets.d.sh
    else
        echo "[Watcher] Error: /usr/local/bin/datasets.d.sh not found!"
    fi
    
    # Remove trigger
    rm -f /shared-state/trigger_datasets
    echo "[Watcher] Done. Waiting for next trigger..."
  fi
  sleep 2
done) &

# 2. Start the Original ERDDAP Entrypoint
# We use 'exec' so this process replaces the shell and receives OS signals (SIGTERM)
echo "[Wrapper] Launching original ERDDAP entrypoint..."
exec /jetty-entrypoint.sh