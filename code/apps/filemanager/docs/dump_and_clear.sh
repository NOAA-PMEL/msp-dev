#!/bin/bash

# Usage: ./dump_and_clear.sh /path/to/usb/mount
USB_PATH=$1
NAMESPACE="raz1-system" # Change if filemanager is in a different namespace

if [ -z "$USB_PATH" ]; then
  echo "Error: Please provide the path to your USB drive."
  echo "Example: ./dump_and_clear.sh /media/acg/USB_DRIVE"
  exit 1
fi

# Create a timestamped backup folder on the USB
BACKUP_DIR="$USB_PATH/filemanager_dump_$(date +%Y%m%d_%H%M)"
mkdir -p "$BACKUP_DIR"

# 1. Get the exact name of the filemanager pod
echo "Locating filemanager pod..."
POD=$(kubectl get pods -n $NAMESPACE -l app=filemanager -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD" ]; then
  echo "Error: Could not find a running filemanager pod."
  exit 1
fi

# 2. Transfer Data using kubectl cp
# (Note: kubectl cp copies the contents of /data out of the pod to your USB)
echo "Transferring data from $POD to $BACKUP_DIR..."
kubectl cp $NAMESPACE/$POD:/data "$BACKUP_DIR/"

# 3. Verify transfer was successful BEFORE deleting
if [ $? -eq 0 ]; then
  echo "Transfer successful! Verifying file sizes..."
  du -sh "$BACKUP_DIR"
  
  # 4. Clear the data inside the PVC
  echo "Clearing data inside the PVC..."
  kubectl exec -n $NAMESPACE $POD -- sh -c 'rm -rf /data/data/* /data/status/*'
  
  # 5. Restart the pod to release open file handles (CRITICAL)
  echo "Restarting filemanager pod to release file descriptors..."
  kubectl rollout restart deployment filemanager -n $NAMESPACE
  kubectl rollout status deployment filemanager -n $NAMESPACE
  
  echo "Maintenance complete. System is ready for the next deployment."
else
  echo "Error: Data transfer failed. Aborting deletion to prevent data loss."
  exit 1
fi