#!/bin/bash

set -e

LOCAL_DIR="$HOME/dataengineering"
REMOTE_DIR="/home/ec2-user/dataengineering"

echo "Syncing .env files..."

echo "→ AirflowInstance"

rsync -avz \
  "$LOCAL_DIR/.env" \
  AirflowInstance:$REMOTE_DIR/
rsync -avz \
  "$LOCAL_DIR/.env" \
  AirflowInstance:$REMOTE_DIR/docker/airflow/

echo "→ SparkInstance"

rsync -avz \
  "$LOCAL_DIR/.env" \
  SparkInstance:$REMOTE_DIR/
rsync -avz \
  "$LOCAL_DIR/.env" \
  SparkInstance:$REMOTE_DIR/docker/spark/

echo "→ DatabaseInstance"

rsync -avz \
  "$LOCAL_DIR/.env" \
  DatabaseInstance:$REMOTE_DIR/
rsync -avz \
  "$LOCAL_DIR/.env" \
  DatabaseInstance:$REMOTE_DIR/docker/database/

echo "Sync completed successfully."
