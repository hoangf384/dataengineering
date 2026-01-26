# --- SETTINGS FILE ---
import os

# Env variables
TAILSCALE_IP = os.getenv("TAILSCALE_IP")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_URL = f"jdbc:mysql://{TAILSCALE_IP}:3306/{MYSQL_DATABASE}?allowPublicKeyRetrieval=true&useSSL=false&rewriteBatchedStatements=true"

# Constants
NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01B21DD213814000
SCHEMA = (
    "create_time", "job_id", "custom_track", "bid",
    "campaign_id", "group_id", "publisher_id"
)