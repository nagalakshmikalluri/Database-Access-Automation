import sqlite3
import logging
import yaml
import os
from db_management import SQLiteDBHandler

def load_settings_config():
    """Load settings configuration from the settings YAML file."""
    with open('/final/settings.yaml', 'r') as file:
        settings_config = yaml.safe_load(file)
    return settings_config

def load_databases_config():
    """Load databases configuration from the databases YAML file."""
    with open('/final/databases.yaml', 'r') as file:
        databases_config = yaml.safe_load(file)
    return databases_config

# Load settings configuration
settings_config = load_settings_config()

# Use paths from settings.yaml
LOG_FILE_PATH = settings_config['logging']['remove_active_users_log_file']

# Create necessary directories
if not os.path.exists(os.path.dirname(LOG_FILE_PATH)):
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

# Set up logging
logging.basicConfig(filename=LOG_FILE_PATH, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()



def remove_expired_temporary_users():
    """Delete expired temporary users from the SQLite database."""
    sqlite_handler = SQLiteDBHandler(settings_config['sqlite']['db_files'][0])

    with sqlite_handler.connect() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM active_users WHERE timestamp < datetime("now", "-30 minutes")')
        conn.commit()
        logger.info("Removed expired temporary users from the active_users table.")

if __name__ == "__main__":
    remove_expired_temporary_users()


