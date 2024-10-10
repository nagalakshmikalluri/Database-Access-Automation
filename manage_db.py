import argparse
import yaml
import logging
import os
from db_management import SQLiteDBHandler, PostgreSQLDBHandler, generate_password
from datetime import datetime
import psycopg2
import random
import string

# Function to load settings from the YAML file
def load_settings_config():
    """Load settings configuration from the settings YAML file."""
    with open('/testing/settings.yaml', 'r') as file:
        settings_config = yaml.safe_load(file)
    return settings_config

# Load settings configuration at the beginning
settings_config = load_settings_config()

# Paths taken from settings.yaml
LOG_FILE_PATH = settings_config['logging']['manage_db_log_file']
PGPASS_FILE_PATH = settings_config['pgpass']['pgpass_file_path']
AUDIT_LOG_FILE_PATH = settings_config['logging']['audit_log_file']
SCRIPT_LOG_FILE_PATH = settings_config['logging']['script_log_file']

# Ensure the log directory exists
if not os.path.exists(os.path.dirname(LOG_FILE_PATH)):
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

# Set up logging
logging.basicConfig(filename=LOG_FILE_PATH, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def load_config(databases_file):
    """Load the database configuration from a YAML file."""
    with open(databases_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def update_pgpass_file(host, port, dbname, username, password):
    """Update the .pgpass file with new credentials."""
    pgpass_file_path = os.path.expanduser("~/.pgpass")
    logger.info(f"Updating .pgpass file at: {pgpass_file_path} with {host}:{port}:{dbname}:{username}:{password}")
    
    print("Updated")
    with open(pgpass_file_path, 'a') as file:
        file.write(f"{host}:{port}:{dbname}:{username}:{password}\n")
    
    os.chmod(pgpass_file_path, 0o600)  # Ensure the file has the correct permissions

    
    with open(pgpass_file_path, 'r') as file:
        contents = file.read()
        logger.info(f".pgpass file contents after update:\n{contents}")




def generate_random_suffix():
    """Generate a random suffix of 6 alphanumeric characters."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

# def create_temporary_user(dbname, schema_name=None, timestamp=None, host=None, databases_file=None):
#     """Create a temporary user for the specified database."""
#     if not dbname or not host or not databases_file:
#         raise ValueError("dbname, host, and databases_file must be specified.")
    
#     # Load configuration settings
#     config = load_config(databases_file)
    
#     # Fetch PostgreSQL configuration
#     db_config = next((db for db in config['databases']['postgres'] if db['dbname'] == dbname and db['host'] == host), None)

#     if not db_config:
#         error_message = f"No configuration found for database '{dbname}' on host '{host}'."
#         logger.error(error_message)
#         print(error_message)
#         return

#     # Path to SQLite database
#     sqlite_db_file = settings_config['sqlite']['db_files'][0]
#     sqlite_handler = SQLiteDBHandler(sqlite_db_file)

#     # Initialize SQLite database
#     sqlite_handler.initialize_db()

#     # Set up PostgreSQL handler and create a random master user
#     pg_handler = PostgreSQLDBHandler(databases_file, sqlite_handler=sqlite_handler)
    
#     master_user_suffix = generate_random_suffix()
#     master_username = f"database_access_automation_user_{master_user_suffix}"
#     master_password = generate_password()

#     update_pgpass_file(host, db_config['port'], dbname, generated_username, generated_password)
    
#     #pg_handler.create_user(master_username, master_password, is_master=True)
    
#     pg_handler.create_master_user()
    
#     logger.info(f"Created master user '{master_username}' with password '{master_password}'.")

#     # Create a new random user and grant access
#     user_suffix = generate_random_suffix()
#     generated_username = f"insta_db_user_{user_suffix}"
#     generated_password = generate_password()
    
#     pg_handler.create_user_and_grant_access(generated_username, generated_password, schema_name, master_username)

#     if timestamp is None:
#         timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#     sqlite_handler.add_to_active_user_db(
#         generated_username, generated_password, schema_name or 'default_schema', dbname, host, db_config['port'], timestamp
#     )
#     logger.info(f"Created user '{generated_username}' with password '{generated_password}' for database '{dbname}'.")

#     print(f"Created user '{generated_username}' with password '{generated_password}' for database '{dbname}'.")

# def create_temporary_user(dbname, schema_name=None, timestamp=None, host=None, databases_file=None):
#     """Create a temporary user for the specified database."""
#     if not dbname or not host or not databases_file:
#         raise ValueError("dbname, host, and databases_file must be specified.")
    
#     # Load configuration settings
#     config = load_config(databases_file)
    
#     # Fetch PostgreSQL configuration
#     db_config = next((db for db in config['databases']['postgres'] if db['dbname'] == dbname and db['host'] == host), None)

#     if not db_config:
#         error_message = f"No configuration found for database '{dbname}' on host '{host}'."
#         logger.error(error_message)
#         print(error_message)
#         return

#     # Path to SQLite database
#     sqlite_db_file = settings_config['sqlite']['db_files'][0]
#     sqlite_handler = SQLiteDBHandler(sqlite_db_file)

#     # Initialize SQLite database
#     sqlite_handler.initialize_db()

#     # Set up PostgreSQL handler and create a random master user
#     pg_handler = PostgreSQLDBHandler(databases_file, sqlite_handler=sqlite_handler)
    
#     master_user_suffix = generate_random_suffix()
#     master_username = f"database_access_automation_user_{master_user_suffix}"
#     master_password = generate_password()

#     # Create the master user first
#     pg_handler.create_master_user()

#     # Log master user creation
#     logger.info(f"Created master user '{master_username}' with password '{master_password}'.")

#     # Create a new random user and grant access
#     user_suffix = generate_random_suffix()
#     generated_username = f"insta_db_user_{user_suffix}"
#     generated_password = generate_password()
    
#     pg_handler.create_user_and_grant_access(generated_username, generated_password, schema_name, master_username)

#     # Update .pgpass file with new credentials
#     update_pgpass_file(host, db_config['port'], dbname, generated_username, generated_password)

#     if timestamp is None:
#         timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#     sqlite_handler.add_to_active_user_db(
#         generated_username, generated_password, schema_name or 'default_schema', dbname, host, db_config['port'], timestamp
#     )
#     logger.info(f"Created user '{generated_username}' with password '{generated_password}' for database '{dbname}'.")

#     print(f"Created user '{generated_username}' with password '{generated_password}' for database '{dbname}'.")

def create_temporary_user(dbname, schema_name=None, timestamp=None, host=None, databases_file=None):
    """Create a temporary user for the specified database."""
    if not dbname or not host or not databases_file:
        raise ValueError("dbname, host, and databases_file must be specified.")
    
    # Load configuration settings
    config = load_config(databases_file)
    
    # Fetch PostgreSQL configuration
    db_config = next((db for db in config['databases']['postgres'] if db['dbname'] == dbname and db['host'] == host), None)

    if not db_config:
        error_message = f"No configuration found for database '{dbname}' on host '{host}'."
        logger.error(error_message)
        print(error_message)
        return

    # Path to SQLite database
    sqlite_db_file = settings_config['sqlite']['db_files'][0]
    sqlite_handler = SQLiteDBHandler(sqlite_db_file)

    # Initialize SQLite database
    sqlite_handler.initialize_db()

    # Set up PostgreSQL handler and create a random master user
    pg_handler = PostgreSQLDBHandler(databases_file, sqlite_handler=sqlite_handler)
    
    master_user_suffix = generate_random_suffix()
    master_username = f"database_access_automation_user_{master_user_suffix}"
    master_password = generate_password()

    # Update .pgpass file for master user
    update_pgpass_file(host, db_config['port'], dbname, master_username, master_password)
    
    pg_handler.create_master_user()
    
    logger.info(f"Created master user '{master_username}' with password '{master_password}'.")

    # Create a new random user and grant access
    user_suffix = generate_random_suffix()
    generated_username = f"insta_db_user_{user_suffix}"
    generated_password = generate_password()

    # Update .pgpass file for the new user
    update_pgpass_file(host, db_config['port'], dbname, generated_username, generated_password)

    pg_handler.create_user_and_grant_access(generated_username, generated_password, schema_name, master_username)

    if timestamp is None:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    sqlite_handler.add_to_active_user_db(
        generated_username, generated_password, schema_name or 'default_schema', dbname, host, db_config['port'], timestamp
    )
    logger.info(f"Created user '{generated_username}' with password '{generated_password}' for database '{dbname}'.")

    print(f"Created user '{generated_username}' with password '{generated_password}' for database '{dbname}'.")


def delete_temporary_user(dbname=None):
    """Delete temporary users for the specified database."""
    if not dbname:
        raise ValueError("dbname must be specified.")
    
    sqlite_handler = SQLiteDBHandler(settings_config['sqlite']['db_files'][0])

    with sqlite_handler.connect() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM active_users WHERE database_name = ?', (dbname,))
        conn.commit()
        logger.info(f"Deleted temporary users for database '{dbname}'.")

def delete_expired_temporary_users():
    """Delete expired temporary users from the SQLite database."""
    sqlite_handler = SQLiteDBHandler(settings_config['sqlite']['db_files'][0])

    with sqlite_handler.connect() as conn:
        cursor = conn.cursor()
        # Select users that are expired
        cursor.execute('SELECT * FROM active_users WHERE timestamp < datetime("now", "-30 minutes")')
        users_to_delete = cursor.fetchall()

        # Print the users that will be deleted (for verification)
        for user in users_to_delete:
            print(f"User to delete: {user}")

        # Delete expired users
        cursor.execute('DELETE FROM active_users WHERE timestamp < datetime("now", "-30 minutes")')
        conn.commit()

    # Log the deletions
    logger.info(f"Deleted {len(users_to_delete)} expired users from active_users table.")
    print(f"Deleted {len(users_to_delete)} expired users from active_users table.")

def list_databases():
    """List all databases from the configuration."""
    db_config = load_config(settings_config['databases_file'])

    for db in db_config['databases']['postgres']:
        print(f"Database: {db.get('friendly_name')}, Host: {db.get('host')}, Port: {db.get('port', 5432)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Database Management Script')
    parser.add_argument('--create_temporary_user', action='store_true', help='Create a temporary user')
    parser.add_argument('--delete_temporary_user', action='store_true', help='Delete a temporary user')
    parser.add_argument('--list_databases', action='store_true', help='List all databases')
    parser.add_argument('--delete_expired_temporary_users', action='store_true', help='Run cron job for deleting expired temporary users')
    parser.add_argument('--dbname', type=str, help='Specify the database name')
    parser.add_argument('--schema', type=str, help='Specify the schema name')
    parser.add_argument('--timestamp', type=str, help='Specify the timestamp for the user entry')
    parser.add_argument('--host', type=str, help='Specify the database host')
    parser.add_argument('--databases_file', type=str, help='Path to the databases YAML file')

    args = parser.parse_args()

    if args.create_temporary_user:
        create_temporary_user(args.dbname, args.schema, args.timestamp, args.host, args.databases_file)
    elif args.delete_temporary_user:
        delete_temporary_user(args.dbname)
    elif args.list_databases:
        list_databases()
    elif args.delete_expired_temporary_users:
        delete_expired_temporary_users()
