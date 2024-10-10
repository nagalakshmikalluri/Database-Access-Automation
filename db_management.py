import sqlite3
import logging
import yaml
import os
import secrets
import string
import random
import psycopg2
from psycopg2 import sql
from datetime import datetime

def load_settings_config():
    """Load settings configuration from the settings YAML file."""
    with open('/testing/settings.yaml', 'r') as file:
        settings_config = yaml.safe_load(file)
    return settings_config

# Load settings configuration at the beginning
settings_config = load_settings_config()

# Paths taken from settings.yaml
PGPASS_FILE_PATH = settings_config['pgpass']['pgpass_file_path']
AUDIT_LOG_FILE_PATH = settings_config['logging']['audit_log_file']
SCRIPT_LOG_FILE_PATH = settings_config['logging']['script_log_file']

# Create necessary directories and files
for path in [PGPASS_FILE_PATH, AUDIT_LOG_FILE_PATH, SCRIPT_LOG_FILE_PATH]:
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

for file_path in [PGPASS_FILE_PATH, AUDIT_LOG_FILE_PATH, SCRIPT_LOG_FILE_PATH]:
    if not os.path.exists(file_path):
        open(file_path, 'w').close()

os.chmod(PGPASS_FILE_PATH, 0o600)
os.chmod(AUDIT_LOG_FILE_PATH, 0o644)
os.chmod(SCRIPT_LOG_FILE_PATH, 0o644)

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = logging.FileHandler(SCRIPT_LOG_FILE_PATH)
log_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

audit_logger = logging.getLogger('audit')
audit_handler = logging.FileHandler(AUDIT_LOG_FILE_PATH)
audit_handler.setLevel(logging.INFO)
audit_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
audit_handler.setFormatter(audit_formatter)
audit_logger.addHandler(audit_handler)

script_logger = logging.getLogger('script')
script_handler = logging.FileHandler(SCRIPT_LOG_FILE_PATH)
script_handler.setLevel(logging.INFO)
script_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
script_handler.setFormatter(script_formatter)
script_logger.addHandler(script_handler)

audit_logger.info('This is an audit log message.')
script_logger.info('This is a script log message.')

class SQLiteDBHandler:
    def __init__(self, db_file):
        self.db_file = db_file
        self.initialize_db()

    def connect(self):
        return sqlite3.connect(self.db_file)

    def initialize_db(self):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS active_users (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                username TEXT,
                                password TEXT,
                                schema_name TEXT,
                                database_name TEXT,
                                host TEXT,
                                port INTEGER,
                                timestamp TEXT
                            )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS master_credentials (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                master_username TEXT,
                                master_password TEXT,
                                host TEXT,
                                friendly_name TEXT
                            )''')
            conn.commit()

    def add_master_credentials(self, master_username, master_password, host, friendly_name):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO master_credentials (master_username, master_password, host, friendly_name) VALUES (?, ?, ?, ?)',
                           (master_username, master_password, host, friendly_name))
            conn.commit()
            logger.debug(f"Inserted master credentials: {master_username}, {host}, {friendly_name}")

    def add_to_active_user_db(self, username, password, schema_name, database_name, host, port, timestamp):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO active_users (username, password, schema_name, database_name, host, port, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)',
                           (username, password, schema_name, database_name, host, port, timestamp))
            conn.commit()

    def remove_from_active_user_db(self, username, database_name):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM active_users WHERE username = ? AND database_name = ?', (username, database_name))
            conn.commit()

    def get_master_credentials(self):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT master_username, master_password, host, friendly_name FROM master_credentials ORDER BY id DESC LIMIT 1')
            return cursor.fetchone()


# class PostgreSQLDBHandler:
#     def __init__(self, config_file, sqlite_handler=None):
#         self.config = self.load_config(config_file)
#         self.sqlite_handler = sqlite_handler
#         self.dbname = self.config['databases']['postgres'][0]['friendly_name']
#         self.host = self.config['databases']['postgres'][0]['host']
#         self.port = self.config['databases']['postgres'][0].get('port', 5432)
#         self.master_user = None
#         self.master_password = None

#     def load_config(self, config_file):
#         with open(config_file, 'r') as file:
#             return yaml.safe_load(file)

#     def create_master_user(self):
#         """Create a random master user."""
#         self.master_user = f"database_access_automation_user_{generate_password(6)}"
#         self.master_password = generate_password()

#         try:
#             with psycopg2.connect(
#                 dbname=self.dbname,
#                 user=self.config['databases']['postgres'][0]['user'],
#                 password=self.config['databases']['postgres'][0]['password'],
#                 host=self.host,
#                 port=self.port
            
#             ) as conn:
#                 # conn.autocommit = True
#                 conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
#                 with conn.cursor() as cursor:
#                     cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s CREATEDB CREATEROLE").format(
#                         sql.Identifier(self.master_user)), [self.master_password])
#                     conn.commit()
#                     logger.info(f"Master user '{self.master_user}' created.")

#                     if self.sqlite_handler:
#                         self.sqlite_handler.add_master_credentials(
#                             self.master_user, self.master_password, self.host, self.dbname)
#                     # cursor.execute(sql.SQL("CREATE DATABASE {}").format(
#                     # sql.Identifier(self.master_user)))
#                     # logger.info(f"Database '{self.master_user}' created.")

#                     # cursor.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
#                     # sql.Identifier(self.master_user), sql.Identifier(self.master_user)))
#                     # logger.info(f"Database '{self.master_user}' ownership granted to '{self.master_user}'.")
                    
#                     # Store the credentials securely, e.g., in .pgpass file or another secure storage
#                     # with open(PGPASS_FILE_PATH, 'a') as pgpass_file:
#                     #     pgpass_file.write(f"{self.host}:{self.port}:{self.dbname}:{self.master_user}:{self.master_password}\n")

#                     cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')")
#                     schemas = cursor.fetchall()
#                     for schema in schemas:
#                         schema_name = schema[0]
#                         cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
#                             sql.Identifier(schema_name), sql.Identifier(self.master_user)))
#                         cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
#                             sql.Identifier(schema_name), sql.Identifier(self.master_user)))
#                         logger.info(f"Granted USAGE on schema '{schema_name}' and SELECT on all tables to '{self.master_user}'.")

#                         # Grant default privileges for future tables created in each schema
#                         cursor.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(
#                             sql.Identifier(schema_name), sql.Identifier(self.master_user)))
#                         logger.info(f"Granted default SELECT privileges on future tables in schema '{schema_name}' to '{self.master_user}'.")
#                     with open(PGPASS_FILE_PATH, 'a') as pgpass_file:
#                         pgpass_file.write(f"{self.host}:{self.port}:{self.master_user}:{self.master_password}\n")

#             display=f"host: {self.host} || database: {self.dbname} || database_access_automation_user: {self.master_user} || database_access_automation_password: {self.master_password}"
#             print(display)
#             connection_string = f"psql -h {self.host} -U {self.master_user} -d {self.dbname}"
#             logger.info(f"Connection string for master user --|| {connection_string} ||--")
#             print(f"Connection string for master user --|| {connection_string} ||--")
#         except psycopg2.Error as e:
#             raise RuntimeError(f"Error creating master user or database: {e}")
#     def create_user(self, schema_name):
#         """Create a random `insta_db_user` and grant privileges on a specific schema."""
#         if not self.master_user or not self.master_password:
#             raise RuntimeError("Master user credentials are not set.")

#         try:
#             with psycopg2.connect(
#                 dbname=self.dbname,
#                 user=self.master_user,
#                 password=self.master_password,
#                 host=self.host,
#                 port=self.port
#             ) as conn:
#                 with conn.cursor() as cursor:
#                     new_username = f'insta_db_user_{generate_password(6)}'
#                     new_password = generate_password()

#                     cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
#                         sql.Identifier(new_username)), [new_password])
#                     conn.commit()

#                     cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON SCHEMA {} TO {}").format(
#                         sql.Identifier(schema_name), sql.Identifier(new_username)))
#                     conn.commit()

#                     logger.info(f"User '{new_username}' with password '{new_password}' created and granted access to schema '{schema_name}'.")
#                     return new_username, new_password

#         except psycopg2.Error as e:
#             raise RuntimeError(f"Error creating user or granting access: {e}")
        
        
#     def create_user_and_grant_access(self, generated_username, generated_password, schema_name, master_username):
#         """Create a random `insta_db_user` and grant privileges on a specific schema."""
#         if not self.master_user or not self.master_password:
#             raise RuntimeError("Master user credentials are not set.")

#         try:
#             with psycopg2.connect(
#                 dbname=self.dbname,
#                 user=self.master_user,
#                 password=self.master_password,
#                 host=self.host,
#                 port=self.port
#             ) as conn:
#                 with conn.cursor() as cursor:
#                     new_username = f'insta_db_user_{generate_password(6)}'
#                     new_password = generate_password()

#                     cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
#                         sql.Identifier(new_username)), [new_password])
#                     conn.commit()

#                     cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON SCHEMA {} TO {}").format(
#                         sql.Identifier(schema_name), sql.Identifier(new_username)))
#                     conn.commit()

#                     # self.sqlite_handler.add_to_active_user_db(
#                     #     new_username, new_password, schema_name, self.dbname, self.host, self.port, 
#                     #     datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#                     logger.info(f"User '{new_username}' with password '{new_password}' created and granted access to schema '{schema_name}'.")
#                     return new_username, new_password

#         except psycopg2.Error as e:
#             raise RuntimeError(f"Error creating user or granting access: {e}")


class PostgreSQLDBHandler:
    def __init__(self, config_file, sqlite_handler=None):
        self.config = self.load_config(config_file)
        self.sqlite_handler = sqlite_handler
        self.dbname = self.config['databases']['postgres'][0]['friendly_name']
        self.host = self.config['databases']['postgres'][0]['host']
        self.port = self.config['databases']['postgres'][0].get('port', 5432)
        self.master_user = None
        self.master_password = None

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def create_master_user(self):
        """Create a random master user."""
        self.master_user = f"database_access_automation_user_{generate_password(6)}"
        self.master_password = generate_password()

        try:
            with psycopg2.connect(
                dbname=self.dbname,
                user=self.config['databases']['postgres'][0]['user'],
                password=self.config['databases']['postgres'][0]['password'],
                host=self.host,
                port=self.port
            ) as conn:
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cursor:
                    cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s CREATEDB CREATEROLE").format(
                        sql.Identifier(self.master_user)), [self.master_password])
                    conn.commit()
                    logger.info(f"Master user '{self.master_user}' created.")

                    if self.sqlite_handler:
                        self.sqlite_handler.add_master_credentials(
                            self.master_user, self.master_password, self.host, self.dbname)
                    
                    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')")
                    schemas = cursor.fetchall()
                    for schema in schemas:
                        schema_name = schema[0]
                        cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                            sql.Identifier(schema_name), sql.Identifier(self.master_user)))
                        cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                            sql.Identifier(schema_name), sql.Identifier(self.master_user)))
                        logger.info(f"Granted USAGE on schema '{schema_name}' and SELECT on all tables to '{self.master_user}'.")

                        cursor.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(
                            sql.Identifier(schema_name), sql.Identifier(self.master_user)))
                        logger.info(f"Granted default SELECT privileges on future tables in schema '{schema_name}' to '{self.master_user}'.")

                    # Append credentials to .pgpass
                    try:
                        with open(PGPASS_FILE_PATH, 'a') as pgpass_file:
                            pgpass_file.write(f"{self.host}:{self.port}:{self.dbname}:{self.master_user}:{self.master_password}\n")
                        os.chmod(PGPASS_FILE_PATH, 0o600)  # Ensure correct permissions
                        print("Updated .pgpass file with master user credentials.")
                        logger.info(f"Updated .pgpass file with master user credentials.")
                    except Exception as e:
                        logger.error(f"Error updating .pgpass file: {e}")

            display=f"host: {self.host} || database: {self.dbname} || database_access_automation_user: {self.master_user} || database_access_automation_password: {self.master_password}"
            print(display)
            connection_string = f"psql -h {self.host} -U {self.master_user} -d {self.dbname}"
            logger.info(f"Connection string for master user --|| {connection_string} ||--")
            print(f"Connection string for master user --|| {connection_string} ||--")
        except psycopg2.Error as e:
            raise RuntimeError(f"Error creating master user or database: {e}")

    def create_user_and_grant_access(self, generated_username, generated_password, schema_name, master_username):
        """Create a random `insta_db_user` and grant privileges on a specific schema."""
        if not self.master_user or not self.master_password:
            raise RuntimeError("Master user credentials are not set.")

        try:
            with psycopg2.connect(
                dbname=self.dbname,
                user=self.master_user,
                password=self.master_password,
                host=self.host,
                port=self.port
            ) as conn:
                with conn.cursor() as cursor:
                    new_username = f'insta_db_user_{generate_password(6)}'
                    new_password = generate_password()

                    cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
                        sql.Identifier(new_username)), [new_password])
                    conn.commit()

                    cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON SCHEMA {} TO {}").format(
                        sql.Identifier(schema_name), sql.Identifier(new_username)))
                    conn.commit()

                    logger.info(f"User '{new_username}' with password '{new_password}' created and granted access to schema '{schema_name}'.")

                    # Append credentials to .pgpass
                    try:
                        with open(PGPASS_FILE_PATH, 'a') as pgpass_file:
                            pgpass_file.write(f"{self.host}:{self.port}:{self.dbname}:{new_username}:{new_password}\n")
                        os.chmod(PGPASS_FILE_PATH, 0o600)  # Ensure correct permissions
                        logger.info(f"Updated .pgpass file with user credentials.")
                    except Exception as e:
                        logger.error(f"Error updating .pgpass file: {e}")

                    return new_username, new_password

        except psycopg2.Error as e:
            raise RuntimeError(f"Error creating user or granting access: {e}")


   

def generate_password(length=12):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for i in range(length))

