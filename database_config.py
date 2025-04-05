from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


DATABASE_CONFIG = {
    'local': {
        'host': 'localhost',
        'user': os.getenv('DB_USER_LOCAL'),
        'password': os.getenv('DB_PASSWORD_LOCAL'),
        'database': os.getenv('DB_NAME_LOCAL')
    },
}

# Get current environment's database config
current_config = DATABASE_CONFIG.get(os.getenv('APP_ENV'), DATABASE_CONFIG['local'])
current_config['_20K_API_KEY'] = os.getenv('_20K_API_KEY')

# Create database URI
DB_URI = f"mysql+mysqlconnector://{current_config['user']}:{current_config['password']}@{current_config['host']}/{current_config['database']}"