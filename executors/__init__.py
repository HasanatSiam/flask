import os
from dotenv import load_dotenv
from config import create_app
from .python import execute as run_script
from .bash import execute as bash_script
from .stored_procedure import execute as execute_procedure
from .stored_function import execute as execute_function
from .http import execute as http_request
from .python_v1 import execute as python_script
from .extensions import db


# load_dotenv()
# Define the path where the .env file is stored
ENV_PATH = "/d01/def/app/server/.server_env"

# Load the .env file (check if it exists before loading it)
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"Error: The .env file was not found at {ENV_PATH}")

secret_key = os.getenv('JWT_SECRET_ACCESS_TOKEN')
database_url = os.getenv("DATABASE_URL")
database_url_test = os.getenv("DATABASE_URL_TEST")
print(f"database_url: {database_url}")
# print(f"database_url_test: {database_url_test}")

flask_app = create_app()
flask_app.config['SECRET_KEY'] = secret_key

# Production database (main)
flask_app.config["SQLALCHEMY_DATABASE_URI"] = database_url

# Test database (secondary) - using SQLALCHEMY_BINDS
# Access via bind_key="db_test" in models or db.session.using_bind("db_test")
if database_url_test:
    flask_app.config["SQLALCHEMY_BINDS"] = {
        "db_test": database_url_test
    }

db.init_app(flask_app)
    
celery_app = flask_app.extensions["celery"]

