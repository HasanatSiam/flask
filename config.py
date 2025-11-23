#config.py
from celery import Celery, Task  # Import Celery and Task classes for asynchronous task management
from flask import Flask          # Import Flask to create and manage a web application
import redis                     # Import Redis library for broker communication
import psycopg2                  # Import psycopg2 to connect with PostgreSQL database
import os                        # Import os for environment variable handling
from dotenv import load_dotenv   # Import load_dotenv to load environment variables from a .env file
import ssl
from datetime import timedelta
from flask_mail import Mail  # Import Flask-Mail for email functionalities

# Load environment variables from the .env file
load_dotenv()  # This loads the variables defined in the .env file into the environment


# Define the path where the .env file is stored
# ENV_PATH = "/d01/def/app/server/.server_env"

# # Load the .env file (check if it exists before loading it)
# if os.path.exists(ENV_PATH):
#     load_dotenv(ENV_PATH)
# else:
#     print(f"Error: The .env file was not found at {ENV_PATH}")

# Initialize Flask-Mail globally
mail = Mail()

# Fetch Redis and database URLs from environment variables
redis_url = os.environ.get("MESSAGE_BROKER")         # Redis URL for Celery's message broker
database_url = os.environ.get("DATABASE_URL")   # PostgreSQL URL for Celery's result backend
FLOWER_URL = os.environ.get("FLOWER_URL")
crypto_secret_key = os.getenv("CRYPTO_SECRET_KEY")
jwt_secret_key = os.getenv("JWT_SECRET_ACCESS_TOKEN")
invitation_expire_time = int(os.getenv("INV_EXPIRE_TIME", 1))  # in days

def parse_expiry(value):
    try:
        value = value.strip().lower()
        if value.endswith('d'):
            return timedelta(days=int(value[:-1]))
        elif value.endswith('h'):
            return timedelta(hours=int(value[:-1]))
        elif value.endswith('m'):
            return timedelta(minutes=int(value[:-1]))
        elif value.isdigit():
            return timedelta(seconds=int(value))
        else:
            raise ValueError(f"Invalid time format: {value}")
    except (ValueError, TypeError) as e:
        raise ValueError(f"Could not parse expiry value '{value}': {e}")



# Function to initialize and configure Celery with Flask
def celery_init_app(app: Flask) -> Celery:
    # Define a custom Celery Task class that runs tasks in Flask's application context
    class FlaskTask(Task):
        def __call__(self, *args: object, **kwargs: object) -> object:
            # Use Flask's app context to ensure proper access to app resources
            with app.app_context():
                return self.run(*args, **kwargs)  # Call the task's run method with arguments

    # Create a Celery instance, associating it with the Flask app name and custom task class
    celery_app = Celery(app.name, task_cls=FlaskTask)
    
    # Configure Celery using the Flask app's configuration
    celery_app.config_from_object(app.config["CELERY"])

    # Manually apply SSL config if needed
    if "broker_use_ssl" in app.config["CELERY"]:
        celery_app.conf.broker_use_ssl = app.config["CELERY"]["broker_use_ssl"]

    
    # Set the created Celery app as the default instance
    celery_app.set_default()
    
    # Store the Celery instance in Flask's extensions for easy access in the app
    app.extensions["celery"] = celery_app
    
    # Return the configured Celery instance
    return celery_app


# Function to create and configure a Flask app
def create_app() -> Flask:
    # Create a Flask application instance
    app = Flask(__name__)
    
    # Configure the app with Celery settings using a dictionary
    # app.config.from_mapping(
    #     CELERY=dict(
    #         broker_url=redis_url,                      # Redis as the message broker
    #         result_backend="db+"+database_url,              # PostgreSQL as the result backend
    #         #result_backend=database_url,              # PostgreSQL as the result backend
    #         beat_scheduler='redbeat.RedBeatScheduler',# RedBeat scheduler for periodic tasks
    #         redbeat_redis_url=redis_url,              # Redis URL for RedBeat configuration
    #         timezone='UTC',                           # Use UTC timezone for tasks
    #         enable_utc=True                          # Enable UTC mode
    #     ),
    # )
    app.config.from_mapping(
        CELERY=dict(
            broker_url=redis_url,                      # Redis as the message broker
            result_backend="db+"+database_url,              # PostgreSQL as the result backend
            #result_backend=database_url,              # PostgreSQL as the result backend
            beat_scheduler='redbeat.RedBeatScheduler',# RedBeat scheduler for periodic tasks
            redbeat_redis_url=redis_url,              # Redis URL for RedBeat configuration
            redbeat_lock_timeout=900,
            # broker_use_ssl = {
            #     'ssl_cert_reqs': ssl.CERT_NONE  # or ssl.CERT_REQUIRED if you have proper certs
            # },
            timezone='UTC',                           # Use UTC timezone for tasks
            enable_utc=True                          # Enable UTC mode
        ),
        # JWT config from .env
        JWT_SECRET_KEY = os.getenv('JWT_SECRET_ACCESS_TOKEN'),
        JWT_ACCESS_TOKEN_EXPIRES = parse_expiry(os.getenv('ACCESS_TOKEN_EXPIRED_TIME', '15m')),
        JWT_REFRESH_TOKEN_EXPIRES = parse_expiry(os.getenv('REFRESH_TOKEN_EXPIRED_TIME', '30d')),
        FLOWER_URL = FLOWER_URL,

        INV_EXPIRE_TIME = invitation_expire_time,
        CRYPTO_SECRET_KEY = crypto_secret_key,

        # --- Flask-Mail Config ---
        MAIL_SERVER=os.getenv("MAIL_SERVER"),
        MAIL_PORT=int(os.getenv("MAIL_PORT", 587)),
        MAIL_USE_TLS=os.getenv("MAIL_USE_TLS", "True").lower() == "true",
        MAIL_USERNAME=os.getenv("EMAIL_USER"),
        MAIL_PASSWORD=os.getenv("EMAIL_PASS"),
        MAIL_DEFAULT_SENDER=("PROCG Team", os.getenv("EMAIL_USER"))

    )
    # Load additional configuration from environment variables with a prefix
    app.config.from_prefixed_env()
    
    # Initialize Celery with the Flask app
    celery_init_app(app)
    
    # Initialize Flask-Mail
    mail.init_app(app)

    # Return the fully configured Flask app
    return app
