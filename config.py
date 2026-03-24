# config.py
import json
import os
from datetime import timedelta
from celery import Celery, Task
from dotenv import load_dotenv
from flask import Flask
from flask_mail import Mail
from executors.extensions import cache

# load_dotenv()

# Production server: load .env from a specific path
ENV_PATH = "/d01/def/app/server/.server_env"
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"Error: The .env file was not found at {ENV_PATH}")


# ── Environment Variables

# Database
database_url      = os.getenv("DATABASE_URL")           # Production DB
database_url_test = os.getenv("DATABASE_URL_TEST")      # Test DB

# Redis / Broker
redis_url         = os.getenv("MESSAGE_BROKER")

# Auth & Security
jwt_secret_key    = os.getenv("JWT_SECRET_ACCESS_TOKEN")
crypto_secret_key = os.getenv("CRYPTO_SECRET_KEY")

# App
FLOWER_URL        = os.getenv("FLOWER_URL")
REACT_ENDPOINT_URL = os.getenv("REACT_ENDPOINT_URL")

# CORS — supports JSON array or comma-separated string in .env
allowed_origins_raw = os.getenv("ALLOWED_ORIGINS", "")
try:
    allowed_origins = json.loads(allowed_origins_raw)
    if not isinstance(allowed_origins, list):
        allowed_origins = [allowed_origins_raw]
except (json.JSONDecodeError, TypeError):
    allowed_origins = [o.strip() for o in allowed_origins_raw.split(",") if o.strip()]


# ── Helpers

def parse_expiry(value: str) -> timedelta:
    """Parse a duration string like '15m', '2h', '7d' into a timedelta."""
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


invitation_expire_time = parse_expiry(os.getenv("INVITATION_ACCESS_TOKEN_EXPIRED_TIME", "1h"))


# ── Flask-Mail (global instance)
mail = Mail()


# ── Celery

class FlaskTask(Task):
    """Celery Task subclass that runs inside a Flask application context."""
    def __call__(self, *args: object, **kwargs: object) -> object:
        with self.app.flask_app.app_context():
            return self.run(*args, **kwargs)


def celery_init_app(app: Flask) -> Celery:
    """Initialize and bind a Celery instance to the Flask app."""
    celery_app = Celery(app.name, task_cls=FlaskTask)
    celery_app.flask_app = app
    celery_app.config_from_object(app.config["CELERY"])

    if "broker_use_ssl" in app.config["CELERY"]:
        celery_app.conf.broker_use_ssl = app.config["CELERY"]["broker_use_ssl"]

    celery_app.set_default()
    app.extensions["celery"] = celery_app
    return celery_app


# ── App Factory

def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)
    app.json.sort_keys = False

    app.config.from_mapping(

        # ── Celery
        CELERY=dict(
            broker_url           = redis_url,
            result_backend       = "db+" + database_url,
            beat_scheduler       = "redbeat.RedBeatScheduler",
            redbeat_redis_url    = redis_url,
            redbeat_lock_timeout = 900,
            # broker_use_ssl = {
            #     'ssl_cert_reqs': ssl.CERT_NONE  # or ssl.CERT_REQUIRED if you have proper certs
            # },
            timezone             = "UTC",
            enable_utc           = True,
        ),

        # ── JWT
        JWT_SECRET_KEY              = os.getenv("JWT_SECRET_ACCESS_TOKEN"),
        JWT_ACCESS_TOKEN_EXPIRES    = parse_expiry(os.getenv("ACCESS_TOKEN_EXPIRED_TIME", "15m")),
        JWT_REFRESH_TOKEN_EXPIRES   = parse_expiry(os.getenv("REFRESH_TOKEN_EXPIRED_TIME", "30d")),
        JWT_TOKEN_LOCATION          = ["headers", "query_string", "cookies"],
        JWT_ACCESS_COOKIE_NAME      = "access_token",
        JWT_REFRESH_COOKIE_NAME     = "refresh_token",
        JWT_COOKIE_CSRF_PROTECT     = True,
        JWT_QUERY_STRING_NAME       = "access_token",

        # ── Mail
        MAIL_SERVER         = os.getenv("MAIL_SERVER"),
        MAIL_PORT           = int(os.getenv("MAIL_PORT", 587)),
        MAIL_USE_TLS        = os.getenv("MAIL_USE_TLS", "True").lower() == "true",
        MAIL_USERNAME       = os.getenv("MAILER_USER"),
        MAIL_PASSWORD       = os.getenv("MAILER_PASS"),
        MAIL_DEFAULT_SENDER = ("PROCG Team", os.getenv("MAILER_USER")),

        # ── Cache (Redis)
        CACHE_TYPE          = "RedisCache",
        CACHE_REDIS_URL     = redis_url,
        CACHE_DEFAULT_TIMEOUT = 300,

        # ── App-Specific
        FLOWER_URL          = FLOWER_URL,
        INV_EXPIRE_TIME     = parse_expiry(os.getenv("INVITATION_ACCESS_TOKEN_EXPIRED_TIME", "1h")),
        CRYPTO_SECRET_KEY   = crypto_secret_key,
    )

    # Load any FLASK_-prefixed env vars (overrides the above if set)
    app.config.from_prefixed_env()

    # ── Initialize Extensions
    celery_init_app(app)
    mail.init_app(app)
    cache.init_app(app)

    return app
