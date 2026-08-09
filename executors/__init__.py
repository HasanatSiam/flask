import os
from dotenv import load_dotenv
from config import create_app
from .extensions import db

# ── Cross-Platform Environment Loader (Linux Server & Local Windows)
SERVER_ENV_PATH = "/d01/def/app/server/.server_env"
if os.path.exists(SERVER_ENV_PATH):
    load_dotenv(SERVER_ENV_PATH)
else:
    load_dotenv()

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

flask_app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_pre_ping": True,
    "pool_recycle": 300,
}
flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(flask_app)


    
celery_app = flask_app.extensions["celery"]

