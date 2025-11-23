import logging
from executors import flask_app
from api import register_blueprints
from redis import Redis
from flask_jwt_extended import JWTManager


from flask_cors import CORS 
from config import redis_url
from itsdangerous import URLSafeTimedSerializer
# from executors.extensions import db

# db.init_app(flask_app)


# Init Services + JWT/CORS
# flower_url = flask_app.config["FLOWER_URL"]

# invitation_expire_time = flask_app.config["INV_EXPIRE_TIME"]
redis_client = Redis.from_url(redis_url, decode_responses=True)

jwt = JWTManager(flask_app)

CORS(
    flask_app,
    resources={r"/*": {"origins": "http://localhost:5173"}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PUT", "DELETE"]
)
# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)




# Register blueprints
register_blueprints(flask_app)

app = flask_app
