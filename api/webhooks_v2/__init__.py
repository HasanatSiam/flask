# api/webhooks_v2/__init__.py
from flask import Blueprint

webhooks_v2_bp = Blueprint('webhooks_v2', __name__)

from . import routes
