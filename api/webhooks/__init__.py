# api/webhooks/__init__.py
from flask import Blueprint

webhooks_bp = Blueprint('webhooks', __name__)

from . import webhooks, logs, events, subscriptions
