from flask import Blueprint

webhooks_bp = Blueprint("webhooks_bp", __name__)

from .webhooks import *
from .webhook_deliveries import *
