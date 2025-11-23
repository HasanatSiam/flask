from flask import Blueprint

messages_bp = Blueprint("messages_bp", __name__)

from .messages import *