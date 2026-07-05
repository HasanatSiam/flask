from flask import Blueprint

controls_bp = Blueprint("controls_bp", __name__)

from . import controls
from . import control_environments

