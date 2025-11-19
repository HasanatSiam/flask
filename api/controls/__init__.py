from flask import Blueprint

controls_bp = Blueprint("controls_bp", __name__)

from .controls import *
from .control_environments import *

