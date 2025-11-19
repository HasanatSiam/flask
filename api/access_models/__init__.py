from flask import Blueprint

access_models_bp = Blueprint("access_models_bp", __name__)

from .access_models import *
from .access_model_logics import *
from .access_model_logic_attributes import *
