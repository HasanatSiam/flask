from flask import Blueprint

access_models_bp = Blueprint("access_models_bp", __name__)

from . import access_models
from . import access_model_logics
from . import access_model_logic_attributes
