from flask import Blueprint

global_conditions_bp = Blueprint("global_conditions_bp", __name__)

from . import global_conditions
from . import global_condition_logics
from . import global_condition_logic_attributes
