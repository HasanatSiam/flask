from flask import Blueprint

glob_conditions_bp = Blueprint("glob_conditions_bp", __name__)

from .global_conditions import *
from .global_condition_logics import *
from .global_condition_logic_attributes import *
