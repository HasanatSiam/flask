from flask import Blueprint

global_conditions_bp = Blueprint("global_conditions_bp", __name__)

from .global_conditions import *
from .global_condition_logics import *
from .global_condition_logic_attributes import *
