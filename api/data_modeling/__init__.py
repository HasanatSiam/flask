from flask import Blueprint

data_modeling_bp = Blueprint("data_modeling_bp", __name__)

from .aggregation import *
from .metadata import *