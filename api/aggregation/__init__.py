from flask import Blueprint

aggregation_bp = Blueprint("aggregation_bp", __name__)

from .aggregation import *