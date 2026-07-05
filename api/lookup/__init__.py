from flask import Blueprint

lookup_bp = Blueprint("lookup_bp", __name__)

from . import lookup
from . import lookup_values
