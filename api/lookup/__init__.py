from flask import Blueprint

lookup_bp = Blueprint("lookup_bp", __name__)

from .lookup import *
from .lookup_values import *
