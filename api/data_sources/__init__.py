from flask import Blueprint

data_sources_bp = Blueprint("data_sources_bp", __name__)

from . import data_sources
from . import connections
from . import application_types
