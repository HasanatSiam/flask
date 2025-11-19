from flask import Blueprint

access_points_bp = Blueprint("access_points_bp", __name__)

from .access_points import *
from .access_entitlements import *
from .access_entitlement_elements import *