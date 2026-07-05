from flask import Blueprint

access_points_bp = Blueprint("access_points_bp", __name__)

from . import access_points
from . import access_entitlements
from . import access_entitlement_elements