from flask import Blueprint

mobile_menu_bp = Blueprint('mobile_menu_bp', __name__)

from . import mobile_menu
