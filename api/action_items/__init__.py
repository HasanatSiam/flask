from flask import Blueprint

action_items_bp = Blueprint("action_items_bp", __name__)

from . import action_items
from . import action_item_assignments

