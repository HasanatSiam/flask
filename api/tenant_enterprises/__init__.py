from flask import Blueprint

tenant_enterprise_bp = Blueprint("tenant_enterprise_bp", __name__)

from .tenant import *
from .enterprises import *
from .job_titles import *
