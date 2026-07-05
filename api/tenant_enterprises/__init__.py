from flask import Blueprint

tenant_enterprise_bp = Blueprint("tenant_enterprise_bp", __name__)

from . import tenant
from . import enterprises
from . import job_titles
