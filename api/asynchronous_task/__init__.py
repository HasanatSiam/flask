from flask import Blueprint

async_task_bp = Blueprint("async_task_bp", __name__)

from .execution_method import *
from .task_parametes import *
from .task_schedules import *
from .task import *
from .view_requests import *

