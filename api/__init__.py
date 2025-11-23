from flask import Blueprint
from .users import users_bp
from api.rbac import rbac_bp
from .access_models import access_models_bp
from .access_points import access_points_bp
from .action_items import access_items_bp
from .asynchronous_task import async_task_bp
from .global_conditions import global_conditions_bp
from .tenant_enterprises import tenant_enterprise_bp
from .controls import controls_bp
from .messages import messages_bp
from .data_sources import data_sources_bp

def register_blueprints(app):
    app.register_blueprint(users_bp)
    app.register_blueprint(rbac_bp)
    app.register_blueprint(access_models_bp)
    app.register_blueprint(access_points_bp)
    app.register_blueprint(access_items_bp)
    app.register_blueprint(async_task_bp)
    app.register_blueprint(global_conditions_bp)
    app.register_blueprint(tenant_enterprise_bp)
    app.register_blueprint(controls_bp)
    app.register_blueprint(messages_bp)
    app.register_blueprint(data_sources_bp)
    
