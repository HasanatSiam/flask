"""
Connector Package
=================
Exports the ConnectorManager which automatically registers all handlers.
"""

from .manager import ConnectorManager

# Import handlers here to trigger @ConnectorManager.register decorators
from . import sql_handler
from . import servicenow_handler
from . import salesforce_handler
from . import mongodb_handler

__all__ = ['ConnectorManager', 'sql_handler', 'servicenow_handler', 'salesforce_handler', 'mongodb_handler']
