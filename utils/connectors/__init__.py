"""
Connector Package
=================
Exports the ConnectorManager which automatically registers all handlers.
In this simplified version, only PostgreSQL is registered.
"""

from .manager import ConnectorManager

# Import handlers here to trigger @ConnectorManager.register decorators
from . import sql_handler
from . import servicenow_handler

__all__ = ['ConnectorManager']
