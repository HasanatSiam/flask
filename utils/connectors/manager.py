"""
Connector Manager
=================
Implements the Registry Pattern. This allows new connectors to be added 
without modifying the core API code.
"""

class ConnectorManager:
    _handlers = {}

    @classmethod
    def register(cls, type_name):
        """Decorator to register a connector handler."""
        def wrapper(handler_class):
            cls._handlers[type_name.lower()] = handler_class
            return handler_class
        return wrapper

    @classmethod
    def get_connector(cls, type_name, config):
        """Get an instantiated connector for a connection type."""
        handler_class = cls._handlers.get(type_name.lower())
        if not handler_class:
            supported = ", ".join(cls._handlers.keys())
            raise ValueError(f"Unsupported connection type: {type_name}. Supported: {supported}")
        return handler_class(config)

    @classmethod
    def get_supported_types(cls):
        """Returns list of all registered connection types."""
        return sorted(list(cls._handlers.keys()))

    @classmethod
    def test(cls, config: dict) -> tuple:
        """Generic test method that routes to the correct connector."""
        try:
            connector = cls.get_connector(config.get('connection_type', ''), config)
            return connector.test_connection()
        except Exception as e:
            return False, str(e)

class BaseConnector:
    """Standard interface that all connectors must implement."""
    
    def __init__(self, config: dict):
        self.config = config

    def test_connection(self) -> tuple:
        """Test connection. Returns tuple: (success: bool, message: str)"""
        raise NotImplementedError()

    def fetch_access_points(self) -> list[dict]:
        """Return normalized access point records for sync."""
        raise NotImplementedError()

    def fetch_entitlements(self, access_point: dict) -> list[dict]:
        """Return entitlements for a specific access point."""
        raise NotImplementedError()
