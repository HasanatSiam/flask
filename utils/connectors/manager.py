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
            cls._handlers[type_name.lower()] = handler_class()
            return handler_class
        return wrapper

    @classmethod
    def get_handler(cls, type_name):
        """Get the registered handler for a connection type."""
        handler = cls._handlers.get(type_name.lower())
        if not handler:
            supported = ", ".join(cls._handlers.keys())
            raise ValueError(f"Unsupported connection type: {type_name}. Supported: {supported}")
        return handler

    @classmethod
    def get_supported_types(cls):
        """Returns list of all registered connection types."""
        return sorted(list(cls._handlers.keys()))

    @classmethod
    def test(cls, config: dict) -> tuple:
        """Generic test method that routes to the correct handler."""
        try:
            handler = cls.get_handler(config.get('connection_type', ''))
            return handler.test(config)
        except Exception as e:
            return False, str(e)

class BaseHandler:
    """Standard interface that all handlers must implement."""
    def test(self, config):
        """Test connection. Returns tuple: (success: bool, message: str)"""
        raise NotImplementedError()
