
"""
ServiceNow Handler (CData)
==========================
Handler for ServiceNow using CData Python Connector.

Additional Parameters Support:
------------------------------
ServiceNow connections may require additional authentication parameters.
Pass them via the 'additional_params' field in your connection config.

Example additional_params:
{
    "AuthScheme": "Basic",           # or "OAuth", "OAuthJWT", etc.
    "InitiateOAuth": "GETANDREFRESH", # For OAuth flows
    "OAuthClientId": "your_client_id",
    "OAuthClientSecret": "your_secret",
    "Timeout": "60",
    "UseDisplayNames": "true"
}

For full list of supported properties, see:
https://cdn.cdata.com/help/SNG/python/
"""
from .manager import ConnectorManager, BaseHandler

try:
    import cdata.servicenow as servicenow
except ImportError:
    servicenow = None

@ConnectorManager.register("servicenow")
class ServiceNowHandler(BaseHandler):
    
    def _get_connection(self, config):
        if not servicenow:
            raise ImportError("CData ServiceNow driver is not installed.")
            
        user = config.get('username', '')
        password = config.get('password', '')
        url = config.get('host', '')
        
        # Build connection string (CData expects a connection string, not kwargs)
        conn_parts = []
        
        if user:
            conn_parts.append(f"User={user}")
        if password:
            conn_parts.append(f"Password={password}")
        if url:
            conn_parts.append(f"Url={url}")
        
        # Add additional parameters (e.g., AuthScheme, OAuth settings, etc.)
        additional = config.get('additional_params', {})
        if additional:
            for key, value in additional.items():
                conn_parts.append(f"{key}={value}")
        
        # Join all parts with semicolons
        conn_string = ";".join(conn_parts)
        
        return servicenow.connect(conn_string)

    def test(self, config):
        """Test ServiceNow connection."""
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
