import requests
from .manager import ConnectorManager, BaseConnector

@ConnectorManager.register("servicenow")
class ServiceNowConnector(BaseConnector):
    
    def _build_session(self) -> requests.Session:
        session = requests.Session()
        
        username = self.config.get('username', '')
        # TODO: Implement credential vault decryption here once ready
        # e.g., plaintext_password = credential_vault.decrypt(self.config.get('password', ''))
        plaintext_password = self.config.get('password', '')
        
        if username and plaintext_password:
            session.auth = (username, plaintext_password)
            
        additional = self.config.get('additional_params', {})
        # Configure additional session parameters if needed (e.g., headers)
        
        return session

    @property
    def base_url(self) -> str:
        host = self.config.get('host', '').rstrip('/')
        if not host.startswith('http'):
            host = f"https://{host}"
        return host

    def test_connection(self) -> tuple:
        """Test ServiceNow connection using the Table API."""
        try:
            session = self._build_session()
            url = f"{self.base_url}/api/now/table/sys_user?sysparm_limit=1"
            resp = session.get(url, timeout=10)
            
            if resp.status_code == 200:
                return True, "Connection successful"
            else:
                return False, f"HTTP {resp.status_code}: {resp.text}"
        except Exception as e:
            return False, str(e)

    def fetch_access_points(self) -> list[dict]:
        """Return normalized access point records for sync."""
        # Placeholder for paginating through sys_user_role / sys_security_acl
        return []

    def fetch_entitlements(self, access_point: dict) -> list[dict]:
        """Return entitlements for a specific access point."""
        # Placeholder for future implementation
        return []
