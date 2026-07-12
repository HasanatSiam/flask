import time
import threading
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

from .manager import ConnectorManager, BaseConnector

class ConnectionRegistry:
    def __init__(self, global_max=200):
        self._engines = {}
        self._lock = threading.Lock()
        self._global_max = global_max

    def get_engine(self, config: dict, uri: str):
        # We'll use a combination of host, port, db, and user as the key 
        # since we don't always have a saved def_connection_id when testing.
        conn_id = config.get('def_connection_id', 'unsaved')
        host = config.get('host', '')
        db_name = config.get('database_name', '')
        user = config.get('username', '')
        
        key = f"{conn_id}:{host}:{db_name}:{user}"
        
        with self._lock:
            if key in self._engines:
                self._engines[key]["last_used"] = time.time()
                return self._engines[key]["engine"]
            
            # Using conservative pool sizes since multiple connections might exist
            engine = create_engine(
                uri,
                pool_size=5, 
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=1800,
            )
            self._engines[key] = {"engine": engine, "last_used": time.time()}
            return engine

# Singleton registry
connection_registry = ConnectionRegistry()

@ConnectorManager.register("postgresql")
@ConnectorManager.register("oracle")
@ConnectorManager.register("mysql")
class SQLAlchemyConnector(BaseConnector):
    
    def _build_uri(self) -> str:
        conn_type = self.config.get('connection_type', 'postgresql').lower()
        host = self.config.get('host', 'localhost')
        port = self.config.get('port')
        database = self.config.get('database_name', '')
        username = quote_plus(self.config.get('username', ''))
        
        # TODO: Implement credential vault decryption here once ready
        # e.g., plaintext_password = credential_vault.decrypt(self.config.get('password', ''))
        plaintext_password = self.config.get('password', '')
        password = quote_plus(plaintext_password) if plaintext_password else ''
        
        additional = self.config.get('additional_params', {})

        dialect_map = {
            'postgresql': 'postgresql+psycopg2',
            'oracle': 'oracle+oracledb',
            'mysql': 'mysql+pymysql'  # or mysqlclient depending on environment
        }
        
        dialect = dialect_map.get(conn_type, 'postgresql+psycopg2')
        
        # Default ports if not specified
        if not port:
            if conn_type == 'postgresql': port = 5432
            elif conn_type == 'oracle': port = 1521
            elif conn_type == 'mysql': port = 3306
            else: port = 5432

        uri = f"{dialect}://{username}:{password}@{host}:{port}/{database}"
        
        # Add SSL mode if specified
        sslmode = additional.get('sslmode')
        if sslmode:
            uri += f"?sslmode={sslmode}"
            
        return uri

    @property
    def engine(self):
        uri = self._build_uri()
        return connection_registry.get_engine(self.config, uri)

    def test_connection(self) -> tuple:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)

    def fetch_access_points(self) -> list[dict]:
        """Return normalized access point records for sync."""
        # Placeholder for dialect-specific query implementation
        return []

    def fetch_entitlements(self, access_point: dict) -> list[dict]:
        """Return entitlements for a specific access point."""
        # Placeholder for future implementation
        return []
