"""
PostgreSQL Handler
==================
Standard handler for PostgreSQL databases.
"""
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

from .manager import ConnectorManager, BaseHandler

@ConnectorManager.register("postgresql")
class PostgreSQLHandler(BaseHandler):
    
    def _build_uri(self, config):
        host = config.get('host', 'localhost')
        port = config.get('port') or 5432
        database = config.get('database_name', '')
        username = quote_plus(config.get('username', ''))
        password = quote_plus(config.get('password', ''))
        additional = config.get('additional_params', {})

        uri = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        
        # Add SSL mode if specified (common for Aiven/Supabase)
        sslmode = additional.get('sslmode')
        if sslmode:
            uri += f"?sslmode={sslmode}"
            
        return uri

    def test(self, config):
        try:
            uri = self._build_uri(config)
            engine = create_engine(uri, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
